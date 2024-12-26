package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/daocloud/crproxy/token"
	"github.com/docker/distribution/registry/api/errcode"
)

func (c *Gateway) cacheManifestResponse(rw http.ResponseWriter, r *http.Request, info *PathInfo, t *token.Token) {
	if c.tryFirstServeCachedManifest(rw, r, info) {
		return
	}

	reqCtx := r.Context()

	u := &url.URL{
		Scheme: "https",
		Host:   info.Host,
		Path:   fmt.Sprintf("/v2/%s/manifests/%s", info.Image, info.Manifests),
	}
	forwardReq, err := http.NewRequestWithContext(reqCtx, r.Method, u.String(), nil)
	if err != nil {
		c.logger.Error("failed to new request", "error", err)
		errcode.ServeJSON(rw, errcode.ErrorCodeUnknown)
		return
	}

	if forwardReq.Header == nil {
		forwardReq.Header = map[string][]string{}
	}

	if info.IsDigestManifests {
		forwardReq.Header.Set("Accept", r.Header.Get("Accept"))
	} else {
		forwardReq.Header.Set("Accept", strings.Join(c.acceptsItems, ","))
	}

	resp, err := c.httpClient.Do(forwardReq)
	if err != nil {
		if c.fallbackServeCachedManifest(rw, r, info) {
			return
		}
		c.logger.Error("failed to request", "url", u, "error", err)
		errcode.ServeJSON(rw, errcode.ErrorCodeUnknown)
		return
	}
	defer func() {
		resp.Body.Close()
	}()

	switch resp.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		if c.fallbackServeCachedManifest(rw, r, info) {
			c.logger.Error("origin manifest response 40x, but hit caches", "url", u, "response", dumpResponse(resp))
			return
		}
		c.logger.Error("origin manifest response 40x", "url", u, "response", dumpResponse(resp))
		errcode.ServeJSON(rw, errcode.ErrorCodeDenied)
		return
	}

	if resp.StatusCode >= http.StatusBadRequest && resp.StatusCode < http.StatusInternalServerError {
		if c.fallbackServeCachedManifest(rw, r, info) {
			c.logger.Error("origin manifest response 4xx, but hit caches", "url", u, "response", dumpResponse(resp))
			return
		}
		c.logger.Error("origin manifest response 4xx", "url", u)
	} else if resp.StatusCode >= http.StatusInternalServerError {
		if c.fallbackServeCachedManifest(rw, r, info) {
			c.logger.Error("origin manifest response 5xx, but hit caches", "url", u, "response", dumpResponse(resp))
			return
		}
		c.logger.Error("origin manifest response 5xx", "url", u)
	} else if resp.StatusCode < http.StatusOK {
		if c.fallbackServeCachedManifest(rw, r, info) {
			c.logger.Error("origin manifest response 1xx, but hit caches", "url", u, "response", dumpResponse(resp))
			return
		}
		c.logger.Error("origin manifest response 1xx", "url", u)
	}

	resp.Header.Del("Docker-Ratelimit-Source")

	header := rw.Header()
	for k, v := range resp.Header {
		key := textproto.CanonicalMIMEHeaderKey(k)
		header[key] = v
	}

	rw.WriteHeader(resp.StatusCode)

	if r.Method == http.MethodHead {
		return
	}

	needCache := resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices
	if !info.IsDigestManifests {
		_, ok := c.accepts[resp.Header.Get("Content-Type")]
		needCache = needCache && ok
	}
	if needCache {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			c.errorResponse(rw, r, err)
			return
		}

		_, _, err = c.cache.PutManifestContent(context.Background(), info.Host, info.Image, info.Manifests, body)
		if err != nil {
			c.errorResponse(rw, r, err)
			return
		}
		rw.Write(body)
	} else {
		io.Copy(rw, resp.Body)
	}
}

func (c *Gateway) tryFirstServeCachedManifest(rw http.ResponseWriter, r *http.Request, info *PathInfo) bool {
	if !info.IsDigestManifests && c.manifestCacheDuration > 0 {
		last, ok := c.manifestCache.Load(manifestCacheKey(info))
		if !ok {
			return false
		}

		if time.Since(last) > c.manifestCacheDuration {
			return false
		}
	}

	return c.serveCachedManifest(rw, r, info)
}

func (c *Gateway) fallbackServeCachedManifest(rw http.ResponseWriter, r *http.Request, info *PathInfo) bool {
	if info.IsDigestManifests {
		return false
	}

	return c.serveCachedManifest(rw, r, info)
}

func (c *Gateway) serveCachedManifest(rw http.ResponseWriter, r *http.Request, info *PathInfo) bool {
	ctx := r.Context()

	content, digest, mediaType, err := c.cache.GetManifestContent(ctx, info.Host, info.Image, info.Manifests)
	if err != nil {
		c.logger.Error("Manifest cache missed", "error", err)
		return false
	}

	c.logger.Info("Manifest cache hit", "digest", digest)
	rw.Header().Set("Docker-Content-Digest", digest)
	rw.Header().Set("Content-Type", mediaType)
	rw.Header().Set("Content-Length", strconv.FormatInt(int64(len(content)), 10))
	if r.Method != http.MethodHead {
		rw.Write(content)
	}

	if c.manifestCacheDuration > 0 && !info.IsDigestManifests {
		c.manifestCache.Store(manifestCacheKey(info), time.Now())
	}
	return true
}

type cacheKey struct {
	Host  string
	Image string
	Tag   string
}

func manifestCacheKey(info *PathInfo) cacheKey {
	return cacheKey{
		Host:  info.Host,
		Image: info.Image,
		Tag:   info.Manifests,
	}
}
