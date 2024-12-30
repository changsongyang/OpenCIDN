package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/daocloud/crproxy/token"
	"github.com/docker/distribution/registry/api/errcode"
)

func (c *Gateway) cacheManifestResponse(rw http.ResponseWriter, r *http.Request, info *PathInfo, t *token.Token) {
	ctx := r.Context()

	done, fallback := c.tryFirstServeCachedManifest(rw, r, info)
	if done {
		return
	}

	key := manifestCacheKey(info)
	closeValue, loaded := c.mutCache.LoadOrStore(key, make(chan struct{}))
	closeCh := closeValue.(chan struct{})
	for loaded {
		select {
		case <-ctx.Done():
			err := ctx.Err().Error()
			c.logger.Warn("context done", "error", err)
			http.Error(rw, err, http.StatusInternalServerError)
			return
		case <-closeCh:
		}
		closeValue, loaded = c.mutCache.LoadOrStore(key, make(chan struct{}))
		closeCh = closeValue.(chan struct{})
	}

	doneCache := func() {
		c.mutCache.Delete(key)
		close(closeCh)
	}

	done, fallback = c.tryFirstServeCachedManifest(rw, r, info)
	if done {
		doneCache()
		return
	}

	type signal struct {
		err error
	}
	signalCh := make(chan signal, 1)

	go func() {
		defer doneCache()
		err := c.cacheManifest(context.Background(), info)
		if err != nil {
			if fallback && c.fallbackServeCachedManifest(rw, r, info) {
				c.logger.Warn("failed to request, but hit caches", "error", err)
				signalCh <- signal{
					err: nil,
				}
				return
			}
		}
		signalCh <- signal{
			err: err,
		}
	}()

	select {
	case <-ctx.Done():
		c.errorResponse(rw, r, ctx.Err())
		return
	case signal := <-signalCh:
		if signal.err != nil {
			c.errorResponse(rw, r, signal.err)
			return
		}

		if c.serveCachedManifest(rw, r, info) {
			return
		}

		c.logger.Error("should not be executed", "url", r.URL.String())
		errcode.ServeJSON(rw, errcode.ErrorCodeUnknown)
		return
	}
}

func (c *Gateway) cacheManifest(ctx context.Context, info *PathInfo) error {
	u := &url.URL{
		Scheme: "https",
		Host:   info.Host,
		Path:   fmt.Sprintf("/v2/%s/manifests/%s", info.Image, info.Manifests),
	}

	if !info.IsDigestManifests {
		forwardReq, err := http.NewRequestWithContext(ctx, http.MethodHead, u.String(), nil)
		if err != nil {
			return err
		}
		// Never trust a client's Accept !!!
		forwardReq.Header.Set("Accept", c.acceptsStr)

		resp, err := c.httpClient.Do(forwardReq)
		if err != nil {
			return err
		}
		if resp.Body != nil {
			resp.Body.Close()
		}
		if resp.StatusCode == http.StatusOK {
			digest := resp.Header.Get("Docker-Content-Digest")
			if digest != "" {
				err = c.cache.RelinkManifest(ctx, info.Host, info.Image, info.Manifests, digest)
				if err != nil {
					c.logger.Warn("failed relink manifest", "url", u.String(), "error", err)
				} else {
					c.logger.Info("relink manifest", "url", u.String())
					return nil
				}
			}
			u.Path = fmt.Sprintf("/v2/%s/manifests/%s", info.Image, digest)
		}
	}

	forwardReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	// Never trust a client's Accept !!!
	forwardReq.Header.Set("Accept", c.acceptsStr)

	resp, err := c.httpClient.Do(forwardReq)
	if err != nil {
		c.logger.Warn("failed to request", "url", u.String(), "error", err)
		return errcode.ErrorCodeUnknown
	}
	defer func() {
		resp.Body.Close()
	}()

	switch resp.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		c.logger.Error("upstream denied", "statusCode", resp.StatusCode, "url", u.String(), "response", dumpResponse(resp))
		return errcode.ErrorCodeDenied
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		c.logger.Error("failed to get body", "statusCode", resp.StatusCode, "url", u.String(), "error", err)
		return errcode.ErrorCodeUnknown
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		var retErrs errcode.Errors
		err = retErrs.UnmarshalJSON(body)
		if err != nil {
			output := body
			if len(output) > 1024 {
				output = output[:1024]
			}
			c.logger.Error("failed to unmarshal body", "url", "statusCode", resp.StatusCode, u.String(), "body", string(output))
			return errcode.ErrorCodeUnknown
		}
		return retErrs
	}

	_, _, err = c.cache.PutManifestContent(ctx, info.Host, info.Image, info.Manifests, body)
	if err != nil {
		return err
	}

	return nil
}

func (c *Gateway) tryFirstServeCachedManifest(rw http.ResponseWriter, r *http.Request, info *PathInfo) (done bool, fallback bool) {
	if c.manifestCacheDuration == 0 {
		return false, true
	}

	if !info.IsDigestManifests {
		last, ok := c.manifestCache.Load(manifestCacheKey(info))
		if !ok {
			return false, true
		}

		if time.Since(last) > c.manifestCacheDuration {
			return false, true
		}
	}

	return c.serveCachedManifest(rw, r, info), false
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
		c.logger.Warn("Manifest cache missed", "host", info.Host, "image", info.Blobs, "manifest", info.Manifests, "digest", digest, "error", err)
		return false
	}

	c.logger.Info("Manifest cache hit", "host", info.Host, "image", info.Blobs, "manifest", info.Manifests, "digest", digest)
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
