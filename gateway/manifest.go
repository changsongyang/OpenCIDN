package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/docker/distribution/registry/api/errcode"
)

func (c *Gateway) cacheManifestResponse(rw http.ResponseWriter, r *http.Request, info *PathInfo) {
	ctx := r.Context()

	if c.manifestCache != nil {
		c.manifestCache.Evict(info)
	}

	var fallback bool
	var cancel func()
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()

	key := manifestCacheKey(info)
	err := c.uniq.Do(ctx, key,
		func(ctx context.Context) (passCtx context.Context, done bool) {
			done, fallback = c.tryFirstServeCachedManifest(rw, r, info)
			if fallback &&
				cancel == nil &&
				c.recacheMaxWait > 0 &&
				c.checkCachedManifest(rw, r, info) {
				ctx, cancel = context.WithTimeout(ctx, c.recacheMaxWait)

			}
			return ctx, done
		},
		func(_ context.Context, cancelCauseFunc context.CancelCauseFunc) error {
			err := c.cacheManifest(info)
			if err != nil {
				return err
			}
			if c.serveCachedManifest(rw, r, info) {
				return nil
			}
			return errcode.ErrorCodeUnknown
		},
	)
	if err != nil {
		if fallback && c.serveCachedManifest(rw, r, info) {
			return
		}

		c.logger.Warn("error response", "remoteAddr", r.RemoteAddr, "error", err.Error())
		errcode.ServeJSON(rw, err)
		return
	}
}

func (c *Gateway) cacheManifest(info *PathInfo) error {
	ctx := context.Background()
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
			c.logger.Error("failed to unmarshal body", "statusCode", resp.StatusCode, "url", u.String(), "body", string(output))
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
	if c.manifestCache == nil {
		return c.serveCachedManifest(rw, r, info), false
	}

	val, ok := c.manifestCache.Get(info)
	if !ok {
		if info.IsDigestManifests {
			return c.serveCachedManifest(rw, r, info), false
		}
		return false, true
	}
	if val.Error != nil {
		errcode.ServeJSON(rw, val.Error)
		return true, false
	}

	if r.Method == http.MethodHead {
		rw.Header().Set("Docker-Content-Digest", val.Digest)
		rw.Header().Set("Content-Type", val.MediaType)
		rw.Header().Set("Content-Length", val.Length)
		return true, false
	}

	return c.serveCachedManifest(rw, r, info), false
}

func (c *Gateway) checkCachedManifest(rw http.ResponseWriter, r *http.Request, info *PathInfo) bool {
	ok, _ := c.cache.StatManifest(r.Context(), info.Host, info.Image, info.Manifests)
	return ok
}

func (c *Gateway) serveCachedManifest(rw http.ResponseWriter, r *http.Request, info *PathInfo) bool {
	ctx := r.Context()

	content, digest, mediaType, err := c.cache.GetManifestContent(ctx, info.Host, info.Image, info.Manifests)
	if err != nil {
		c.logger.Warn("Manifest cache missed", "host", info.Host, "image", info.Blobs, "manifest", info.Manifests, "digest", digest)
		return false
	}

	length := strconv.FormatInt(int64(len(content)), 10)

	if c.manifestCache != nil {
		c.manifestCache.Put(info, cacheValue{
			Digest:    digest,
			MediaType: mediaType,
			Length:    length,
		})
	}

	c.logger.Info("Manifest cache hit", "host", info.Host, "image", info.Blobs, "manifest", info.Manifests, "digest", digest)

	rw.Header().Set("Docker-Content-Digest", digest)
	rw.Header().Set("Content-Type", mediaType)
	rw.Header().Set("Content-Length", length)

	if r.Method != http.MethodHead {
		rw.Write(content)
	}

	return true
}
