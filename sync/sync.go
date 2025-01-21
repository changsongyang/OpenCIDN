package crproxy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/daocloud/crproxy/cache"
	"github.com/daocloud/crproxy/internal/sets"
	"github.com/daocloud/crproxy/internal/slices"
	"github.com/daocloud/crproxy/internal/utils"
	"github.com/distribution/reference"
	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest/manifestlist"
	"github.com/docker/distribution/manifest/ocischema"
	"github.com/docker/distribution/manifest/schema1"
	"github.com/docker/distribution/manifest/schema2"
	"github.com/docker/distribution/registry/client"
	"github.com/opencontainers/go-digest"
)

type namedWithoutDomain struct {
	reference.Reference
	name string
}

func (n namedWithoutDomain) Name() string {
	return n.name
}

func newNameWithoutDomain(named reference.Named, name string) reference.Named {
	return namedWithoutDomain{
		Reference: named,
		name:      name,
	}
}

type SyncManager struct {
	transport http.RoundTripper
	caches    []*cache.Cache
	logger    *slog.Logger
	deep      bool
	quick     bool

	uniqBlob *sets.Set[digest.Digest]

	excludeTags    []*regexp.Regexp
	filterPlatform func(pf manifestlist.PlatformSpec) bool
}

type Option func(*SyncManager)

func WithDeep(deep bool) Option {
	return func(c *SyncManager) {
		c.deep = deep
	}
}

func WithQuick(quick bool) Option {
	return func(c *SyncManager) {
		c.quick = quick
	}
}

func WithLogger(logger *slog.Logger) Option {
	return func(c *SyncManager) {
		c.logger = logger
	}
}

func WithCaches(caches ...*cache.Cache) Option {
	return func(c *SyncManager) {
		c.caches = caches
	}
}

func WithTransport(transport http.RoundTripper) Option {
	return func(c *SyncManager) {
		c.transport = transport
	}
}

func WithFilterPlatform(filterPlatform func(pf manifestlist.PlatformSpec) bool) Option {
	return func(c *SyncManager) {
		c.filterPlatform = filterPlatform
	}
}

func WithExcludeTags(excludeTags []*regexp.Regexp) Option {
	return func(c *SyncManager) {
		c.excludeTags = excludeTags
	}
}

func NewSyncManager(opts ...Option) (*SyncManager, error) {
	c := &SyncManager{
		logger:    slog.Default(),
		transport: http.DefaultTransport,
		uniqBlob:  sets.NewSet[digest.Digest](),
	}
	for _, opt := range opts {
		opt(c)
	}

	if len(c.caches) == 0 {
		return nil, fmt.Errorf("cache is required")
	}

	return c, nil
}

func (c *SyncManager) Image(ctx context.Context, image string) error {
	return c.ImageWithCallback(ctx, image, nil)
}

func (c *SyncManager) ImageWithCallback(ctx context.Context, image string, blobFunc func(blob string, progress int64, size int64)) error {
	var regexTag *regexp.Regexp
	ref, err := reference.Parse(image)
	if err != nil {
		parts := strings.SplitN(image, ":", 2)
		if len(parts) != 2 {
			return fmt.Errorf("parse image %q failed: %w", image, err)
		}

		ref, err = reference.Parse(parts[0])
		if err != nil {
			return fmt.Errorf("parse image %q failed: %w", image, err)
		}

		image = parts[0]

		if parts[1] != "" {
			regexTag, err = regexp.Compile(parts[1])
			if err != nil {
				return fmt.Errorf("compile regex failed: %w", err)
			}
		}
	}

	named, ok := ref.(reference.Named)
	if !ok {
		return fmt.Errorf("%s is not a name", ref)
	}

	host := reference.Domain(named)

	path := reference.Path(named)

	host, path = utils.CorrectImage(host, path)
	name := newNameWithoutDomain(named, path)

	repo, err := client.NewRepository(name, "https://"+host, c.transport)
	if err != nil {
		return fmt.Errorf("create repository failed: %w", err)
	}

	ms, err := repo.Manifests(ctx)
	if err != nil {
		return fmt.Errorf("get manifests failed: %w", err)
	}

	bs := repo.Blobs(ctx)

	blobCallback := func(caches []*cache.Cache, dgst digest.Digest, size int64, pf *manifestlist.PlatformSpec, name string) error {
		if c.uniqBlob.Contains(dgst) {
			c.logger.Info("skip blob by unique", "image", image, "digest", dgst)
			return nil
		}
		c.uniqBlob.Add(dgst)

		blob := dgst.String()
		var gotSize int64
		var subCaches []*cache.Cache
		for _, cache := range caches {
			stat, err := cache.StatBlob(ctx, blob)
			if err == nil {
				if size > 0 {
					gotSize = stat.Size()
					if size == gotSize {
						continue
					}
					c.logger.Error("size is not meeting expectations", "image", image, "digest", dgst, "size", size, "gotSize", gotSize)
				} else {
					continue
				}
			}
			subCaches = append(subCaches, cache)
		}

		if len(subCaches) == 0 {
			if blobFunc != nil {
				blobFunc(blob, gotSize, gotSize)
			}
			c.logger.Info("skip blob by cache", "image", image, "digest", dgst)
			return nil
		}

		f, err := bs.Open(ctx, dgst)
		if err != nil {
			return fmt.Errorf("open blob failed: %w", err)
		}
		defer f.Close()

		if blobFunc != nil {
			blobFunc(blob, 0, 0)
		}

		c.logger.Info("start sync blob", "image", image, "digest", dgst, "platform", pf)

		if len(subCaches) == 1 {
			n, err := subCaches[0].PutBlob(ctx, blob, f)
			if err != nil {
				return fmt.Errorf("put blob failed: %w", err)
			}
			if blobFunc != nil {
				blobFunc(blob, n, n)
			}
			c.logger.Info("finish sync blob", "image", image, "digest", dgst, "platform", pf, "size", n)
			return nil
		}

		var writers []io.Writer
		var closers []io.Closer
		var wg sync.WaitGroup

		for _, ca := range subCaches {
			pr, pw := io.Pipe()
			writers = append(writers, pw)
			closers = append(closers, pw)
			wg.Add(1)
			go func(cache *cache.Cache, pr io.Reader) {
				defer wg.Done()
				_, err := cache.PutBlob(ctx, blob, pr)
				if err != nil {
					c.logger.Error("put blob failed", "image", image, "digest", dgst, "platform", pf, "error", err)
					io.Copy(io.Discard, pr)
					return
				}
			}(ca, pr)
		}

		n, err := io.Copy(io.MultiWriter(writers...), f)
		if err != nil {
			return fmt.Errorf("copy blob failed: %w", err)
		}
		for _, c := range closers {
			c.Close()
		}

		wg.Wait()

		if blobFunc != nil {
			blobFunc(blob, n, n)
		}

		c.logger.Info("finish sync blob", "image", image, "digest", dgst, "platform", pf, "size", n)
		return nil
	}

	var mpps []manifestPushPending

	manifestCallback := func(caches []*cache.Cache, tagOrHash string, m distribution.Manifest) error {
		_, playload, err := m.Payload()
		if err != nil {
			return fmt.Errorf("get manifest payload failed: %w", err)
		}

		mpps = append(mpps, manifestPushPending{
			caches:    caches,
			tagOrHash: tagOrHash,
			playload:  playload,
		})
		return nil
	}

	ts := repo.Tags(ctx)

	switch ref.(type) {
	case reference.Digested, reference.Tagged:
		c.logger.Info("Start sync", "image", image)
		err = c.syncLayerFromManifestList(ctx, host, path, image, ms, ts, ref, blobCallback, manifestCallback, host+"/"+ref.String())
		if err != nil {
			return fmt.Errorf("sync layer from manifest list failed: %w", err)
		}
	default:
		tags, err := ts.All(ctx)
		if err != nil {
			return fmt.Errorf("get tags failed: %w", err)
		}

		if regexTag != nil || len(c.excludeTags) != 0 {
			tags = slices.Filter(tags, func(tag string) bool {
				if regexTag != nil && !regexTag.MatchString(tag) {
					return false
				}

				if len(c.excludeTags) != 0 {
					for _, reg := range c.excludeTags {
						if reg.MatchString(tag) {
							return false
						}
					}
				}

				return true
			})
		}

		if c.quick {
			cacheTags := sets.NewSet[string]()
			for i, cache := range c.caches {
				tags, err := cache.ListTags(ctx, host, path)
				if err != nil {
					return fmt.Errorf("failed to list tags: %w", err)
				}

				if i == 0 {
					cacheTags.Add(tags...)
				} else {
					cacheTags.Intersection(sets.NewSet(tags...))
				}
			}

			sourceTags := sets.NewSet(tags...)
			sourceTags.Difference(cacheTags)

			tags = sourceTags.List()
		}

		sort.Strings(tags)

		c.logger.Info("Start sync", "image", image, "tags", tags, "size", len(tags))

		rand.Shuffle(len(tags), func(i, j int) {
			tags[i], tags[j] = tags[j], tags[i]
		})

		for _, tag := range tags {
			t, err := reference.WithTag(name, tag)
			if err != nil {
				return fmt.Errorf("with tag failed: %w", err)
			}
			err = c.syncLayerFromManifestList(ctx, host, path, image, ms, ts, t, blobCallback, manifestCallback, host+"/"+t.String())
			if err != nil {
				return fmt.Errorf("sync layer from manifest list failed: %w", err)
			}
		}
	}

	for i := len(mpps) - 1; i >= 0; i-- {
		mpp := mpps[i]
		for _, cache := range mpp.caches {
			_, _, _, err = cache.PutManifestContent(ctx, host, path, mpp.tagOrHash, mpp.playload)
			if err != nil {
				return fmt.Errorf("put manifest content failed: %w", err)
			}
		}
	}
	return nil
}

type manifestPushPending struct {
	caches    []*cache.Cache
	tagOrHash string
	playload  []byte
}

func (c *SyncManager) syncLayerFromManifestList(ctx context.Context, host, path, image string, ms distribution.ManifestService, ts distribution.TagService, ref reference.Reference,
	digestCallback func(caches []*cache.Cache, dgst digest.Digest, size int64, pf *manifestlist.PlatformSpec, name string) error,
	manifestCallback func(caches []*cache.Cache, tagOrHash string, m distribution.Manifest) error, name string) error {

	var (
		m   distribution.Manifest
		err error
	)

	var caches []*cache.Cache

	if c.deep {
		caches = c.caches
	}

	var dgst digest.Digest
	switch r := ref.(type) {
	case reference.Digested:
		dgst = r.Digest()

		if !c.deep {
			for _, cache := range c.caches {
				b, _ := cache.StatManifest(ctx, host, path, dgst.String())
				if !b {
					caches = append(caches, cache)
				}
			}
			if len(caches) == 0 {
				c.logger.Info("skip manifest by cache", "image", image, "digest", dgst)
				return nil
			}
		}
		m, err = ms.Get(ctx, dgst)
		if err != nil {
			return fmt.Errorf("get manifest digest failed: %w", err)
		}
		err = manifestCallback(caches, dgst.String(), m)
		if err != nil {
			return fmt.Errorf("manifest callback failed: %w", err)
		}
	case reference.Tagged:
		tag := r.Tag()
		desc, err := ts.Get(ctx, tag)
		if err != nil {
			return fmt.Errorf("get tag failed: %w", err)
		}
		dgst = desc.Digest
		if !c.deep {
			for _, cache := range c.caches {
				b, _ := cache.StatOrRelinkManifest(ctx, host, path, tag, dgst.String())
				if !b {
					caches = append(caches, cache)
				}
			}
			if len(caches) == 0 {
				c.logger.Info("skip manifest by cache", "image", image, "digest", dgst, "tag", tag)
				return nil
			}
		}
		m, err = ms.Get(ctx, dgst)
		if err != nil {
			return fmt.Errorf("get manifest digest failed: %w", err)
		}
		err = manifestCallback(caches, tag, m)
		if err != nil {
			return fmt.Errorf("manifest callback failed: %w", err)
		}
	default:
		return fmt.Errorf("%s no reference to any source", ref)
	}

	switch m := m.(type) {
	case *manifestlist.DeserializedManifestList:
		for _, mfest := range m.ManifestList.Manifests {
			if c.filterPlatform != nil && !c.filterPlatform(mfest.Platform) {
				c.logger.Info("skip manifest by filter platform", "image", image, "digest", mfest.Digest, "platform", mfest.Platform)
				continue
			}

			var subCaches []*cache.Cache
			if !c.deep {
				for _, cache := range caches {
					stat, err := cache.StatBlob(ctx, mfest.Digest.String())
					if err == nil && stat.Size() == mfest.Size {
						continue
					}
					subCaches = append(subCaches, cache)
				}

				if len(subCaches) == 0 {
					c.logger.Info("skip manifest by cache", "image", image, "digest", mfest.Digest, "platform", mfest.Platform)
					continue
				}
			} else {
				subCaches = caches
			}

			m0, err := ms.Get(ctx, mfest.Digest)
			if err != nil {
				return fmt.Errorf("get manifest failed: %w", err)
			}
			err = manifestCallback(subCaches, mfest.Digest.String(), m0)
			if err != nil {
				return fmt.Errorf("manifest callback failed: %w", err)
			}
			err = c.syncLayerFromManifest(m0, func(dgst digest.Digest, size int64) error {
				return digestCallback(subCaches, dgst, size, &mfest.Platform, name)
			})
			if err != nil {
				return fmt.Errorf("get layer from manifest failed: %w", err)
			}
		}
		return nil
	default:
		return c.syncLayerFromManifest(m, func(dgst digest.Digest, size int64) error {
			return digestCallback(caches, dgst, size, nil, name)
		})
	}
}

func (c *SyncManager) syncLayerFromManifest(m distribution.Manifest, cb func(dgst digest.Digest, size int64) error) error {
	switch m := m.(type) {
	case *ocischema.DeserializedManifest:
		if m.Config.Size != 0 {
			err := cb(m.Config.Digest, m.Config.Size)
			if err != nil {
				return fmt.Errorf("digest callback failed: %w", err)
			}
		}
		for _, layer := range m.Layers {
			if layer.Size == 0 {
				continue
			}
			err := cb(layer.Digest, layer.Size)
			if err != nil {
				return fmt.Errorf("digest callback failed: %w", err)
			}
		}
	case *schema2.DeserializedManifest:
		if m.Config.Size != 0 {
			err := cb(m.Config.Digest, m.Config.Size)
			if err != nil {
				return fmt.Errorf("digest callback failed: %w", err)
			}
		}
		for _, layer := range m.Layers {
			if layer.Size == 0 {
				continue
			}
			err := cb(layer.Digest, layer.Size)
			if err != nil {
				return fmt.Errorf("digest callback failed: %w", err)
			}
		}
	case *schema1.SignedManifest:
		for _, layer := range m.FSLayers {
			err := cb(layer.BlobSum, -1)
			if err != nil {
				return fmt.Errorf("digest callback failed: %w", err)
			}
		}
	}
	return nil
}
