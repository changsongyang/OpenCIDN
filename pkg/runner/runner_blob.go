package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OpenCIDN/OpenCIDN/pkg/cache"
	"github.com/OpenCIDN/OpenCIDN/pkg/queue/client"
	"github.com/OpenCIDN/OpenCIDN/pkg/queue/model"
	"github.com/wzshiming/httpseek"
	"github.com/wzshiming/sss"
)

func (r *Runner) runBlobSync(ctx context.Context) {
	for {
		err := r.runOnceBlobSync(context.Background())
		if err != nil {
			if err != errWait {
				r.logger.Warn("runOnceBlobSync", "error", err)
				select {
				case <-time.After(time.Second):
				case <-ctx.Done():
					return
				}
			} else {
				select {
				case <-r.syncBlobCh:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

func (r *Runner) getBlobPending() []client.MessageResponse {
	r.pendingMut.Lock()
	defer r.pendingMut.Unlock()

	var pendingMessages []client.MessageResponse
	for _, msg := range r.blobPending {
		if msg.Status == model.StatusPending {
			pendingMessages = append(pendingMessages, msg)
		}
	}

	sort.Slice(pendingMessages, func(i, j int) bool {
		a := pendingMessages[i]
		b := pendingMessages[j]

		if a.Priority != b.Priority {
			return a.Priority > b.Priority
		}
		return a.MessageID < b.MessageID
	})

	return pendingMessages
}

func (r *Runner) runOnceBlobSync(ctx context.Context) error {
	var (
		err  error
		errs []error
		resp client.MessageResponse
	)

	pending := r.getBlobPending()
	if len(pending) == 0 {
		return errWait
	}

	for _, msg := range pending {
		resp, err = r.queueClient.Consume(ctx, msg.MessageID, r.lease)
		if err != nil {
			errs = append(errs, err)
		} else {
			break
		}
	}

	if resp.MessageID == 0 || resp.Content == "" {
		if len(errs) == 0 {
			return errWait
		}
		return errors.Join(errs...)
	}

	return r.blobSync(ctx, resp)
}

func (r *Runner) blob(ctx context.Context, host, name, blob string, size int64, gotSize, progress *atomic.Int64) error {
	u := &url.URL{
		Scheme: "https",
		Host:   host,
		Path:   fmt.Sprintf("/v2/%s/blobs/%s", name, blob),
	}

	if size == 0 {
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, u.String(), nil)
		if err != nil {
			return err
		}
		resp, err := r.httpClient.Do(req)
		if err != nil {
			return err
		}
		if resp.Body != nil {
			_ = resp.Body.Close()
		}

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("failed to head blob: status code %d", resp.StatusCode)
		}
		size = resp.ContentLength
	}

	if size > 0 {
		gotSize.Store(size)
	}

	var caches []*cache.Cache

	if r.bigCache != nil && r.bigCacheSize > 0 && size >= int64(r.bigCacheSize) {
		caches = append([]*cache.Cache{r.bigCache}, r.caches...)
	} else {
		caches = append(caches, r.caches...)
	}

	var subCaches []*cache.Cache
	for _, cache := range caches {
		stat, err := cache.StatBlob(ctx, blob)
		if err == nil {
			gotSize := stat.Size()
			if size == gotSize {
				continue
			}
			r.logger.Error("size is not meeting expectations", "digest", blob, "size", size, "gotSize", gotSize)
		}
		subCaches = append(subCaches, cache)
	}

	if len(subCaches) == 0 {
		r.logger.Info("skip blob by cache", "digest", blob)
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}

	if r.resumeSize != 0 && size > int64(r.resumeSize) {
		var offset int64 = math.MaxInt

		rbws := []sss.FileWriter{}
		for _, cache := range subCaches {
			f, err := cache.BlobWriter(ctx, blob, true)
			if err == nil {
				if offset != 0 {
					offset = min(offset, f.Size())
				}
				rbws = append(rbws, f)

			} else {
				offset = 0
				f, err = cache.BlobWriter(ctx, blob, false)
				if err != nil {
					return err
				}
				rbws = append(rbws, f)
			}
		}

		if offset > size {
			return fmt.Errorf("offset %d exceeds expected size %d", offset, size)
		}
		progress.Store(offset)

		if offset != size {
			var writers []io.Writer
			for _, w := range rbws {
				n := w.Size() - offset
				if n == 0 {
					writers = append(writers, w)
				} else if n > 0 {
					writers = append(writers, &skipWriter{
						writer: w,
						offset: uint64(n),
					})
				} else {
					panic("runner: resume write blob error")
				}
			}

			seeker := httpseek.NewSeekerWithHTTPClient(ctx, r.httpClient, req)
			defer seeker.Close()

			reader := httpseek.NewMustReadSeeker(seeker, 0, func(retry int, err error) error {
				if retry > 2 {
					return err
				}
				r.logger.Warn("blob reader", "retry", retry, "digest", blob, "error", err)
				return nil
			})
			_, err := reader.Seek(offset, io.SeekStart)
			if err != nil {
				return err
			}

			body := &readerCounter{
				r:       reader,
				counter: progress,
			}

			n, err := io.Copy(io.MultiWriter(writers...), body)
			if err != nil {
				return fmt.Errorf("copy blob failed: %w", err)
			}

			if offset+n != size {
				return fmt.Errorf("copy blob failed: expected size %d, got offset %d, append %d", size, offset, n)
			}
		}

		var errs []error
		for _, c := range rbws {
			err := c.Commit(ctx)
			if err != nil {
				errs = append(errs, err)
			}
		}

		for _, cache := range subCaches {
			fi, err := cache.StatBlob(ctx, blob)
			if err != nil {
				errs = append(errs, err)
				continue
			}

			if fi.Size() != size {
				if err := cache.DeleteBlob(ctx, blob); err != nil {
					errs = append(errs, fmt.Errorf("%s is %d, but expected %d: %w", blob, fi.Size(), size, err))
				} else {
					errs = append(errs, fmt.Errorf("%s is %d, but expected %d", blob, fi.Size(), size))
				}
			}
		}

		return errors.Join(errs...)
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to get blob: status code %d", resp.StatusCode)
	}

	r.logger.Info("start sync blob", "digest", blob, "url", u.String())

	if resp.ContentLength > 0 && size > 0 {
		if resp.ContentLength != size {
			return fmt.Errorf("failed to retrieve blob: expected size %d, got %d", size, resp.ContentLength)
		}
	}

	body := &readerCounter{
		r:       resp.Body,
		counter: progress,
	}

	if len(subCaches) == 1 {
		n, err := subCaches[0].PutBlob(ctx, blob, body)
		if err != nil {
			return fmt.Errorf("put blob failed: %w", err)
		}

		r.logger.Info("finish sync blob", "digest", blob, "size", n)
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
				r.logger.Error("put blob failed", "digest", blob, "error", err)
				io.Copy(io.Discard, pr)
				return
			}
		}(ca, pr)
	}

	n, err := io.Copy(io.MultiWriter(writers...), body)
	if err != nil {
		return fmt.Errorf("copy blob failed: %w", err)
	}
	for _, c := range closers {
		c.Close()
	}

	wg.Wait()

	r.logger.Info("finish sync blob", "digest", blob, "size", n)
	return nil
}

func (r *Runner) blobSync(ctx context.Context, resp client.MessageResponse) error {
	var errCh = make(chan error, 1)

	var gotSize, progress atomic.Int64

	go func() {
		errCh <- r.blob(ctx, resp.Data.Host, resp.Data.Image, resp.Content, resp.Data.Size, &gotSize, &progress)
	}()

	return r.heartbeat(ctx, resp.MessageID, &gotSize, &progress, errCh)
}

type readerCounter struct {
	r       io.Reader
	counter *atomic.Int64
}

func (r *readerCounter) Read(b []byte) (int, error) {
	n, err := r.r.Read(b)

	r.counter.Add(int64(n))
	return n, err
}

type skipWriter struct {
	writer  io.Writer
	offset  uint64
	skipped uint64
}

func (w *skipWriter) Write(p []byte) (int, error) {
	if w.skipped >= w.offset {
		return w.writer.Write(p)
	}

	remaining := w.offset - w.skipped
	if uint64(len(p)) <= remaining {
		w.skipped += uint64(len(p))
		return len(p), nil
	}

	w.skipped = w.offset
	written, err := w.writer.Write(p[remaining:])
	return int(remaining) + written, err
}
