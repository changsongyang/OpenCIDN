package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/daocloud/crproxy/cache"
	"github.com/daocloud/crproxy/queue/client"
	"github.com/daocloud/crproxy/queue/model"
	csync "github.com/daocloud/crproxy/sync"
)

type Runner struct {
	caches      []*cache.Cache
	httpClient  *http.Client
	client      *client.MessageClient
	syncManager *csync.SyncManager
	lease       string
	pendingMut  sync.Mutex
	pending     map[int64]client.MessageResponse
	syncCh      chan struct{}
}

func NewRunner(httpClient *http.Client, caches []*cache.Cache, lease, baseURL string, adminToken string, syncManager *csync.SyncManager) (*Runner, error) {
	cli := client.NewMessageClient(httpClient, baseURL, adminToken)
	return &Runner{
		httpClient:  httpClient,
		caches:      caches,
		client:      cli,
		lease:       lease,
		syncManager: syncManager,
		pending:     make(map[int64]client.MessageResponse),
		syncCh:      make(chan struct{}),
	}, nil
}

func (r *Runner) Run(ctx context.Context, logger *slog.Logger) error {
	logger.Info("lease", "id", r.lease)
	go r.watch(ctx, logger)

	r.sync(ctx, r.lease, logger)
	return ctx.Err()
}

func (r *Runner) watch(ctx context.Context, logger *slog.Logger) {
	for ctx.Err() == nil {
		if err := r.runWatch(ctx); err != nil {
			logger.Error("watch", "error", err)
		}
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return
		}
	}
}

func (r *Runner) sync(ctx context.Context, id string, logger *slog.Logger) {
	for i := 0; ctx.Err() == nil; i++ {
		if err := r.runOnceSync(context.Background(), id, logger); err != nil {
			if err != errWait {
				logger.Warn("sync", "error", err)
			} else {
				select {
				case <-r.syncCh:
				case <-ctx.Done():
					return
				}
				continue
			}
		}
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return
		}
	}
}

func (r *Runner) runWatch(ctx context.Context) error {
	ch, err := r.client.WatchList(ctx)
	if err != nil {
		return err
	}

	r.pendingMut.Lock()
	clear(r.pending)
	r.pendingMut.Unlock()

	for msg := range ch {
		r.pendingMut.Lock()
		if msg.Status == model.StatusPending {
			r.pending[msg.MessageID] = msg
		} else {
			delete(r.pending, msg.MessageID)
		}
		r.pendingMut.Unlock()

		if msg.Status == model.StatusPending {
			select {
			case r.syncCh <- struct{}{}:
			default:
			}
		}
	}
	return nil
}

func (r *Runner) getPending() []client.MessageResponse {
	r.pendingMut.Lock()
	defer r.pendingMut.Unlock()

	var pendingMessages []client.MessageResponse
	for _, msg := range r.pending {
		if msg.Status == model.StatusPending {
			pendingMessages = append(pendingMessages, msg)
		}
	}

	sort.Slice(pendingMessages, func(i, j int) bool {
		return pendingMessages[i].Priority > pendingMessages[j].Priority
	})

	return pendingMessages
}

var errWait = fmt.Errorf("no message received and no errors occurred")

func (r *Runner) runOnceSync(ctx context.Context, id string, logger *slog.Logger) error {
	var (
		err  error
		errs []error
		resp client.MessageResponse
	)

	pending := r.getPending()
	if len(pending) == 0 {
		return errWait
	}

	for _, msg := range pending {
		resp, err = r.client.Consume(ctx, msg.MessageID, id)
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

	if strings.HasPrefix(resp.Content, "sha256:") {
		if resp.Data.Host == "" || resp.Data.Image == "" {
			return nil
		}
		return r.blobSync(ctx, id, resp, logger)
	}

	return r.imageSync(ctx, id, resp, logger)
}

func (r *Runner) blob(ctx context.Context, host, name, blob string, size int64, gotSize, progress *atomic.Int64, logger *slog.Logger) error {
	var subCaches []*cache.Cache
	for _, cache := range r.caches {
		stat, err := cache.StatBlob(ctx, blob)
		if err == nil {
			if size > 0 {
				gotSize := stat.Size()
				if size == gotSize {
					continue
				}
				logger.Error("size is not meeting expectations", "digest", blob, "size", size, "gotSize", gotSize)
			} else {
				continue
			}
		}
		subCaches = append(subCaches, cache)
	}

	if len(subCaches) == 0 {
		logger.Info("skip blob by cache", "digest", blob)
		return nil
	}

	u := &url.URL{
		Scheme: "https",
		Host:   host,
		Path:   fmt.Sprintf("/v2/%s/blobs/%s", name, blob),
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to retrieve blob: status code %d", resp.StatusCode)
	}

	logger.Info("start sync blob", "digest", blob, "url", u.String())

	if resp.ContentLength > 0 && size > 0 {
		if resp.ContentLength != size {
			return fmt.Errorf("failed to retrieve blob: expected size %d, got %d", size, resp.ContentLength)
		}
	}

	if size > 0 {
		gotSize.Store(size)
	}
	if resp.ContentLength > 0 {
		gotSize.Store(resp.ContentLength)
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

		logger.Info("finish sync blob", "digest", blob, "size", n)
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
				logger.Error("put blob failed", "digest", blob, "error", err)
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

	logger.Info("finish sync blob", "digest", blob, "size", n)
	return nil
}

func (r *Runner) blobSync(ctx context.Context, id string, resp client.MessageResponse, logger *slog.Logger) error {
	err := r.client.Heartbeat(ctx, resp.MessageID, client.HeartbeatRequest{
		Lease: id,
	})
	if err != nil {
		_ = r.client.Cancel(ctx, resp.MessageID, client.CancelRequest{
			Lease: id,
		})
		return err
	}

	var errCh = make(chan error, 1)
	d := resp.Data

	var gotSize, progress atomic.Int64

	go func() {
		errCh <- r.blob(ctx, d.Host, d.Image, resp.Content, d.Size, &gotSize, &progress, logger)
	}()

	ticker := time.NewTicker(10 * time.Second)

	for {
		select {
		case <-ticker.C:
			err := r.client.Heartbeat(ctx, resp.MessageID, client.HeartbeatRequest{
				Lease: id,
				Data: model.MessageAttr{
					Size:     gotSize.Load(),
					Progress: progress.Load(),
				},
			})

			if err != nil {
				logger.Error("Heartbeat", "error", err)
			}

		case err := <-errCh:
			if err == nil {
				_ = r.client.Heartbeat(ctx, resp.MessageID, client.HeartbeatRequest{
					Lease: id,
					Data: model.MessageAttr{
						Size:     gotSize.Load(),
						Progress: progress.Load(),
					},
				})
				return r.client.Complete(ctx, resp.MessageID, client.CompletedRequest{
					Lease: id,
				})
			}

			if errors.Is(err, context.Canceled) {
				return r.client.Cancel(ctx, resp.MessageID, client.CancelRequest{
					Lease: id,
				})
			}

			return r.client.Failed(ctx, resp.MessageID, client.FailedRequest{
				Lease: id,
				Data: model.MessageAttr{
					Error: err.Error(),
				},
			})
		case <-ctx.Done():
			return r.client.Cancel(ctx, resp.MessageID, client.CancelRequest{
				Lease: id,
			})
		}
	}
}

func (r *Runner) imageSync(ctx context.Context, id string, resp client.MessageResponse, logger *slog.Logger) error {
	err := r.client.Heartbeat(ctx, resp.MessageID, client.HeartbeatRequest{
		Lease: id,
	})
	if err != nil {
		_ = r.client.Cancel(ctx, resp.MessageID, client.CancelRequest{
			Lease: id,
		})
		return err
	}

	var bmMut sync.Mutex
	var bm []model.Blob
	var updated bool

	var errCh = make(chan error, 1)

	go func() {
		errCh <- r.syncManager.ImageWithCallback(ctx, resp.Content, func(blob string, progress, size int64) {
			bmMut.Lock()
			defer bmMut.Unlock()
			updated = true
			for i, m := range bm {
				if m.Digest == blob {
					bm[i].Progress = progress
					bm[i].Size = size
					return
				}
			}
			bm = append(bm, model.Blob{
				Digest:   blob,
				Progress: progress,
				Size:     size,
			})
		})
	}()

	ticker := time.NewTicker(10 * time.Second)

	for {
		select {
		case <-ticker.C:
			bmMut.Lock()
			nbm := append([]model.Blob{}, bm...)
			updated = false
			bmMut.Unlock()

			err := r.client.Heartbeat(ctx, resp.MessageID, client.HeartbeatRequest{
				Lease: id,
				Data: model.MessageAttr{
					Blobs: nbm,
				},
			})

			if err != nil {
				logger.Error("Heartbeat", "error", err)
			}

		case err := <-errCh:
			if err == nil {
				if updated {
					err = r.client.Heartbeat(ctx, resp.MessageID, client.HeartbeatRequest{
						Lease: id,
						Data: model.MessageAttr{
							Blobs: bm,
						},
					})

					if err != nil {
						logger.Error("Heartbeat", "error", err)
					}
				}

				return r.client.Complete(ctx, resp.MessageID, client.CompletedRequest{
					Lease: id,
				})
			}

			if errors.Is(err, context.Canceled) {
				return r.client.Cancel(ctx, resp.MessageID, client.CancelRequest{
					Lease: id,
				})
			}

			return r.client.Failed(ctx, resp.MessageID, client.FailedRequest{
				Lease: id,
				Data: model.MessageAttr{
					Error: err.Error(),
				},
			})
		case <-ctx.Done():
			return r.client.Cancel(ctx, resp.MessageID, client.CancelRequest{
				Lease: id,
			})
		}
	}
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
