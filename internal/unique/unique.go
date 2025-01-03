package unique

import (
	"context"
	"sync"
)

type Unique[K comparable] struct {
	tracing sync.Map
}

type PreCheck func(ctx context.Context) (passCtx context.Context, done bool)
type Workload func(ctx context.Context, cancelCauseFunc context.CancelCauseFunc) (err error)

func (u *Unique[K]) signal(key K) (chan struct{}, bool) {
	sig, loaded := u.tracing.LoadOrStore(key, make(chan struct{}))
	return sig.(chan struct{}), loaded
}

func (u *Unique[K]) signalWait(ctx context.Context, key K) (func(), bool, error) {
	var needRecheck bool
	sig, loaded := u.signal(key)
	for loaded {
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-sig:
		}
		sig, loaded = u.signal(key)
		needRecheck = true
	}

	endFunc := func() {
		u.tracing.Delete(key)
		close(sig)
	}
	return endFunc, needRecheck, nil
}

func (u *Unique[K]) Do(
	ctx context.Context,
	key K,
	pc PreCheck,
	wl Workload,
) error {
	ctx, done := pc(ctx)
	if done {
		return nil
	}

	endFunc, needRecheck, err := u.signalWait(ctx, key)
	if err != nil {
		return err
	}

	if needRecheck {
		ctx, done = pc(ctx)
		if done {
			endFunc()
			return nil
		}
	}

	ctx, cancel := context.WithCancelCause(ctx)
	errCh := make(chan error, 1)
	go func() {
		defer endFunc()
		errCh <- wl(ctx, cancel)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		cancel(nil)
		return err
	}
}
