package transport

import (
	"log/slog"
	"net/http"
	"time"
)

type logTransport struct {
	baseTransport http.RoundTripper
	logger        *slog.Logger
	minElapsed    time.Duration
}

func NewLogTransport(baseTransport http.RoundTripper, logger *slog.Logger, minElapsed time.Duration) http.RoundTripper {
	return &logTransport{
		baseTransport: baseTransport,
		logger:        logger,
		minElapsed:    minElapsed,
	}
}

func (l *logTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		if elapsed >= l.minElapsed {
			l.logger.Warn("long time request", "elapsed", elapsed.String(), "url", req.URL.String())
		}
	}()

	return l.baseTransport.RoundTrip(req)
}
