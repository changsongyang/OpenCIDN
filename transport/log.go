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

func (l *logTransport) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		if elapsed >= l.minElapsed {
			if err != nil {
				l.logger.Warn("long time request error",
					"elapsed", elapsed.String(),
					"method", req.Method,
					"url", req.URL.String(),
					"error", err,
				)
			} else {
				l.logger.Warn("long time request",
					"elapsed", elapsed.String(),
					"method", req.Method,
					"url", req.URL.String(),
					"statusCode", resp.StatusCode,
				)
			}
		}
	}()

	return l.baseTransport.RoundTrip(req)
}
