package manifests

import (
	"fmt"
	"io"
	"net/http"
)

type PathInfo struct {
	Host  string
	Image string

	Manifests         string
	IsDigestManifests bool
}

func dumpResponse(resp *http.Response) string {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	return fmt.Sprintf("%d %d %q", resp.StatusCode, resp.ContentLength, string(body))
}
