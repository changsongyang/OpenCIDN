package test_test

import (
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"os"
	"testing"
)

var manifestTestdata = []struct {
	Host     string
	Image    string
	Manifest string
}{
	{"registry-1.docker.io", "library/hello-world", "latest"},
	{"gcr.io", "distroless/static", "latest"},
	{"ghcr.io", "opencidn/opencidn/gateway", "v0.0.1"},
}

func testManifest(t *testing.T, server *httptest.Server, host, image, tag string) {
	url := server.URL + "/v2/" + host + "/" + image + "/manifests/" + tag
	req, err := http.NewRequest(http.MethodHead, url, nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != http.StatusOK {
		data, _ := httputil.DumpResponse(resp, true)
		os.Stderr.Write(data)
		t.Fatal(resp.Status, url)
	}

	digest := resp.Header.Get("Docker-Content-Digest")
	if digest == "" {
		data, _ := httputil.DumpResponse(resp, true)
		os.Stderr.Write(data)
		t.Fatal(resp.Header)
	}
}
