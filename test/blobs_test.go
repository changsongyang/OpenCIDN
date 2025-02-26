package test_test

import (
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"os"
	"testing"
)

var blobTestdata = []struct {
	Host   string
	Image  string
	Digest string
}{
	{"registry-1.docker.io", "library/hello-world", "sha256:03b62250a3cb1abd125271d393fc08bf0cc713391eda6b57c02d1ef85efcc25c"},
	{"gcr.io", "distroless/static", "sha256:5d7d2b4256071447af7ff304af79cc961e7e2fb862009ece2c10fc25cebd16e6"},
	{"ghcr.io", "opencidn/opencidn/gateway", "sha256:37fd4879f0d54f46e876a7e86e2e5eaf1304dc747aedf836883c934490d5c7fc"},
}

func testBlob(t *testing.T, server *httptest.Server, host, image, digest string) {
	url := server.URL + "/v2/" + host + "/" + image + "/blobs/" + digest
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

}
