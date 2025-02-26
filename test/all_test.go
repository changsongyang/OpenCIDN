package test_test

import (
	"fmt"
	"net/http/httptest"
	"testing"
)

func TestAll(t *testing.T) {
	envs := []func() (*httptest.Server, string, func()){
		OnlyForward,
		WithCache,
	}
	for _, env := range envs {
		server, name, done := env()
		t.Run(name, func(t *testing.T) {
			t.Cleanup(done)

			for _, m := range manifestTestdata {
				t.Run(fmt.Sprintf("%s/%s:%s", m.Host, m.Image, m.Manifest), func(t *testing.T) {
					testManifest(t, server, m.Host, m.Image, m.Manifest)
				})
			}

			for _, m := range blobTestdata {
				t.Run(fmt.Sprintf("%s/%s@%s", m.Host, m.Image, m.Digest), func(t *testing.T) {
					testBlob(t, server, m.Host, m.Image, m.Digest)
				})
			}
		})
	}
}
