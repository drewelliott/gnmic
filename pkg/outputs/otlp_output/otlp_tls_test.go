// © 2026 NVIDIA Corporation
//
// This code is a Contribution to the gNMIc project ("Work") made under the Google Software Grant and Corporate Contributor License Agreement ("CLA") and governed by the Apache License 2.0.
// No other rights or licenses in or to any of NVIDIA's intellectual property are granted for any other purpose.
// This code is provided on an "as is" basis without any warranties of any kind.
//
// SPDX-License-Identifier: Apache-2.0

package otlp_output

import (
	"context"
	"crypto/ecdsa"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/openconfig/gnmic/pkg/api/types"
	"github.com/openconfig/gnmic/pkg/outputs"
	"github.com/stretchr/testify/require"
)

func TestCreateTLSConfigFor_ReloadsClientCertificateWhenFilesChange(t *testing.T) {
	ca, caKey := mustGenCA(t)
	certA, keyA := mustGenLeaf(t, ca, caKey, "client-a", false)
	certB, keyB := mustGenLeaf(t, ca, caKey, "client-b", false)
	certPath, keyPath := writeClientPair(t, certA, keyA)

	o := newInitHTTPHelperOutput()
	cfg := &config{TLS: &types.TLSConfig{CertFile: certPath, KeyFile: keyPath}}
	tlsConfig, certReloader, err := o.createTLSConfigFor(cfg)
	require.NoError(t, err)
	defer certReloader.close()

	require.Empty(t, tlsConfig.Certificates, "OTLP mTLS must not retain a static client certificate")
	require.NotNil(t, tlsConfig.GetClientCertificate)

	gotA, err := tlsConfig.GetClientCertificate(nil)
	require.NoError(t, err)
	require.Equal(t, "client-a", certificateCommonName(t, gotA))

	unchanged, err := tlsConfig.GetClientCertificate(nil)
	require.NoError(t, err)
	require.Same(t, gotA, unchanged, "unchanged files must return the cached certificate")

	// Secret projections can expose the new cert and key a moment apart. A
	// transient mismatched pair retains A, then the completed rotation loads B.
	writeFileWithNewMTime(t, certPath, pemCert(certB))
	duringRotation, err := tlsConfig.GetClientCertificate(nil)
	require.NoError(t, err)
	require.Same(t, gotA, duringRotation, "a partial rotation must retain the last known good certificate")

	writeFileWithNewMTime(t, keyPath, pemKey(keyB))
	gotB, err := tlsConfig.GetClientCertificate(nil)
	require.NoError(t, err)
	require.Equal(t, "client-b", certificateCommonName(t, gotB))
	require.NotSame(t, gotA, gotB)
}

func TestClientCertificateReloader_ProactiveInterval(t *testing.T) {
	ca, caKey := mustGenCA(t)
	certA, keyA := mustGenLeaf(t, ca, caKey, "client-a", false)
	certB, keyB := mustGenLeaf(t, ca, caKey, "client-b", false)
	certPath, keyPath := writeClientPair(t, certA, keyA)

	r, err := newClientCertificateReloader(certPath, keyPath, 10*time.Millisecond, nil)
	require.NoError(t, err)
	defer r.close()

	writeFileWithNewMTime(t, certPath, pemCert(certB))
	writeFileWithNewMTime(t, keyPath, pemKey(keyB))

	// Do not invoke GetClientCertificate here: the interval itself must update
	// the cache, even though an established connection will keep using A until
	// it reconnects and performs a new TLS handshake.
	require.Eventually(t, func() bool {
		return cachedCertificateCommonName(r) == "client-b"
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, "client-b", cachedCertificateCommonName(r))
}

func TestInitHTTPFor_ClientCertificateReloadsOnReconnect(t *testing.T) {
	peerNames := make(chan string, 2)
	srv := newMTLSTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
			http.Error(w, "missing peer certificate", http.StatusUnauthorized)
			return
		}
		peerNames <- r.TLS.PeerCertificates[0].Subject.CommonName
		w.WriteHeader(http.StatusNoContent)
	})
	defer srv.Close()
	srv.allowOnlyClientCertificate(srv.clientCert)

	o := newInitHTTPHelperOutput()
	cfg := &config{
		Endpoint: srv.URL,
		Protocol: "http",
		TLS: &types.TLSConfig{
			CaFile:   srv.CAPath(),
			CertFile: srv.ClientCertPath(),
			KeyFile:  srv.ClientKeyPath(),
		},
	}
	hs, err := o.initHTTPFor(cfg)
	require.NoError(t, err)
	transport := &transportState{httpState: hs}
	defer transport.cleanup()
	state := &outputState{cfg: cfg, transport: transport}

	request := func() string {
		require.Eventually(t, func() bool {
			return o.sendHTTP(context.Background(), state, validExportRequest()) == nil
		}, time.Second, 20*time.Millisecond)
		return <-peerNames
	}

	require.Equal(t, "gnmic-test-client", request())

	certB, keyB := mustGenLeaf(t, srv.caCert, srv.caKey, "client-b", false)
	writeFileWithNewMTime(t, srv.ClientCertPath(), pemCert(certB))
	writeFileWithNewMTime(t, srv.ClientKeyPath(), pemKey(keyB))
	srv.allowOnlyClientCertificate(certB)

	// GetClientCertificate runs only on a handshake. Closing the server-side
	// connection models Panoptes dropping a session authenticated with the old
	// certificate; the existing HTTP transport must reconnect and present B.
	srv.CloseClientConnections()
	require.Equal(t, "client-b", request())
}

func TestTLSReloadIntervalConfig(t *testing.T) {
	cfg := new(config)
	require.NoError(t, outputs.DecodeConfig(map[string]any{
		"tls": map[string]any{"reload-interval": "250ms"},
	}, cfg))
	require.Equal(t, 250*time.Millisecond, cfg.TLS.ReloadInterval)

	oldCfg := &config{TLS: &types.TLSConfig{}}
	newCfg := &config{TLS: &types.TLSConfig{ReloadInterval: time.Second}}
	require.True(t, needsTransportRebuild(oldCfg, newCfg), "changing the interval must restart its transport-owned poller")
	require.True(t, oldCfg.TLS.Equal(newCfg.TLS), "the OTLP-only interval must not alter shared TLS equality semantics")

	o := newInitHTTPHelperOutput()
	invalid := &config{
		Endpoint: "localhost:4317",
		Protocol: "grpc",
		TLS:      &types.TLSConfig{ReloadInterval: -time.Second},
	}
	o.setDefaultsFor(invalid)
	require.ErrorContains(t, o.validateConfig(invalid), "reload-interval")

	incompletePair := &config{
		Endpoint: "localhost:4317",
		Protocol: "grpc",
		TLS:      &types.TLSConfig{CertFile: "/client.crt"},
	}
	o.setDefaultsFor(incompletePair)
	require.ErrorContains(t, o.validateConfig(incompletePair), "must be set together")
}

func TestCreateTLSConfigFor_WithoutClientPairDoesNotInstallReloader(t *testing.T) {
	o := newInitHTTPHelperOutput()

	t.Run("skip_verify_only", func(t *testing.T) {
		cfg := &config{TLS: &types.TLSConfig{SkipVerify: true, ReloadInterval: time.Second}}
		tlsConfig, certReloader, err := o.createTLSConfigFor(cfg)
		require.NoError(t, err)
		require.Nil(t, certReloader)
		require.Nil(t, tlsConfig.GetClientCertificate)
		require.Empty(t, tlsConfig.Certificates)
	})

	t.Run("ca_only", func(t *testing.T) {
		ca, _ := mustGenCA(t)
		caPath := filepath.Join(t.TempDir(), "ca.crt")
		require.NoError(t, os.WriteFile(caPath, pemCert(ca), 0o600))
		cfg := &config{TLS: &types.TLSConfig{CaFile: caPath, ReloadInterval: time.Second}}

		tlsConfig, certReloader, err := o.createTLSConfigFor(cfg)
		require.NoError(t, err)
		require.Nil(t, certReloader)
		require.NotNil(t, tlsConfig.RootCAs)
		require.Nil(t, tlsConfig.GetClientCertificate)
		require.Empty(t, tlsConfig.Certificates)
	})
}

func TestCreateTLSConfigFor_InvalidClientPairFails(t *testing.T) {
	o := newInitHTTPHelperOutput()

	t.Run("incomplete", func(t *testing.T) {
		_, certReloader, err := o.createTLSConfigFor(&config{
			TLS: &types.TLSConfig{CertFile: "/client.crt"},
		})
		require.ErrorContains(t, err, "must be set together")
		require.Nil(t, certReloader)
	})

	t.Run("mismatched_initial_pair", func(t *testing.T) {
		ca, caKey := mustGenCA(t)
		certA, _ := mustGenLeaf(t, ca, caKey, "client-a", false)
		_, keyB := mustGenLeaf(t, ca, caKey, "client-b", false)
		certPath, keyPath := writeClientPair(t, certA, keyB)

		_, certReloader, err := o.createTLSConfigFor(&config{
			TLS: &types.TLSConfig{CertFile: certPath, KeyFile: keyPath},
		})
		require.Error(t, err)
		require.Nil(t, certReloader)
	})
}

func writeClientPair(t *testing.T, cert *x509.Certificate, key *ecdsa.PrivateKey) (string, string) {
	t.Helper()
	dir := t.TempDir()
	certPath := filepath.Join(dir, "client.crt")
	keyPath := filepath.Join(dir, "client.key")
	require.NoError(t, os.WriteFile(certPath, pemCert(cert), 0o600))
	require.NoError(t, os.WriteFile(keyPath, pemKey(key), 0o600))
	return certPath, keyPath
}

func writeFileWithNewMTime(t *testing.T, path string, data []byte) {
	t.Helper()
	previous, err := os.Stat(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o600))

	modTime := time.Now().Add(time.Second)
	if !modTime.After(previous.ModTime()) {
		modTime = previous.ModTime().Add(time.Second)
	}
	require.NoError(t, os.Chtimes(path, modTime, modTime))
}

func certificateCommonName(t *testing.T, certificate *tls.Certificate) string {
	t.Helper()
	require.NotNil(t, certificate)
	require.NotEmpty(t, certificate.Certificate)
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	require.NoError(t, err)
	return leaf.Subject.CommonName
}

func cachedCertificateCommonName(r *clientCertificateReloader) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.certificate == nil || len(r.certificate.Certificate) == 0 {
		return ""
	}
	leaf, err := x509.ParseCertificate(r.certificate.Certificate[0])
	if err != nil {
		return ""
	}
	return leaf.Subject.CommonName
}
