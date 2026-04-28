// © 2026 NVIDIA Corporation
//
// This code is a Contribution to the gNMIc project ("Work") made under the Google Software Grant and Corporate Contributor License Agreement ("CLA") and governed by the Apache License 2.0.
// No other rights or licenses in or to any of NVIDIA's intellectual property are granted for any other purpose.
// This code is provided on an "as is" basis without any warranties of any kind.
//
// SPDX-License-Identifier: Apache-2.0

package otlp_output

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMTLSHarness_ClientCanReachServer(t *testing.T) {
	srv := newMTLSTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	defer srv.Close()

	client := srv.NewClient()
	resp, err := client.Get(srv.URL + "/ping")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestMTLSHarness_ClientWithoutCertIsRejected(t *testing.T) {
	srv := newMTLSTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	defer srv.Close()

	// Client with TLS but NO client cert → handshake should fail.
	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: srv.CAPool()},
	}}
	_, err := client.Get(srv.URL + "/ping")
	require.Error(t, err, "server must reject client with no cert")
}

// mTLSTestServer is an httptest server that requires a client cert
// issued by its embedded CA. Cert/key files are materialized to tmpdir
// so they can be passed through TLSConfig.CaFile / CertFile / KeyFile.
type mTLSTestServer struct {
	*httptest.Server
	caPool      *x509.CertPool
	caPEMPath   string
	cliCertPEM  []byte
	cliKeyPEM   []byte
	cliCertPath string
	cliKeyPath  string
}

func newMTLSTestServer(t *testing.T, handler http.HandlerFunc) *mTLSTestServer {
	t.Helper()

	caCert, caKey := mustGenCA(t)
	serverCert, serverKey := mustGenLeaf(t, caCert, caKey, "localhost", true)
	clientCert, clientKey := mustGenLeaf(t, caCert, caKey, "gnmic-test-client", false)

	caPool := x509.NewCertPool()
	caPool.AddCert(caCert)

	srvTLSCert, err := tls.X509KeyPair(pemCert(serverCert), pemKey(serverKey))
	require.NoError(t, err)

	srv := httptest.NewUnstartedServer(handler)
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{srvTLSCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
		MinVersion:   tls.VersionTLS12,
	}
	// Suppress expected TLS-handshake-rejection noise on stderr during tests
	// like TestMTLSHarness_ClientWithoutCertIsRejected. Future tests reusing
	// this harness benefit automatically.
	srv.Config.ErrorLog = log.New(io.Discard, "", 0)
	srv.StartTLS()

	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.crt")
	cliCertPath := filepath.Join(dir, "client.crt")
	cliKeyPath := filepath.Join(dir, "client.key")
	require.NoError(t, os.WriteFile(caPath, pemCert(caCert), 0o600))
	require.NoError(t, os.WriteFile(cliCertPath, pemCert(clientCert), 0o600))
	require.NoError(t, os.WriteFile(cliKeyPath, pemKey(clientKey), 0o600))

	return &mTLSTestServer{
		Server:      srv,
		caPool:      caPool,
		caPEMPath:   caPath,
		cliCertPEM:  pemCert(clientCert),
		cliKeyPEM:   pemKey(clientKey),
		cliCertPath: cliCertPath,
		cliKeyPath:  cliKeyPath,
	}
}

func (s *mTLSTestServer) CAPool() *x509.CertPool { return s.caPool }
func (s *mTLSTestServer) CAPath() string         { return s.caPEMPath }
func (s *mTLSTestServer) ClientCertPath() string { return s.cliCertPath }
func (s *mTLSTestServer) ClientKeyPath() string  { return s.cliKeyPath }

// NewClient returns a plain http.Client with the server CA trusted and
// the client cert loaded — useful for harness self-tests.
func (s *mTLSTestServer) NewClient() *http.Client {
	cliCert, _ := tls.X509KeyPair(s.cliCertPEM, s.cliKeyPEM)
	return &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs:      s.caPool,
			Certificates: []tls.Certificate{cliCert},
			MinVersion:   tls.VersionTLS12,
		},
	}}
}

func mustGenCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "gnmic-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return cert, key
}

func mustGenLeaf(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, cn string, isServer bool) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		// ECDSA keys: KeyUsageDigitalSignature only. KeyUsageKeyEncipherment
		// is RSA-specific (RFC 5246) and meaningless for EC keys; the Go stdlib
		// generate_cert.go documents this explicitly.
		KeyUsage: x509.KeyUsageDigitalSignature,
	}
	if isServer {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		// httptest.Server.URL is https://127.0.0.1:<port> (or [::1] for v6),
		// so the server cert must have IP SANs as well as the DNS SAN.
		tmpl.DNSNames = []string{"localhost"}
		tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}
	} else {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return cert, key
}

func pemCert(c *x509.Certificate) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: c.Raw})
}

func pemKey(k *ecdsa.PrivateKey) []byte {
	der, _ := x509.MarshalECPrivateKey(k)
	return pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der})
}

func TestResolveMetricsURL(t *testing.T) {
	cases := []struct {
		name     string
		endpoint string
		tls      bool
		want     string
		wantErr  bool
	}{
		{"bare_host_port_tls", "panoptes.example.com:4318", true, "https://panoptes.example.com:4318/v1/metrics", false},
		{"bare_host_port_plain", "localhost:4318", false, "http://localhost:4318/v1/metrics", false},
		{"full_https_url_with_path", "https://panoptes.example.com/api/v1/metrics", true, "https://panoptes.example.com/api/v1/metrics", false},
		{"full_http_url_no_path", "http://localhost:4318", false, "http://localhost:4318/v1/metrics", false},
		{"full_https_url_no_path_appends_default", "https://panoptes.example.com:4318", true, "https://panoptes.example.com:4318/v1/metrics", false},
		{"full_url_with_root_slash_path", "https://panoptes.example.com/", true, "https://panoptes.example.com/v1/metrics", false},
		{"empty_endpoint", "", false, "", true},
		{"url_with_whitespace", " https://x.com ", true, "https://x.com/v1/metrics", false},
		// Decision-path: explicit URL with malformed structure must reach url.Parse failure.
		{"malformed_full_url", "http://[::1", false, "", true},
		// Decision-path: synthesized bare endpoints must also be validated, otherwise garbage
		// like "foo bar:4318" survives Init and only fails much later inside http.NewRequestWithContext.
		{"bare_endpoint_with_space_rejected", "foo bar:4318", false, "", true},
		{"bare_endpoint_with_control_char_rejected", "foo\nbar:4318", false, "", true},
		// Decision-path: per the OTLP exporter spec, only http and https schemes are valid.
		{"unsupported_scheme_ftp", "ftp://example.com:4318", false, "", true},
		{"unsupported_scheme_grpc", "grpc://example.com:4317", false, "", true},
		// Decision-path: port-only ":4318" must NOT pass — Hostname() check catches it.
		{"port_only_bare_rejected", ":4318", false, "", true},
		// Decision-path: bare endpoint must be host:port only; paths require a full URL.
		{"bare_endpoint_with_path_rejected", "localhost:4318/custom", false, "", true},
		// Decision-path: userinfo in URL must be rejected — auth goes through Headers config.
		{"userinfo_rejected", "https://user:pass@host:4318", true, "", true},
		// Positive: IPv6 bare and full URLs must work end-to-end.
		{"ipv6_bare_with_brackets", "[::1]:4318", false, "http://[::1]:4318/v1/metrics", false},
		{"ipv6_full_url", "https://[::1]:4318", true, "https://[::1]:4318/v1/metrics", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveMetricsURL(tc.endpoint, tc.tls)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
