// © 2026 NVIDIA Corporation
//
// This code is a Contribution to the gNMIc project ("Work") made under the Google Software Grant and Corporate Contributor License Agreement ("CLA") and governed by the Apache License 2.0.
// No other rights or licenses in or to any of NVIDIA's intellectual property are granted for any other purpose.
// This code is provided on an "as is" basis without any warranties of any kind.
//
// SPDX-License-Identifier: Apache-2.0

package otlp_output

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openconfig/gnmic/pkg/api/types"
	"github.com/openconfig/gnmic/pkg/formatters"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	metricsv1 "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"
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

func TestInitHTTPFor_WithMTLS(t *testing.T) {
	srv := newMTLSTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	defer srv.Close()

	o := &otlpOutput{}
	o.cfg = new(atomic.Pointer[config])
	o.logger = log.New(io.Discard, "", 0)
	cfg := &config{
		Endpoint: srv.URL, // full https URL from httptest
		Protocol: "http",
		TLS: &types.TLSConfig{
			CaFile:   srv.CAPath(),
			CertFile: srv.ClientCertPath(),
			KeyFile:  srv.ClientKeyPath(),
		},
		Headers: map[string]string{"X-Scope-OrgID": "tenant-1"},
	}
	o.cfg.Store(cfg)

	hs, err := o.initHTTPFor(cfg)
	require.NoError(t, err)
	require.NotNil(t, hs)
	require.NotNil(t, hs.client)
	require.Contains(t, hs.endpoint, "/v1/metrics")
	require.Equal(t, "application/x-protobuf", hs.headers.Get("Content-Type"))
	require.Equal(t, "tenant-1", hs.headers.Get("X-Scope-OrgID"))
}

func TestInitHTTPFor_NoTLSBlock(t *testing.T) {
	o := &otlpOutput{}
	o.cfg = new(atomic.Pointer[config])
	o.logger = log.New(io.Discard, "", 0)
	cfg := &config{Endpoint: "localhost:4318", Protocol: "http"}
	o.cfg.Store(cfg)

	hs, err := o.initHTTPFor(cfg)
	require.NoError(t, err)
	require.Equal(t, "http://localhost:4318/v1/metrics", hs.endpoint)
}

// Strict-coherence guard: configuring TLS while pinning a plaintext URL is
// almost always a misconfiguration that silently disables mTLS. Reject at Init.
func TestInitHTTPFor_PlaintextURLWithTLSBlockErrors(t *testing.T) {
	o := &otlpOutput{}
	o.cfg = new(atomic.Pointer[config])
	o.logger = log.New(io.Discard, "", 0)
	cfg := &config{
		Endpoint: "http://panoptes.observability.svc:4318",
		Protocol: "http",
		TLS:      &types.TLSConfig{CaFile: "/some/ca.crt"},
	}
	o.cfg.Store(cfg)

	_, err := o.initHTTPFor(cfg)
	require.Error(t, err, "must reject http:// URL when tls block is configured")
	require.Contains(t, err.Error(), "tls")
}

// Mirror case stays permissive: https:// URL with no tls block uses system roots.
func TestInitHTTPFor_HTTPSURLWithoutTLSBlockAllowed(t *testing.T) {
	o := &otlpOutput{}
	o.cfg = new(atomic.Pointer[config])
	o.logger = log.New(io.Discard, "", 0)
	cfg := &config{Endpoint: "https://panoptes.example.com/v1/metrics", Protocol: "http"}
	o.cfg.Store(cfg)

	hs, err := o.initHTTPFor(cfg)
	require.NoError(t, err)
	require.Equal(t, "https://panoptes.example.com/v1/metrics", hs.endpoint)
}

// Decision-path: createTLSConfigFor failure must propagate as a clear Init error.
func TestInitHTTPFor_TLSCreateFails(t *testing.T) {
	o := &otlpOutput{}
	o.cfg = new(atomic.Pointer[config])
	o.logger = log.New(io.Discard, "", 0)
	cfg := &config{
		Endpoint: "https://panoptes.example.com:4318",
		Protocol: "http",
		TLS: &types.TLSConfig{
			CaFile:   "/nonexistent/path/ca.crt",
			CertFile: "/nonexistent/path/client.crt",
			KeyFile:  "/nonexistent/path/client.key",
		},
	}
	o.cfg.Store(cfg)

	_, err := o.initHTTPFor(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "TLS")
}

// Decision-path: empty endpoint reaching resolveMetricsURL must surface as Init error.
func TestInitHTTPFor_EmptyEndpointErrors(t *testing.T) {
	o := &otlpOutput{}
	o.cfg = new(atomic.Pointer[config])
	o.logger = log.New(io.Discard, "", 0)
	cfg := &config{Endpoint: "", Protocol: "http"}
	o.cfg.Store(cfg)

	_, err := o.initHTTPFor(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "endpoint is required")
}

func TestSendHTTP_SuccessfulExport(t *testing.T) {
	var gotBody []byte
	var gotContentType, gotOrgID string

	srv := newMTLSTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, otlpHTTPMetricsPath, r.URL.Path)
		gotContentType = r.Header.Get("Content-Type")
		gotOrgID = r.Header.Get("X-Scope-OrgID")
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	})
	defer srv.Close()

	o := &otlpOutput{}
	o.cfg = new(atomic.Pointer[config])
	o.httpState = new(atomic.Pointer[httpClientState])
	o.logger = log.New(io.Discard, "", 0)

	cfg := &config{
		Endpoint: srv.URL,
		Protocol: "http",
		Timeout:  5 * time.Second,
		TLS: &types.TLSConfig{
			CaFile:   srv.CAPath(),
			CertFile: srv.ClientCertPath(),
			KeyFile:  srv.ClientKeyPath(),
		},
		Headers: map[string]string{"X-Scope-OrgID": "tenant-1"},
	}
	o.cfg.Store(cfg)

	hs, err := o.initHTTPFor(cfg)
	require.NoError(t, err)
	o.httpState.Store(hs)

	// Minimal valid ExportMetricsServiceRequest.
	req := &metricsv1.ExportMetricsServiceRequest{}
	require.NoError(t, o.sendHTTP(context.Background(), req))

	require.Equal(t, "application/x-protobuf", gotContentType)
	require.Equal(t, "tenant-1", gotOrgID)

	// Round-trip check: unmarshal body and compare.
	var roundtrip metricsv1.ExportMetricsServiceRequest
	require.NoError(t, proto.Unmarshal(gotBody, &roundtrip))
}

// withCfg applies a mutation to a copy of the current config and atomically
// swaps it back. Test-only convenience — production reloads via Update().
// Direct mutation of o.cfg.Load() (e.g. `o.cfg.Load().Timeout = 0`) is an
// unsynchronized write that defeats atomic.Pointer; this helper does it safely.
func withCfg(o *otlpOutput, mutate func(*config)) {
	cfg := *o.cfg.Load()
	mutate(&cfg)
	o.cfg.Store(&cfg)
}

// newHTTPTestOutput is shared by every test that drives sendHTTP through a real
// httptest server. Defining it here in Task 5 so subsequent tasks can use it
// without re-introducing.
func newHTTPTestOutput(t *testing.T, srv *mTLSTestServer) *otlpOutput {
	t.Helper()
	o := &otlpOutput{}
	o.cfg = new(atomic.Pointer[config])
	o.httpState = new(atomic.Pointer[httpClientState])
	o.logger = log.New(io.Discard, "", 0)
	cfg := &config{
		Endpoint: srv.URL,
		Protocol: "http",
		Timeout:  2 * time.Second,
		TLS: &types.TLSConfig{
			CaFile:   srv.CAPath(),
			CertFile: srv.ClientCertPath(),
			KeyFile:  srv.ClientKeyPath(),
		},
	}
	o.cfg.Store(cfg)
	hs, err := o.initHTTPFor(cfg)
	require.NoError(t, err)
	o.httpState.Store(hs)
	return o
}

// Decision-path: defensive guard for "Init never ran or partially failed".
func TestSendHTTP_NoStateInitialized(t *testing.T) {
	o := &otlpOutput{}
	o.cfg = new(atomic.Pointer[config])
	o.httpState = new(atomic.Pointer[httpClientState]) // never Stored
	o.logger = log.New(io.Discard, "", 0)
	o.cfg.Store(&config{Protocol: "http", Endpoint: "https://x"})

	err := o.sendHTTP(context.Background(), &metricsv1.ExportMetricsServiceRequest{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "not initialized")
}

// Decision-path: cfg.Timeout == 0 skips the WithTimeout branch.
// We start an mTLS server, send normally, and just verify success — exercising
// the no-timeout path. Hangs would be caught by the test runner's overall timeout.
func TestSendHTTP_NoTimeoutConfigured(t *testing.T) {
	srv := newMTLSTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	defer srv.Close()

	o := newHTTPTestOutput(t, srv)
	withCfg(o, func(c *config) { c.Timeout = 0 })

	require.NoError(t, o.sendHTTP(context.Background(), &metricsv1.ExportMetricsServiceRequest{}))
}

// Decision-path: client.Do failure (transport error). Stop the server first so
// the dial fails — this exercises the "HTTP export failed" branch without
// hitting the response classification code.
func TestSendHTTP_DialFailure(t *testing.T) {
	srv := newMTLSTestServer(t, func(w http.ResponseWriter, r *http.Request) {})
	o := newHTTPTestOutput(t, srv)
	srv.Close() // close before the call so dial fails

	err := o.sendHTTP(context.Background(), &metricsv1.ExportMetricsServiceRequest{})
	require.Error(t, err)
}

// Decision-path: proto.Marshal fails when a string field contains invalid UTF-8
// (proto3 requires UTF-8). The server handler must NOT be reached because the
// failure is local to the marshal step.
func TestSendHTTP_MarshalRejectsInvalidUTF8(t *testing.T) {
	srv := newMTLSTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("server must not receive a request when proto.Marshal fails")
	})
	defer srv.Close()

	o := newHTTPTestOutput(t, srv)
	invalid := string([]byte{0xff, 0xfe, 0xfd}) // not valid UTF-8
	req := &metricsv1.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{
				ScopeMetrics: []*metricspb.ScopeMetrics{
					{
						Metrics: []*metricspb.Metric{
							{Name: invalid},
						},
					},
				},
			},
		},
	}
	err := o.sendHTTP(context.Background(), req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "marshal OTLP request")
}

// Table-driven decision-path coverage for isPermanentHTTPError.
// Per OTLP spec (https://opentelemetry.io/docs/specs/otlp/), retryable status
// codes are 429, 502, 503, 504. All other 4xx and 5xx are permanent.
// Transport-level errors (errors.As fails to find *httpExportError) are
// treated as retryable — typically transient network blips.
func TestIsPermanentHTTPError_Table(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		permanent bool
	}{
		// Transport / wrapped non-HTTP errors → retryable.
		{"transport_error", fmt.Errorf("dial tcp: connection refused"), false},
		{"nil_error", nil, false}, // nil isn't permanent (caller checks err != nil first)

		// Retryable per OTLP spec.
		{"status_429_too_many_requests", &httpExportError{status: 429}, false},
		{"status_502_bad_gateway", &httpExportError{status: 502}, false},
		{"status_503_service_unavailable", &httpExportError{status: 503}, false},
		{"status_504_gateway_timeout", &httpExportError{status: 504}, false},

		// Permanent (4xx and 5xx not in retryable set).
		{"status_400_bad_request", &httpExportError{status: 400}, true},
		{"status_401_unauthorized", &httpExportError{status: 401}, true},
		{"status_403_forbidden", &httpExportError{status: 403}, true},
		{"status_404_not_found", &httpExportError{status: 404}, true},
		{"status_408_request_timeout_permanent_per_otlp", &httpExportError{status: 408}, true},
		{"status_500_internal_server_error", &httpExportError{status: 500}, true},
		{"status_501_not_implemented", &httpExportError{status: 501}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.permanent, isPermanentHTTPError(tc.err))
		})
	}
}

// Sanity check that the integration with a live server matches the table:
// permanent 400 surfaces as permanent, retryable 503 does not, and sendHTTP
// itself never retries (the loop is in sendBatch).
func TestSendHTTP_PermanentErrorNotRetried(t *testing.T) {
	var calls int
	srv := newMTLSTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusBadRequest) // 400
	})
	defer srv.Close()

	o := newHTTPTestOutput(t, srv)
	err := o.sendHTTP(context.Background(), &metricsv1.ExportMetricsServiceRequest{})
	require.Error(t, err)
	require.True(t, isPermanentHTTPError(err))
	require.Equal(t, 1, calls, "sendHTTP itself should not retry")
}

func TestSendHTTP_RetryableError(t *testing.T) {
	srv := newMTLSTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable) // 503
	})
	defer srv.Close()

	o := newHTTPTestOutput(t, srv)
	err := o.sendHTTP(context.Background(), &metricsv1.ExportMetricsServiceRequest{})
	require.Error(t, err)
	require.False(t, isPermanentHTTPError(err))
}

// Table-driven decision-path coverage for parseRetryAfter (RFC 7231 §7.1.3).
func TestParseRetryAfter_Table(t *testing.T) {
	now := time.Date(2026, 4, 27, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		in   string
		want time.Duration
	}{
		{"empty", "", 0},
		{"whitespace_only", "   ", 0},
		{"zero_seconds", "0", 0},
		{"five_seconds", "5", 5 * time.Second},
		{"surrounded_by_whitespace", "  5  ", 5 * time.Second},
		{"negative_seconds_clamped_to_zero", "-1", 0},
		{"unparseable", "not-a-number", 0},
		{"http_date_in_future", now.Add(10 * time.Second).UTC().Format(http.TimeFormat), 10 * time.Second},
		{"http_date_in_past", now.Add(-time.Hour).UTC().Format(http.TimeFormat), 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseRetryAfter(tc.in, now)
			// HTTP-date precision is seconds; allow a tiny tolerance to absorb formatting jitter.
			diff := got - tc.want
			if diff < 0 {
				diff = -diff
			}
			require.Less(t, diff, time.Second, "got=%v want=%v", got, tc.want)
		})
	}
}

func TestSendHTTP_PartialSuccessReturnsError(t *testing.T) {
	srv := newMTLSTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		// Build a 200 response containing a PartialSuccess with rejected points.
		respProto := &metricsv1.ExportMetricsServiceResponse{
			PartialSuccess: &metricsv1.ExportMetricsPartialSuccess{
				RejectedDataPoints: 7,
				ErrorMessage:       "schema validation failed for 7 points",
			},
		}
		body, err := proto.Marshal(respProto)
		require.NoError(t, err)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	})
	defer srv.Close()

	o := newHTTPTestOutput(t, srv)
	err := o.sendHTTP(context.Background(), &metricsv1.ExportMetricsServiceRequest{})
	require.Error(t, err, "PartialSuccess with rejected points must surface as an error (parity with gRPC)")
	require.Contains(t, err.Error(), "rejected 7")
}

// Decision-path: PartialSuccess present but RejectedDataPoints == 0 must NOT
// surface as an error. This is the "informational" PartialSuccess case (e.g.
// server reporting metadata about a fully-accepted batch).
func TestSendHTTP_PartialSuccessZeroRejected(t *testing.T) {
	srv := newMTLSTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		respProto := &metricsv1.ExportMetricsServiceResponse{
			PartialSuccess: &metricsv1.ExportMetricsPartialSuccess{
				RejectedDataPoints: 0,
				ErrorMessage:       "informational",
			},
		}
		body, _ := proto.Marshal(respProto)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	})
	defer srv.Close()

	o := newHTTPTestOutput(t, srv)
	require.NoError(t, o.sendHTTP(context.Background(), &metricsv1.ExportMetricsServiceRequest{}))
}

// Decision-path: empty 200 body must succeed (the early-return short-circuit).
func TestSendHTTP_EmptyResponseBody(t *testing.T) {
	srv := newMTLSTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK) // no body written
	})
	defer srv.Close()

	o := newHTTPTestOutput(t, srv)
	require.NoError(t, o.sendHTTP(context.Background(), &metricsv1.ExportMetricsServiceRequest{}))
}

// Decision-path (debug off): malformed (non-protobuf) 200 body must NOT cause
// an error; the transport-level success is authoritative. The Debug branch
// is silent in this variant.
func TestSendHTTP_MalformedResponseBody_DebugOff(t *testing.T) {
	srv := newMTLSTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("this is definitely not a protobuf"))
	})
	defer srv.Close()

	o := newHTTPTestOutput(t, srv)
	require.False(t, o.cfg.Load().Debug, "precondition: debug must be off for this variant")
	require.NoError(t, o.sendHTTP(context.Background(), &metricsv1.ExportMetricsServiceRequest{}))
}

// Decision-path (debug on): same scenario but Debug enabled — exercises the
// `if cfg.Debug { o.logger.Printf(...) }` branch. Captures log output via a
// buffer to verify the warning was emitted.
func TestSendHTTP_MalformedResponseBody_DebugOn(t *testing.T) {
	srv := newMTLSTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("not a protobuf"))
	})
	defer srv.Close()

	var logbuf bytes.Buffer
	o := newHTTPTestOutput(t, srv)
	o.logger = log.New(&logbuf, "", 0)
	withCfg(o, func(c *config) { c.Debug = true })

	require.NoError(t, o.sendHTTP(context.Background(), &metricsv1.ExportMetricsServiceRequest{}))
	require.Contains(t, logbuf.String(), "failed to unmarshal OTLP response body")
}

// Decision-path: when EnableMetrics is set, PartialSuccess rejections must
// increment the otlpRejectedDataPoints counter (parity with gRPC path).
func TestSendHTTP_PartialSuccessIncrementsMetric(t *testing.T) {
	srv := newMTLSTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		respProto := &metricsv1.ExportMetricsServiceResponse{
			PartialSuccess: &metricsv1.ExportMetricsPartialSuccess{
				RejectedDataPoints: 3,
				ErrorMessage:       "rejected 3",
			},
		}
		body, _ := proto.Marshal(respProto)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	})
	defer srv.Close()

	o := newHTTPTestOutput(t, srv)
	withCfg(o, func(c *config) {
		c.EnableMetrics = true
		c.Name = "test-metric-output"
	})

	cfgName := o.cfg.Load().Name
	before := testutil.ToFloat64(otlpRejectedDataPoints.WithLabelValues(cfgName))
	err := o.sendHTTP(context.Background(), &metricsv1.ExportMetricsServiceRequest{})
	require.Error(t, err)
	after := testutil.ToFloat64(otlpRejectedDataPoints.WithLabelValues(cfgName))
	require.Equal(t, float64(3), after-before, "metric must advance by RejectedDataPoints")
}

func TestSendHTTP_RetryAfterHeaderHonored(t *testing.T) {
	start := time.Now()
	var callTimes []time.Time
	var calls int
	srv := newMTLSTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		callTimes = append(callTimes, time.Now())
		calls++
		if calls <= 1 {
			w.Header().Set("Retry-After", "1") // 1 second
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	defer srv.Close()

	o := newHTTPTestOutput(t, srv)
	withCfg(o, func(c *config) {
		c.MaxRetries = 3
		c.resourceTagSet = map[string]bool{}
	})

	// Build one event and run through the public sendBatch path so we exercise the retry loop.
	events := []*formatters.EventMsg{{
		Name: "test", Timestamp: time.Now().UnixNano(),
		Tags:   map[string]string{"source": "x"},
		Values: map[string]interface{}{"value": int64(1)},
	}}
	o.sendBatch(context.Background(), events)

	require.GreaterOrEqual(t, calls, 2, "should retry at least once")
	require.GreaterOrEqual(t, time.Since(start), 1*time.Second, "must wait >=1s per Retry-After")
}

func TestSendHTTP_PermanentErrorStopsRetryLoop(t *testing.T) {
	var calls int
	srv := newMTLSTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusBadRequest)
	})
	defer srv.Close()

	o := newHTTPTestOutput(t, srv)
	withCfg(o, func(c *config) {
		c.MaxRetries = 5
		c.resourceTagSet = map[string]bool{}
	})

	events := []*formatters.EventMsg{{
		Name: "test", Timestamp: time.Now().UnixNano(),
		Tags:   map[string]string{"source": "x"},
		Values: map[string]interface{}{"value": int64(1)},
	}}
	o.sendBatch(context.Background(), events)

	require.Equal(t, 1, calls, "permanent 400 must not retry")
}

// Table-driven decision-path coverage for effectiveRetrySleep.
func TestEffectiveRetrySleep_Table(t *testing.T) {
	const testCap = 30 * time.Second
	cases := []struct {
		name       string
		protocol   string
		attempt    int
		retryAfter time.Duration
		want       time.Duration
	}{
		{"grpc_attempt0_no_retry_after", "grpc", 0, 0, 100 * time.Millisecond},
		{"grpc_attempt2_no_retry_after", "grpc", 2, 0, 300 * time.Millisecond},
		{"grpc_ignores_retry_after", "grpc", 0, 5 * time.Second, 100 * time.Millisecond},
		{"http_attempt0_no_retry_after", "http", 0, 0, 100 * time.Millisecond},
		{"http_attempt1_falls_back_to_backoff", "http", 1, 0, 200 * time.Millisecond},
		{"http_zero_retry_after_falls_back", "http", 0, 0, 100 * time.Millisecond},
		{"http_negative_retry_after_falls_back", "http", 0, -time.Second, 100 * time.Millisecond},
		{"http_retry_after_under_cap_used", "http", 0, 5 * time.Second, 5 * time.Second},
		{"http_retry_after_at_cap_used", "http", 0, testCap, testCap},
		{"http_retry_after_above_cap_clamped", "http", 0, 5 * time.Minute, testCap},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := effectiveRetrySleep(tc.protocol, tc.attempt, tc.retryAfter, testCap)
			require.Equal(t, tc.want, got)
		})
	}
}

// Decision-path: retryAfterFromError must return 0 for plain (non-typed) errors.
// The Retry-After-honoring tests cover the typed-error path.
func TestRetryAfterFromError_NonHTTPError(t *testing.T) {
	require.Equal(t, time.Duration(0), retryAfterFromError(fmt.Errorf("plain error")))
	require.Equal(t, time.Duration(0), retryAfterFromError(nil))
}

// Decision-path: a Retry-After sleep must be interruptible by ctx cancellation
// so Close() during a retry doesn't pin the worker.
func TestSendBatch_ContextCancelledDuringRetryWait(t *testing.T) {
	srv := newMTLSTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		// Always 503 with a long Retry-After so the loop wants to sleep >=1s.
		w.Header().Set("Retry-After", "5")
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	defer srv.Close()

	o := newHTTPTestOutput(t, srv)
	withCfg(o, func(c *config) {
		c.MaxRetries = 5
		c.resourceTagSet = map[string]bool{}
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(150 * time.Millisecond) // give first attempt time to fail and start sleeping
		cancel()
	}()

	events := []*formatters.EventMsg{{
		Name: "test", Timestamp: time.Now().UnixNano(),
		Tags:   map[string]string{"source": "x"},
		Values: map[string]interface{}{"value": int64(1)},
	}}

	start := time.Now()
	o.sendBatch(ctx, events)
	elapsed := time.Since(start)

	// Cancellation should land well before the 5-second Retry-After elapses.
	require.Less(t, elapsed, 2*time.Second, "ctx cancel must interrupt the retry sleep")
}
