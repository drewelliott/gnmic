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
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	metricsv1 "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/protobuf/proto"
)

const otlpHTTPMetricsPath = "/v1/metrics"

// httpExportError carries the HTTP status code alongside the underlying error,
// so the caller can distinguish retryable (429/5xx) from permanent (4xx) failures.
type httpExportError struct {
	status     int
	retryAfter time.Duration // populated from Retry-After header if present; 0 otherwise
	msg        string
}

func (e *httpExportError) Error() string {
	return fmt.Sprintf("HTTP export returned status %d: %s", e.status, e.msg)
}

// isPermanentHTTPError returns true when the error should NOT be retried.
// Per OTLP spec (https://opentelemetry.io/docs/specs/otlp/), retryable status
// codes are 429 Too Many Requests, 502 Bad Gateway, 503 Service Unavailable,
// and 504 Gateway Timeout. All other 4xx and 5xx are permanent.
// Transport-level errors (no *httpExportError) are retryable — typically
// transient network blips.
func isPermanentHTTPError(err error) bool {
	var hee *httpExportError
	if !errors.As(err, &hee) {
		return false
	}
	switch hee.status {
	case http.StatusTooManyRequests,    // 429
		http.StatusBadGateway,          // 502
		http.StatusServiceUnavailable,  // 503
		http.StatusGatewayTimeout:      // 504
		return false
	default:
		return true
	}
}

// parseRetryAfter interprets the Retry-After header per RFC 7231 §7.1.3.
// It accepts either a delta-seconds integer or an HTTP-date.
// Returns 0 when the header is absent, empty, or unparseable.
func parseRetryAfter(h string, now time.Time) time.Duration {
	h = strings.TrimSpace(h)
	if h == "" {
		return 0
	}
	// Delta-seconds form.
	if secs, err := strconv.Atoi(h); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}
	// HTTP-date form.
	if t, err := http.ParseTime(h); err == nil {
		d := t.Sub(now)
		if d < 0 {
			return 0
		}
		return d
	}
	return 0
}

// resolveMetricsURL normalizes a user-supplied endpoint into the absolute URL
// that sendHTTP will POST to. It accepts either a bare "host:port"
// (in which case scheme is chosen from tlsEnabled and path defaults to /v1/metrics)
// or a full URL (scheme is preserved; if no path is set, /v1/metrics is appended).
//
// Both forms are revalidated by url.Parse before return: a bare endpoint that
// would synthesize an unparseable URL (e.g. embedded whitespace or control
// characters) is rejected here so the failure surfaces at Init time, not at
// the first send. This also makes http.NewRequestWithContext in sendHTTP
// unreachable for malformed inputs (see Appendix D).
func resolveMetricsURL(endpoint string, tlsEnabled bool) (string, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return "", fmt.Errorf("endpoint is required")
	}

	// If no scheme, synthesize one. The bare form is strictly host:port — a
	// path component here is a configuration mistake (e.g. "localhost:4318/foo")
	// that we reject rather than silently mangle into "/foo/v1/metrics".
	if !strings.Contains(endpoint, "://") {
		if strings.ContainsRune(endpoint, '/') {
			return "", fmt.Errorf("bare endpoint %q must be host:port only; use a full URL if a path is needed", endpoint)
		}
		scheme := "http"
		if tlsEnabled {
			scheme = "https"
		}
		endpoint = scheme + "://" + endpoint + otlpHTTPMetricsPath
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("invalid endpoint URL %q: %w", endpoint, err)
	}
	// Per the OTLP exporter spec, only http and https are valid schemes.
	// Reject anything else here so misconfigurations fail at Init rather than
	// surfacing as a confusing client.Do error at first send.
	switch strings.ToLower(u.Scheme) {
	case "http", "https":
		// ok
	default:
		return "", fmt.Errorf("unsupported endpoint scheme %q: must be http or https", u.Scheme)
	}
	// Reject userinfo in URL — auth must go through the Headers config field
	// (e.g. "Authorization: Bearer ..."). Userinfo in URLs leaks into logs and
	// error messages and is not how OTLP backends authenticate clients.
	if u.User != nil {
		return "", fmt.Errorf("endpoint must not include userinfo; use the headers config field for authentication")
	}
	// url.Parse is lenient — a Host that contains whitespace or control chars
	// will parse but cannot be used in a real request. Reject explicitly.
	// Use Hostname() (not Host) so a port-only string like ":4318" — which
	// has Host==":4318" but no actual hostname — is rejected. This is the
	// difference between "fails at Init" and "fails on first dial".
	if u.Hostname() == "" || strings.ContainsAny(u.Host, " \t\r\n") {
		return "", fmt.Errorf("invalid endpoint host %q", u.Host)
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = otlpHTTPMetricsPath
	}
	return u.String(), nil
}

// httpClientState is the transport-specific state for protocol: http.
// Stored in an atomic.Pointer on otlpOutput so it can be swapped atomically
// during live config reload.
type httpClientState struct {
	client   *http.Client
	endpoint string
	headers  http.Header
}

func (o *otlpOutput) initHTTPFor(cfg *config) (*httpClientState, error) {
	// Strict-coherence guard (ratified Decision 1): an explicit http:// scheme
	// combined with a configured tls block is almost always a misconfiguration
	// that silently disables mTLS. Reject early with a clear message rather than
	// letting the operator discover this only via packet capture in production.
	endpoint := strings.TrimSpace(cfg.Endpoint)
	if cfg.TLS != nil && strings.HasPrefix(strings.ToLower(endpoint), "http://") {
		return nil, fmt.Errorf("endpoint scheme is http:// but tls block is configured; use https:// (or a bare host:port) or remove the tls block")
	}

	var tlsConfig *tls.Config
	if cfg.TLS != nil {
		var err error
		tlsConfig, err = o.createTLSConfigFor(cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create TLS config: %w", err)
		}
	}

	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
		// TLSHandshakeTimeout matches http.DefaultTransport's value; without this
		// a hung TLS server could stall the dialer indefinitely even though we
		// rely on per-request context deadlines.
		TLSHandshakeTimeout:   10 * time.Second,
		MaxIdleConns:          10,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}

	client := &http.Client{
		Transport: transport,
		// No client-side Timeout: per-request context timeout is the authoritative deadline,
		// matching the gRPC path (which uses context.WithTimeout in sendGRPC).
	}

	// Pass the already-trimmed `endpoint` through (not cfg.Endpoint) so the
	// coherence check above and URL resolution operate on the same string.
	resolvedURL, err := resolveMetricsURL(endpoint, cfg.TLS != nil)
	if err != nil {
		return nil, err
	}

	hdr := http.Header{}
	for k, v := range cfg.Headers {
		hdr.Set(k, v)
	}
	hdr.Set("Content-Type", "application/x-protobuf")
	// Compression header is set per-request in sendHTTP if compression is enabled.

	o.logger.Printf("initialized OTLP HTTP client for endpoint: %s", resolvedURL)
	return &httpClientState{client: client, endpoint: resolvedURL, headers: hdr}, nil
}

func (o *otlpOutput) sendHTTP(ctx context.Context, req *metricsv1.ExportMetricsServiceRequest) error {
	hs := o.httpState.Load()
	if hs == nil {
		return fmt.Errorf("HTTP client not initialized")
	}

	body, err := proto.Marshal(req)
	if err != nil {
		// proto.Marshal can fail for requests containing invalid UTF-8 in string
		// fields (proto3 mandates valid UTF-8). Tested in TestSendHTTP_MarshalRejectsInvalidUTF8.
		return fmt.Errorf("marshal OTLP request: %w", err)
	}

	cfg := o.cfg.Load()
	if cfg.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.Timeout)
		defer cancel()
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, hs.endpoint, bytes.NewReader(body))
	if err != nil {
		// unreachable: hs.endpoint was validated by resolveMetricsURL at Init time;
		// method is a constant; body is a *bytes.Reader. See Appendix D.
		return fmt.Errorf("build HTTP request: %w", err)
	}
	httpReq.Header = hs.headers.Clone()

	resp, err := hs.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("HTTP export failed: %w", err)
	}
	// Defers run LIFO: drain runs before close so the connection is reusable.
	// Drain happens AFTER any deliberate read of the body below — order matters.
	defer resp.Body.Close()
	defer io.Copy(io.Discard, resp.Body)

	if resp.StatusCode/100 == 2 {
		return nil
	}

	// Best-effort read of a small response body to include in the error message.
	bodySnippet := make([]byte, 256)
	n, _ := io.ReadFull(resp.Body, bodySnippet)
	return &httpExportError{
		status:     resp.StatusCode,
		retryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()),
		msg:        string(bodySnippet[:n]),
	}
}
