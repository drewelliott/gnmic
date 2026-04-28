// © 2026 NVIDIA Corporation
//
// This code is a Contribution to the gNMIc project ("Work") made under the Google Software Grant and Corporate Contributor License Agreement ("CLA") and governed by the Apache License 2.0.
// No other rights or licenses in or to any of NVIDIA's intellectual property are granted for any other purpose.
// This code is provided on an "as is" basis without any warranties of any kind.
//
// SPDX-License-Identifier: Apache-2.0

package otlp_output

import (
	"fmt"
	"net/url"
	"strings"
)

const otlpHTTPMetricsPath = "/v1/metrics"

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
