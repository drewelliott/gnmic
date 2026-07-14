// © 2026 NVIDIA Corporation
//
// This code is a Contribution to the gNMIc project ("Work") made under the Google Software Grant and Corporate Contributor License Agreement ("CLA") and governed by the Apache License 2.0.
// No other rights or licenses in or to any of NVIDIA's intellectual property are granted for any other purpose.
// This code is provided on an "as is" basis without any warranties of any kind.
//
// SPDX-License-Identifier: Apache-2.0

package otlp_output

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// certificateFileState contains the file attributes used to detect a rotated
// client certificate or key. os.Stat follows symlinks, which is important for
// projected Kubernetes Secrets whose stable paths point through ..data.
type certificateFileState struct {
	modTime time.Time
	size    int64
}

func newCertificateFileState(info os.FileInfo) certificateFileState {
	return certificateFileState{
		modTime: info.ModTime(),
		size:    info.Size(),
	}
}

func (s certificateFileState) equal(other certificateFileState) bool {
	return s.size == other.size && s.modTime.Equal(other.modTime)
}

// clientCertificateReloader owns the OTLP client's last known good certificate.
// The TLS callback and optional interval goroutine share this instance, so a
// successful reload is immediately visible to every subsequent handshake.
type clientCertificateReloader struct {
	mu sync.Mutex

	certFile string
	keyFile  string
	logger   *slog.Logger

	certificate *tls.Certificate
	certState   certificateFileState
	keyState    certificateFileState

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

func newClientCertificateReloader(certFile, keyFile string, reloadInterval time.Duration, logger *slog.Logger) (*clientCertificateReloader, error) {
	r := &clientCertificateReloader{
		certFile: certFile,
		keyFile:  keyFile,
		logger:   logger,
	}

	// Preserve the existing fail-fast behavior: an unreadable or invalid pair
	// must fail Init/Update instead of surfacing on the first export attempt.
	if _, _, err := r.reloadIfChanged(true); err != nil {
		return nil, err
	}

	if reloadInterval > 0 {
		r.start(reloadInterval)
	}
	return r, nil
}

// getClientCertificate is installed as tls.Config.GetClientCertificate. It
// stats both files on every new TLS handshake and only reads/parses them when
// either mtime or size changed. A failed rotation keeps the last known good
// pair active and is retried on the next handshake or interval tick.
func (r *clientCertificateReloader) getClientCertificate(cri *tls.CertificateRequestInfo) (*tls.Certificate, error) {
	certificate, reloaded, err := r.reloadIfChanged(false)
	if err != nil {
		if r.logger != nil {
			r.logger.Warn("failed to reload OTLP client certificate; keeping last known good certificate", "cert-file", r.certFile, "err", err)
		}
		if certificate == nil {
			return nil, err
		}
	} else if reloaded {
		r.logReload(certificate)
	}

	// Match crypto/tls's static Certificates selection behavior: when the
	// server's request cannot use this certificate, send an empty certificate.
	if cri != nil {
		if err := cri.SupportsCertificate(certificate); err != nil {
			return &tls.Certificate{}, nil
		}
	}
	return certificate, nil
}

func (r *clientCertificateReloader) reloadIfChanged(force bool) (*tls.Certificate, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	certInfo, err := os.Stat(r.certFile)
	if err != nil {
		return r.certificate, false, fmt.Errorf("failed to stat client certificate file: %w", err)
	}
	keyInfo, err := os.Stat(r.keyFile)
	if err != nil {
		return r.certificate, false, fmt.Errorf("failed to stat client key file: %w", err)
	}

	certState := newCertificateFileState(certInfo)
	keyState := newCertificateFileState(keyInfo)
	if !force && r.certificate != nil && certState.equal(r.certState) && keyState.equal(r.keyState) {
		return r.certificate, false, nil
	}

	// Capture metadata before reading. If a rotation races these reads, either
	// parsing fails (and the last good pair is retained) or the next stat sees
	// different metadata and immediately performs another reload.
	certPEM, err := os.ReadFile(r.certFile)
	if err != nil {
		return r.certificate, false, fmt.Errorf("failed to read client certificate file: %w", err)
	}
	keyPEM, err := os.ReadFile(r.keyFile)
	if err != nil {
		return r.certificate, false, fmt.Errorf("failed to read client key file: %w", err)
	}
	certificate, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return r.certificate, false, fmt.Errorf("failed to load client certificate key pair: %w", err)
	}

	r.certificate = &certificate
	r.certState = certState
	r.keyState = keyState
	return r.certificate, true, nil
}

func (r *clientCertificateReloader) start(reloadInterval time.Duration) {
	r.stopCh = make(chan struct{})
	r.doneCh = make(chan struct{})

	go func() {
		defer close(r.doneCh)
		ticker := time.NewTicker(reloadInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				certificate, reloaded, err := r.reloadIfChanged(false)
				if err != nil {
					if r.logger != nil {
						r.logger.Warn("failed to reload OTLP client certificate; keeping last known good certificate", "cert-file", r.certFile, "err", err)
					}
					continue
				}
				if reloaded {
					r.logReload(certificate)
				}
			case <-r.stopCh:
				return
			}
		}
	}()
}

func (r *clientCertificateReloader) logReload(certificate *tls.Certificate) {
	if r.logger == nil {
		return
	}
	if certificate != nil && len(certificate.Certificate) > 0 {
		leaf, err := x509.ParseCertificate(certificate.Certificate[0])
		if err == nil {
			r.logger.Info("reloaded OTLP client certificate", "cert-file", r.certFile, "not-after", leaf.NotAfter)
			return
		}
	}
	r.logger.Info("reloaded OTLP client certificate", "cert-file", r.certFile)
}

// close stops the optional proactive reload goroutine. It is safe to call more
// than once and is a no-op when reload-interval is zero.
func (r *clientCertificateReloader) close() {
	if r == nil || r.stopCh == nil {
		return
	}
	r.stopOnce.Do(func() {
		close(r.stopCh)
	})
	<-r.doneCh
}
