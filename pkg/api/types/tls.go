package types

import (
	"fmt"
	"time"
)

type TLSConfig struct {
	CaFile     string `mapstructure:"ca-file,omitempty"`
	KeyFile    string `mapstructure:"key-file,omitempty"`
	CertFile   string `mapstructure:"cert-file,omitempty"`
	SkipVerify bool   `mapstructure:"skip-verify,omitempty"`
	ClientAuth string `mapstructure:"client-auth,omitempty"`
	// ReloadInterval controls proactive client certificate reload checks for
	// components that opt in to certificate hot reload. A zero value disables
	// periodic checks. Opt-in components compare this field separately so it
	// does not change reload behavior for unrelated TLS consumers.
	ReloadInterval time.Duration `mapstructure:"reload-interval,omitempty"`
}

func (t *TLSConfig) Validate() error {
	if t == nil {
		return nil
	}
	switch t.ClientAuth {
	case "", "request":
	case "require", "verify-if-given", "require-verify":
		if t.CaFile == "" {
			return fmt.Errorf("ca-file is required when `client-auth` is %q", t.ClientAuth)
		}
	default:
		return fmt.Errorf("unknown `client-auth` mode: %s", t.ClientAuth)
	}
	return nil
}

func (t *TLSConfig) Equal(other *TLSConfig) bool {
	if t == nil && other == nil {
		return true
	}
	if t == nil || other == nil {
		return false
	}
	return t.CaFile == other.CaFile &&
		t.CertFile == other.CertFile &&
		t.KeyFile == other.KeyFile &&
		t.SkipVerify == other.SkipVerify &&
		t.ClientAuth == other.ClientAuth
}
