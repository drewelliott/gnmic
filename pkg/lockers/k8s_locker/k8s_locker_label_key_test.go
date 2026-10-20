// © 2026 NVIDIA Corporation
//
// This code is a Contribution to the gNMIc project ("Work") made under the Google Software Grant and Corporate Contributor License Agreement ("CLA") and governed by the Apache License 2.0.
// No other rights or licenses in or to any of NVIDIA's intellectual property are granted for any other purpose.
// This code is provided on an "as is" basis without any warranties of any kind.
//
// SPDX-License-Identifier: Apache-2.0

package k8s_locker

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes/fake"
)

func newTestLocker() *k8sLocker {
	return &k8sLocker{
		Cfg: &config{
			Namespace:     "gnmic-test",
			LeaseDuration: 10 * time.Second,
			RenewPeriod:   5 * time.Second,
			RetryTimer:    100 * time.Millisecond,
		},
		clientset:       fake.NewSimpleClientset(),
		logger:          slog.New(slog.DiscardHandler),
		m:               new(sync.RWMutex),
		acquiredlocks:   make(map[string]*lock),
		attemptinglocks: make(map[string]*lock),
		identity:        "test-instance",
	}
}

func TestListFindsLocksWhoseLabelKeyWasReplaced(t *testing.T) {
	// The leader reads target ownership through List, so a target locked
	// under a replaced label key must still be listed under its own key.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	k := newTestLocker()
	keys := map[string]string{
		"gnmic/gnmic-juniper-jhb01-jhb01-001-prod/targets/bbr1-cp1-tan1-jhb01": "gnmic-juniper-jhb01-jhb01-001-prod-0",
		"gnmic/gnmic-juniper-jhb01-jhb01-001-prod/targets/bbr2-cp1-tan1-jhb01": "gnmic-juniper-jhb01-jhb01-001-prod-1",
		"gnmic/gnmic-juniper-jhb01-jhb01-001-prod/targets/r1":                  "gnmic-juniper-jhb01-jhb01-001-prod-0",
		"gnmic/other-cluster/targets/bbr1-cp1-tan1-jhb01":                      "other-0",
	}
	for key, holder := range keys {
		ok, err := k.Lock(ctx, key, []byte(holder))
		if err != nil || !ok {
			t.Fatalf("Lock(%q) = %v, %v; want true, nil", key, ok, err)
		}
	}
	got, err := k.List(ctx, "gnmic/gnmic-juniper-jhb01-jhb01-001-prod/targets")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	want := map[string]string{}
	for key, holder := range keys {
		if strings.HasPrefix(key, "gnmic/gnmic-juniper-jhb01-jhb01-001-prod/targets") {
			want[key] = holder
		}
	}
	if len(got) != len(want) {
		t.Fatalf("List returned %d locks %v, want %d %v", len(got), got, len(want), want)
	}
	for key, holder := range want {
		if got[key] != holder {
			t.Errorf("List()[%q] = %q, want %q", key, got[key], holder)
		}
	}
}

func TestLabelKeyForKeepsValidKeys(t *testing.T) {
	// Every key that is a valid label key today must label its Lease exactly
	// as before, so instances on either side of an upgrade agree.
	for _, nkey := range []string{
		"gnmic-gnmic-juniper-pdx04-pdx04-001-prod-targets-pdx04-bbr-01",
		"gnmic-gnmic-juniper-pdx04-pdx04-001-prod-leader",
		"gnmic-default-cluster-targets-router1",
	} {
		if got := labelKeyFor(nkey); got != nkey {
			t.Errorf("labelKeyFor(%q) = %q, want it unchanged", nkey, got)
		}
	}
}

func TestLabelKeyForReplacesInvalidKeys(t *testing.T) {
	long := "gnmic-gnmic-juniper-jhb01-jhb01-001-prod-targets-bbr1-cp1-tan1-jhb01"
	if len(validation.IsQualifiedName(long)) == 0 {
		t.Fatalf("test key %q (%d chars) is unexpectedly a valid label key", long, len(long))
	}
	for _, nkey := range []string{long, "gnmic-cluster-targets-Router_A.", strings.Repeat("a", 300)} {
		got := labelKeyFor(nkey)
		if errs := validation.IsQualifiedName(got); len(errs) != 0 {
			t.Errorf("labelKeyFor(%q) = %q is not a valid label key: %v", nkey, got, errs)
		}
		if got != labelKeyFor(nkey) {
			t.Errorf("labelKeyFor(%q) is not deterministic", nkey)
		}
	}
	other := "gnmic-gnmic-juniper-jhb01-jhb01-001-prod-targets-bbr2-cp1-tan1-jhb01"
	if labelKeyFor(long) == labelKeyFor(other) {
		t.Errorf("distinct keys %q and %q share label key %q", long, other, labelKeyFor(long))
	}
}
