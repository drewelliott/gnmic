// © 2025-2026 NVIDIA Corporation
//
// This code is a Contribution to the gNMIc project ("Work") made under the Google Software Grant and Corporate Contributor License Agreement ("CLA") and governed by the Apache License 2.0.
// No other rights or licenses in or to any of NVIDIA's intellectual property are granted for any other purpose.
// This code is provided on an "as is" basis without any warranties of any kind.
//
// SPDX-License-Identifier: Apache-2.0

package otlp_output

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/openconfig/gnmi/proto/gnmi"
	gutils "github.com/openconfig/gnmic/pkg/utils"
	metricsv1 "go.opentelemetry.io/proto/otlp/collector/metrics/v1"

	"github.com/openconfig/gnmic/pkg/api/types"
	"github.com/openconfig/gnmic/pkg/api/utils"
	"github.com/openconfig/gnmic/pkg/formatters"
	"github.com/openconfig/gnmic/pkg/outputs"
	"github.com/zestor-dev/zestor/store"
)

const (
	outputType             = "otlp"
	defaultTimeout         = 10 * time.Second
	defaultBatchSize       = 1000
	defaultNumWorkers      = 1
	defaultMaxRetries      = 3
	defaultProtocol        = "grpc"
	defaultCompressionHTTP = "gzip" // default-on for protocol: http; gRPC unaffected
	loggingPrefix          = "[otlp_output:%s] "
	// maxRetryAfter caps a server-supplied Retry-After to prevent a single bad
	// response from pinning a worker for arbitrarily long. Operators rebalancing
	// or upgrading Panoptes occasionally emit large values.
	maxRetryAfter = 30 * time.Second
)

func init() {
	outputs.Register(outputType, func() outputs.Output {
		return &otlpOutput{}
	})
}

// otlpOutput implements the Output interface for OTLP metrics export
type otlpOutput struct {
	outputs.BaseOutput

	state *atomic.Pointer[outputState]
	// stateMu serializes the (Load, state.transport.inFlight.Add(1)) pair in
	// sendBatch with Update's (Swap, start Wait) so we never Add to a
	// WaitGroup whose Wait has already returned 0. Held briefly — no
	// contention at gnmic batching rates (1-10 Hz).
	stateMu sync.Mutex
	dynCfg  *atomic.Pointer[dynConfig]
	eventCh *atomic.Pointer[chan *formatters.EventMsg]

	logger   *log.Logger
	rootCtx  context.Context
	cancelFn context.CancelFunc
	wg       *sync.WaitGroup

	// Metrics
	reg *prometheus.Registry
	// store
	store store.Store[any]
}

// transportState owns the protocol-level connection (gRPC conn or HTTP
// Transport) and the inFlight WaitGroup that tracks every batch currently
// using it. It is held by outputState through a pointer so a config-only
// reload can publish a new outputState that shares the same transportState
// — every concurrent batch, regardless of which outputState revision it
// captured, increments the same inFlight. A subsequent rebuild reload
// allocates a new transportState; cleanup of the old transportState
// blocks on its inFlight before closing the underlying conn/Transport,
// so an active batch can never observe a torn-down transport.
//
// transportState contains a sync.WaitGroup, so it must never be copied —
// only passed/stored as *transportState.
type transportState struct {
	grpcState *grpcClientState // nil when protocol != grpc
	httpState *httpClientState // nil when protocol != http

	// inFlight tracks every concurrent user of this transport, across all
	// outputStates that share this *transportState. sendBatch increments
	// under stateMu before unlocking; Done() fires from a defer. Cleanup
	// (in Update's rebuild path and Close) Waits on this before closing
	// conn/Transport.
	inFlight sync.WaitGroup
}

// cleanup releases the transport-level resources held here.
// Safe on a nil receiver and on a transportState whose protocol-specific
// fields are already nil.
func (t *transportState) cleanup() {
	if t == nil {
		return
	}
	if t.grpcState != nil && t.grpcState.conn != nil {
		_ = t.grpcState.conn.Close()
	}
	if t.httpState != nil && t.httpState.client != nil {
		if tr, ok := t.httpState.client.Transport.(*http.Transport); ok {
			tr.CloseIdleConnections()
		}
	}
}

// outputState bundles the consistent (cfg, transport) tuple. Workers Load()
// this once at the top of sendBatch and dispatch against the snapshot — a
// concurrent Update() that swaps protocol cannot tear the pair, eliminating
// the "X client not initialized" race when a worker raced cfg.Load against
// transport-pointer Swap.
//
// Config-only reloads share the same *transportState pointer with the
// previous outputState; only a transport-rebuild reload allocates a new
// transportState. This guarantees that the inFlight WaitGroup which
// cleanup waits on really does account for every batch in flight on
// that transport, regardless of how many config-only reloads were layered
// on top of it.
type outputState struct {
	cfg       *config
	transport *transportState
}

type dynConfig struct {
	evps []formatters.EventProcessor
}

type grpcClientState struct {
	conn   *grpc.ClientConn
	client metricsv1.MetricsServiceClient
}

// config holds the OTLP output configuration
type config struct {
	// name of the output
	Name string `mapstructure:"name,omitempty"`
	// endpoint of the OTLP collector
	Endpoint string `mapstructure:"endpoint,omitempty"`
	// "grpc" or "http"
	// defaults to "grpc"
	Protocol string `mapstructure:"protocol,omitempty"`
	// RPC timeout
	Timeout time.Duration `mapstructure:"timeout,omitempty"`
	// TLS configuration
	TLS *types.TLSConfig `mapstructure:"tls,omitempty"`

	// Batching
	BatchSize  int           `mapstructure:"batch-size,omitempty"`
	Interval   time.Duration `mapstructure:"interval,omitempty"`
	BufferSize int           `mapstructure:"buffer-size,omitempty"`

	// Compression applied to the HTTP request body. Only honored when protocol: http.
	// Valid values: "none", "gzip". Defaults to "gzip" when protocol is "http".
	// The gRPC path ignores this field.
	Compression string `mapstructure:"compression,omitempty"`

	// Retry
	MaxRetries int `mapstructure:"max-retries,omitempty"`

	// Metric naming
	// string, to be used as the metric namespace
	MetricPrefix string `mapstructure:"metric-prefix,omitempty"`
	// boolean, if true the subscription name will be prepended to the metric name after the prefix.
	AppendSubscriptionName bool `mapstructure:"append-subscription-name,omitempty"`
	// boolean, if true, string type values are exported as gauge metrics with value=1
	// and the string stored as an attribute named "value".
	// if false, string values are dropped.
	StringsAsAttributes bool `mapstructure:"strings-as-attributes,omitempty"`
	// boolean, if true, the leading "/" of the metric path is trimmed before the
	// slash-to-underscore conversion, so a path like "/interfaces/..." becomes
	// "interfaces_..." instead of "_interfaces_...". Defaults to false for
	// backward compatibility.
	StripLeadingUnderscore bool `mapstructure:"strip-leading-underscore,omitempty"`

	// Tags whose values are placed as OTLP Resource attributes and excluded
	// from data point attributes.
	// Set to an empty list to put all tags on data points (useful for Prometheus compatibility).
	ResourceTagKeys []string `mapstructure:"resource-tag-keys,omitempty"`

	// Regex patterns matched against the value key to classify a metric as a
	// monotonic cumulative counter (Sum). Unmatched metrics become Gauges.
	// If empty, all metrics are exported as Gauges.
	CounterPatterns []string `mapstructure:"counter-patterns,omitempty"`

	// Resource attributes
	ResourceAttributes map[string]string `mapstructure:"resource-attributes,omitempty"`

	// Headers to include with every export request (gRPC metadata / HTTP headers).
	// Use this to set e.g. "X-Scope-OrgID" for Grafana Mimir, Loki, Tempo, etc.
	Headers map[string]string `mapstructure:"headers,omitempty"`

	// Precomputed lookup set for ResourceTagKeys (not from config file).
	resourceTagSet map[string]bool
	// Compiled regexes from CounterPatterns.
	counterRegexes []*regexp.Regexp

	// Performance
	NumWorkers int `mapstructure:"num-workers,omitempty"`

	// Debugging
	Debug         bool `mapstructure:"debug,omitempty"`
	EnableMetrics bool `mapstructure:"enable-metrics,omitempty"`

	// Event processors
	EventProcessors []string `mapstructure:"event-processors,omitempty"`
}

func (o *otlpOutput) initFields() {
	o.state = new(atomic.Pointer[outputState])
	o.dynCfg = new(atomic.Pointer[dynConfig])
	o.eventCh = new(atomic.Pointer[chan *formatters.EventMsg])
	o.wg = new(sync.WaitGroup)
	o.logger = log.New(io.Discard, loggingPrefix, utils.DefaultLoggingFlags)
}

// loadCfg returns the currently-published config, or nil if state was never
// stored. Centralizing the deref protects against a nil state.Load() in
// places (such as String) that may be called pre-Init in tests.
func (o *otlpOutput) loadCfg() *config {
	if s := o.state.Load(); s != nil {
		return s.cfg
	}
	return nil
}

func (o *otlpOutput) String() string {
	cfg := o.loadCfg()
	b, err := json.Marshal(cfg)
	if err != nil {
		return ""
	}
	return string(b)
}

// buildOutputState constructs an outputState for the given config. The error
// path is exclusively transport-init failures; callers handle protocol-level
// validation upstream via validateConfig.
func (o *otlpOutput) buildOutputState(cfg *config) (*outputState, error) {
	t := &transportState{}
	switch cfg.Protocol {
	case "grpc":
		gs, err := o.initGRPCFor(cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize gRPC transport: %w", err)
		}
		t.grpcState = gs
	case "http":
		hs, err := o.initHTTPFor(cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize HTTP transport: %w", err)
		}
		t.httpState = hs
	default:
		// unreachable: validateConfig rejects this earlier. Kept as a
		// defensive belt-and-suspenders to mirror Init's old switch.
		return nil, fmt.Errorf("unsupported protocol %q: must be 'grpc' or 'http'", cfg.Protocol)
	}
	return &outputState{cfg: cfg, transport: t}, nil
}

// Init initializes the OTLP output
func (o *otlpOutput) Init(ctx context.Context, name string, cfg map[string]interface{}, opts ...outputs.Option) error {
	o.initFields()

	ncfg := new(config)
	err := outputs.DecodeConfig(cfg, ncfg)
	if err != nil {
		return err
	}
	if ncfg.Name == "" {
		ncfg.Name = name
	}
	o.logger.SetPrefix(fmt.Sprintf(loggingPrefix, ncfg.Name))

	options := &outputs.OutputOptions{}
	for _, opt := range opts {
		if err := opt(options); err != nil {
			return err
		}
	}
	o.store = options.Store

	// Set defaults
	if options.Name != "" {
		ncfg.Name = options.Name
	}
	o.setDefaultsFor(ncfg)
	if err := o.validateConfig(ncfg); err != nil {
		return err
	}

	// Apply logger
	if options.Logger != nil {
		o.logger.SetOutput(options.Logger.Writer())
		o.logger.SetFlags(options.Logger.Flags())
	}

	// Initialize registry and register metrics (no transport open yet).
	o.reg = options.Registry
	if err := o.registerMetrics(ncfg); err != nil {
		return err
	}

	// From this point on, any error must unregister the collectors we just
	// registered; otherwise a retry of Init will hit "duplicate registration".
	initSucceeded := false
	defer func() {
		if !initSucceeded {
			o.unregisterMetrics()
		}
	}()

	// Build event processors (no transport open yet).
	dc := new(dynConfig)
	dc.evps, err = o.buildEventProcessors(ncfg)
	if err != nil {
		return err
	}
	o.dynCfg.Store(dc)

	// Build and publish the outputState (cfg + transport). This is the last
	// resource-allocating step that can fail; if buildOutputState succeeds,
	// Init succeeds (worker startup below cannot fail).
	state, err := o.buildOutputState(ncfg)
	if err != nil {
		return err
	}
	o.state.Store(state)

	// Initialize worker channels and start workers.
	eventCh := make(chan *formatters.EventMsg, ncfg.BufferSize)
	o.eventCh.Store(&eventCh)

	// Start workers
	o.rootCtx = ctx
	var wctx context.Context
	wctx, o.cancelFn = context.WithCancel(o.rootCtx)
	o.wg.Add(ncfg.NumWorkers)
	for i := 0; i < ncfg.NumWorkers; i++ {
		go o.worker(wctx, i, o.wg)
	}

	o.logger.Printf("initialized OTLP output: endpoint=%s, protocol=%s, batch-size=%d, workers=%d",
		ncfg.Endpoint, ncfg.Protocol, ncfg.BatchSize, ncfg.NumWorkers)

	initSucceeded = true
	return nil
}

func (o *otlpOutput) Update(ctx context.Context, cfg map[string]any) error {
	newCfg := new(config)
	err := outputs.DecodeConfig(cfg, newCfg)
	if err != nil {
		return err
	}

	o.setDefaultsFor(newCfg)
	if err := o.validateConfig(newCfg); err != nil {
		return err
	}

	currState := o.state.Load()
	var currCfg *config
	if currState != nil {
		currCfg = currState.cfg
	}

	swapChannel := channelNeedsSwap(currCfg, newCfg)
	restartWorkers := needsWorkerRestart(currCfg, newCfg)
	rebuildTransport := needsTransportRebuild(currCfg, newCfg)
	rebuildProcessors := currCfg == nil || slices.Compare(currCfg.EventProcessors, newCfg.EventProcessors) != 0

	dc := new(dynConfig)
	prevDC := o.dynCfg.Load()
	if rebuildProcessors {
		dc.evps, err = o.buildEventProcessors(newCfg)
		if err != nil {
			return err
		}
	} else if prevDC != nil {
		dc.evps = prevDC.evps
	}
	o.dynCfg.Store(dc)

	// Publish the new outputState atomically. Two paths:
	//   1) needsTransportRebuild → build a fresh transport and Swap. Workers
	//      holding the old state reference can finish their batches; we
	//      Wait() on the old state's inFlight WaitGroup before cleanup so
	//      teardown happens after the last in-flight user — regardless of
	//      Timeout × (MaxRetries+1) + maxRetryAfter.
	//   2) Otherwise (e.g. batch-size, resource-tag-keys) → reuse the
	//      transport pointers from the old state. No close, no rebuild.
	if rebuildTransport {
		newState, err := o.buildOutputState(newCfg)
		if err != nil {
			return fmt.Errorf("failed to build new transport state: %w", err)
		}
		o.stateMu.Lock()
		oldState := o.state.Swap(newState)
		o.stateMu.Unlock()
		// Cleanup waits for actual in-flight users to drain — no wall-clock
		// guess. inFlight lives on transportState, so it accounts for every
		// batch using this transport across any config-only reloads that
		// were layered on top of it. After they all complete (Done fires),
		// the WaitGroup unblocks and we close the underlying transport.
		// Run async so Update returns promptly. Swap returns nil if Update
		// races Init (defensive — reachable through some test paths).
		if oldState != nil {
			go func() {
				oldState.transport.inFlight.Wait()
				oldState.transport.cleanup()
			}()
		}
	} else {
		// Cfg-only change. Share the *transportState pointer with the
		// previous state so a single inFlight WaitGroup tracks every batch
		// using this transport across all config-only reloads.
		// nil-currState is unreachable in practice (needsTransportRebuild
		// returns true on nil, taking the rebuild branch above); the guard
		// below is defensive.
		newState := &outputState{cfg: newCfg}
		if currState != nil {
			newState.transport = currState.transport
		}
		o.stateMu.Lock()
		o.state.Store(newState)
		o.stateMu.Unlock()
		// No cleanup — the transport is still live and shared with newState.
	}

	if swapChannel || restartWorkers {
		var newChan chan *formatters.EventMsg
		if swapChannel {
			newChan = make(chan *formatters.EventMsg, newCfg.BufferSize)
		} else {
			newChan = *o.eventCh.Load()
		}

		runCtx, cancel := context.WithCancel(o.rootCtx)
		newWG := new(sync.WaitGroup)

		oldCancel := o.cancelFn
		oldWG := o.wg
		oldEventCh := *o.eventCh.Load()

		o.cancelFn = cancel
		o.wg = newWG
		o.eventCh.Store(&newChan)

		newWG.Add(newCfg.NumWorkers)
		for i := 0; i < newCfg.NumWorkers; i++ {
			go o.worker(runCtx, i, newWG)
		}

		if oldCancel != nil {
			oldCancel()
		}
		if oldWG != nil {
			oldWG.Wait()
		}

		if swapChannel {
		OUTER_LOOP:
			for {
				select {
				case ev, ok := <-oldEventCh:
					if !ok {
						break OUTER_LOOP
					}
					select {
					case newChan <- ev:
					default:
					}
				default:
					break OUTER_LOOP
				}
			}
		}
	}

	o.logger.Printf("updated OTLP output: %s", o.String())
	return nil
}

func (o *otlpOutput) Validate(cfg map[string]any) error {
	ncfg := new(config)
	err := outputs.DecodeConfig(cfg, ncfg)
	if err != nil {
		return err
	}
	o.setDefaultsFor(ncfg)
	return o.validateConfig(ncfg)
}

func (o *otlpOutput) UpdateProcessor(name string, pcfg map[string]any) error {
	cfg := o.loadCfg()
	dc := o.dynCfg.Load()

	newEvps, changed, err := outputs.UpdateProcessorInSlice(
		o.logger,
		o.store,
		cfg.EventProcessors,
		dc.evps,
		name,
		pcfg,
	)
	if err != nil {
		return err
	}
	if changed {
		newDC := *dc
		newDC.evps = newEvps
		o.dynCfg.Store(&newDC)
		o.logger.Printf("updated event processor %s", name)
	}
	return nil
}

// Write handles incoming gNMI messages
func (o *otlpOutput) Write(ctx context.Context, rsp proto.Message, meta outputs.Meta) {
	if rsp == nil {
		return
	}

	cfg := o.loadCfg()
	dc := o.dynCfg.Load()
	if dc == nil {
		return
	}

	// Type assert to gNMI SubscribeResponse
	subsResp, ok := rsp.(*gnmi.SubscribeResponse)
	if !ok {
		if cfg.Debug {
			o.logger.Printf("received non-SubscribeResponse message, ignoring")
		}
		return
	}

	// Convert gNMI response to EventMsg format
	subscriptionName := meta["subscription-name"]
	if subscriptionName == "" {
		subscriptionName = "default"
	}

	events, err := formatters.ResponseToEventMsgs(subscriptionName, subsResp, meta, dc.evps...)
	if err != nil {
		if cfg.Debug {
			o.logger.Printf("failed to convert response to events: %v", err)
		}
		return
	}

	// Send events to worker channel
	eventCh := *o.eventCh.Load()
	for _, event := range events {
		select {
		case eventCh <- event:
		case <-ctx.Done():
			return
		default:
			if cfg.Debug {
				o.logger.Printf("event channel full, dropping event")
			}
			if cfg.EnableMetrics {
				otlpNumberOfFailedEvents.WithLabelValues(cfg.Name, "channel_full").Inc()
			}
		}
	}
}

// WriteEvent handles individual EventMsg
func (o *otlpOutput) WriteEvent(ctx context.Context, ev *formatters.EventMsg) {
	if ev == nil {
		return
	}

	cfg := o.loadCfg()
	eventCh := *o.eventCh.Load()

	select {
	case eventCh <- ev:
	case <-ctx.Done():
		return
	default:
		if cfg.Debug {
			o.logger.Printf("event channel full, dropping event")
		}
		if cfg.EnableMetrics {
			otlpNumberOfFailedEvents.WithLabelValues(cfg.Name, "channel_full").Inc()
		}
	}
}

// Close closes the OTLP output. Safe to call multiple times.
func (o *otlpOutput) Close() error {
	if o.cancelFn != nil {
		o.cancelFn()
		o.cancelFn = nil
	}

	// Close event channel once.
	if ch := o.eventCh.Swap(nil); ch != nil {
		close(*ch)
	}

	// Wait for workers to finish
	o.wg.Wait()

	// Cleanup transport state. By this point all workers have exited
	// (wg.Wait returned), so no goroutine holds an outputState reference —
	// the transport's inFlight has already drained to zero. The lock around
	// the Swap preserves the same invariant Update relies on (no Add can
	// race a Wait), protecting any future code path that holds state
	// without going through sendBatch. cleanup() handles a nil receiver
	// and a transportState whose protocol-specific fields are already nil,
	// so the post-initFields pre-Init shape is also safe.
	o.stateMu.Lock()
	oldState := o.state.Swap(nil)
	o.stateMu.Unlock()
	if oldState != nil {
		oldState.transport.cleanup()
	}

	o.unregisterMetrics()
	return nil
}

// worker processes events in batches.
// wg is passed explicitly (rather than read from o.wg) so each goroutine
// captures its own WaitGroup pointer at spawn time. Update swaps o.wg when
// num-workers/batch-size/interval change; reading o.wg from the worker
// would race that swap and could land Done() on the wrong WaitGroup.
func (o *otlpOutput) worker(ctx context.Context, id int, wg *sync.WaitGroup) {
	defer wg.Done()

	cfg := o.loadCfg()
	if cfg.Debug {
		o.logger.Printf("worker %d started", id)
	}

	batch := make([]*formatters.EventMsg, 0, cfg.BatchSize)
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	chPtr := o.eventCh.Load()
	if chPtr == nil {
		return
	}
	eventCh := *chPtr

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				flushCtx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
				defer cancel()
				o.sendBatch(flushCtx, batch)
			}
			if cfg.Debug {
				o.logger.Printf("worker %d stopped", id)
			}
			return

		case event, ok := <-eventCh:
			if !ok {
				if len(batch) > 0 {
					flushCtx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
					defer cancel()
					o.sendBatch(flushCtx, batch)
				}
				return
			}

			batch = append(batch, event)
			if len(batch) >= cfg.BatchSize {
				o.sendBatch(ctx, batch)
				batch = make([]*formatters.EventMsg, 0, cfg.BatchSize)
			}

		case <-ticker.C:
			if len(batch) > 0 {
				o.sendBatch(ctx, batch)
				batch = make([]*formatters.EventMsg, 0, cfg.BatchSize)
			}
		}
	}
}

func (o *otlpOutput) sendBatch(ctx context.Context, events []*formatters.EventMsg) {
	if len(events) == 0 {
		return
	}

	// Single Load() at the top of the batch — workers dispatch against this
	// snapshot for every retry attempt. A concurrent Update() that swaps
	// protocol cannot tear the (cfg, transport) pair: this batch finishes
	// against the captured state, and the old transport stays alive until
	// the in-flight WaitGroup drains (see Update's rebuild path).
	//
	// The (Load, Add) pair must be serialized against Update's (Swap, Wait)
	// via stateMu so we never Add to a WaitGroup whose Wait has already
	// returned 0 (which would panic per sync.WaitGroup contract).
	o.stateMu.Lock()
	state := o.state.Load()
	state.transport.inFlight.Add(1)
	o.stateMu.Unlock()
	defer state.transport.inFlight.Done()
	cfg := state.cfg
	start := time.Now()

	req := o.convertToOTLP(cfg, events)

	var err error
retry:
	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		switch cfg.Protocol {
		case "grpc":
			err = o.sendGRPC(ctx, state, req)
		case "http":
			err = o.sendHTTP(ctx, state, req)
		default:
			// Init's transport switch rejects unsupported protocols at startup,
			// and Task 8 adds the same guard to validateConfig (so reload is also
			// covered). This default is the last line of defense — log and abort
			// immediately rather than spinning MaxRetries on an error that
			// cannot resolve itself.
			o.logger.Printf("unsupported protocol %q, aborting batch", cfg.Protocol)
			err = fmt.Errorf("unsupported protocol %q", cfg.Protocol)
			break retry
		}

		if err == nil {
			if cfg.Debug {
				o.logger.Printf("successfully sent %d events (attempt %d)", len(events), attempt+1)
			}
			if cfg.EnableMetrics {
				otlpNumberOfSentEvents.WithLabelValues(cfg.Name).Add(float64(len(events)))
				otlpSendDuration.WithLabelValues(cfg.Name).Observe(time.Since(start).Seconds())
			}
			return
		}

		// Permanent errors abort the loop. isPermanentHTTPError covers HTTP status
		// classification; isPartialSuccessError covers PartialSuccess on either
		// transport (gRPC and HTTP both can return *partialSuccessError).
		if isPermanentHTTPError(err) || isPartialSuccessError(err) {
			o.logger.Printf("permanent error, not retrying: %v", err)
			break
		}

		if attempt < cfg.MaxRetries {
			sleep := effectiveRetrySleep(cfg.Protocol, attempt, retryAfterFromError(err), maxRetryAfter)
			// Use a select on ctx.Done so Close()/cancellation can interrupt a long
			// Retry-After or backoff sleep instead of pinning the worker.
			select {
			case <-time.After(sleep):
			case <-ctx.Done():
				return
			}
		}
	}

	o.logger.Printf("failed to send batch after %d retries: %v", cfg.MaxRetries, err)
	if cfg.EnableMetrics {
		otlpNumberOfFailedEvents.WithLabelValues(cfg.Name, "send_failed").Add(float64(len(events)))
	}
}

func (o *otlpOutput) setDefaultsFor(c *config) {
	if c.Timeout == 0 {
		c.Timeout = defaultTimeout
	}
	if c.BatchSize == 0 {
		c.BatchSize = defaultBatchSize
	}
	if c.NumWorkers == 0 {
		c.NumWorkers = defaultNumWorkers
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = defaultMaxRetries
	}
	if c.Protocol == "" {
		c.Protocol = defaultProtocol
	}
	if c.Protocol == "http" && c.Compression == "" {
		c.Compression = defaultCompressionHTTP
	}
	if c.Name == "" {
		c.Name = "gnmic-otlp-" + uuid.New().String()
	}
	if c.Interval == 0 {
		c.Interval = 5 * time.Second
	}
	if c.BufferSize == 0 {
		c.BufferSize = c.BatchSize * 2
	}
	c.resourceTagSet = make(map[string]bool, len(c.ResourceTagKeys))
	for _, k := range c.ResourceTagKeys {
		c.resourceTagSet[k] = true
	}
}

func (o *otlpOutput) validateConfig(c *config) error {
	switch c.Protocol {
	case "grpc", "http":
		// ok — setDefaultsFor populates "" → "grpc" before we get here
	default:
		return fmt.Errorf("unsupported protocol %q: must be 'grpc' or 'http'", c.Protocol)
	}
	switch c.Compression {
	case "", "none", "gzip":
		// ok ("" remains valid for grpc protocol where the field is ignored)
	default:
		return fmt.Errorf("invalid compression %q: must be one of 'none' or 'gzip'", c.Compression)
	}
	if c.Endpoint == "" {
		return fmt.Errorf("endpoint is required")
	}
	c.counterRegexes = make([]*regexp.Regexp, 0, len(c.CounterPatterns))
	for _, p := range c.CounterPatterns {
		re, err := regexp.Compile(p)
		if err != nil {
			return fmt.Errorf("invalid counter-pattern %q: %w", p, err)
		}
		c.counterRegexes = append(c.counterRegexes, re)
	}
	return nil
}

func (o *otlpOutput) buildEventProcessors(cfg *config) ([]formatters.EventProcessor, error) {
	tcs, ps, acts, err := gutils.GetConfigMaps(o.store)
	if err != nil {
		return nil, err
	}
	return formatters.MakeEventProcessors(o.logger, cfg.EventProcessors, ps, tcs, acts)
}

func (o *otlpOutput) initGRPCFor(cfg *config) (*grpcClientState, error) {
	var opts []grpc.DialOption

	if cfg.TLS != nil {
		tlsConfig, err := o.createTLSConfigFor(cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create TLS config: %w", err)
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.NewClient(cfg.Endpoint, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP client: %w", err)
	}

	o.logger.Printf("initialized OTLP gRPC client for endpoint: %s", cfg.Endpoint)
	return &grpcClientState{
		conn:   conn,
		client: metricsv1.NewMetricsServiceClient(conn),
	}, nil
}

func (o *otlpOutput) createTLSConfigFor(cfg *config) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: cfg.TLS.SkipVerify,
	}

	if cfg.TLS.CaFile != "" || cfg.TLS.CertFile != "" {
		return utils.NewTLSConfig(
			cfg.TLS.CaFile,
			cfg.TLS.CertFile,
			cfg.TLS.KeyFile,
			"",
			cfg.TLS.SkipVerify,
			false,
		)
	}

	return tlsConfig, nil
}

// unregisterMetrics removes any of this output's collectors that are registered
// with o.reg. Safe on partial registration and safe to call when EnableMetrics
// is false or o.reg is nil. Used both:
//   - inside registerMetrics to roll back a partial registration on failure
//   - in Init's deferred cleanup path when a later step fails after registerMetrics succeeded
//   - in Close to free the collectors so a future Init on the same registry works
func (o *otlpOutput) unregisterMetrics() {
	if o.reg == nil {
		return
	}
	o.reg.Unregister(otlpNumberOfSentEvents)
	o.reg.Unregister(otlpNumberOfFailedEvents)
	o.reg.Unregister(otlpSendDuration)
	o.reg.Unregister(otlpRejectedDataPoints)
}

func (o *otlpOutput) registerMetrics(cfg *config) error {
	if !cfg.EnableMetrics {
		return nil
	}

	if o.reg == nil {
		return nil
	}

	success := false
	defer func() {
		if !success {
			o.unregisterMetrics()
		}
	}()

	if err := o.reg.Register(otlpNumberOfSentEvents); err != nil {
		return err
	}
	if err := o.reg.Register(otlpNumberOfFailedEvents); err != nil {
		return err
	}
	if err := o.reg.Register(otlpSendDuration); err != nil {
		return err
	}
	if err := o.reg.Register(otlpRejectedDataPoints); err != nil {
		return err
	}

	success = true
	return nil
}

// Helper functions for detecting config changes

func channelNeedsSwap(old, nw *config) bool {
	if old == nil || nw == nil {
		return true
	}
	return old.BufferSize != nw.BufferSize
}

func needsWorkerRestart(old, nw *config) bool {
	if old == nil || nw == nil {
		return true
	}
	return old.NumWorkers != nw.NumWorkers ||
		old.BatchSize != nw.BatchSize ||
		old.Interval != nw.Interval
}

func needsTransportRebuild(old, nw *config) bool {
	if old == nil || nw == nil {
		return true
	}
	return old.Endpoint != nw.Endpoint ||
		old.Protocol != nw.Protocol ||
		old.Compression != nw.Compression ||
		!old.TLS.Equal(nw.TLS)
}
