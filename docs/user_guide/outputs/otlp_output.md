`gnmic` can export subscription updates as [OpenTelemetry](https://opentelemetry.io/) metrics using the [OpenTelemetry Protocol (OTLP)](https://opentelemetry.io/docs/specs/otlp/).

The OTLP output supports both OTLP/gRPC and OTLP/HTTP. OTLP/gRPC commonly uses port `4317`; OTLP/HTTP commonly uses the HTTP/protobuf metrics endpoint on port `4318`.

## Configuration

An OTLP output can be defined under the `outputs` section of the `gnmic` configuration file:

```yaml
outputs:
  output1:
    # required
    type: otlp
    # required, address of the OTLP endpoint
    endpoint: localhost:4317
    # string, transport protocol. One of "grpc" or "http".
    # defaults to "grpc".
    protocol: grpc
    # duration, defaults to 10s.
    # request timeout for each export attempt.
    timeout: 10s
    # tls config.
    tls:
      # string, path to the CA certificate file.
      # used to verify the server certificate when `skip-verify` is false.
      ca-file:
      # string, client certificate file.
      cert-file:
      # string, client key file.
      key-file:
      # boolean, if true, the client will not verify the server certificate.
      skip-verify: false
    # integer, defaults to 1000.
    # number of events to buffer before sending a batch to the OTLP endpoint.
    batch-size: 1000
    # duration, defaults to 5s.
    # events are sent every `interval` or when the batch is full, whichever comes first.
    interval: 5s
    # integer, defaults to 2x batch-size.
    # size of the internal event buffer.
    # buffer-size:
    # integer, defaults to 3.
    # number of retries per export request on failure.
    max-retries: 3
    # string, request body compression. Only honored when protocol is "http";
    # the gRPC path ignores this field.
    # One of "none" or "gzip". Defaults to "gzip" when protocol is "http".
    compression: gzip
    # string, metric namespace prefix.
    metric-prefix: ""
    # boolean, if true the subscription name is prepended to the metric name
    # after the prefix.
    append-subscription-name: false
    # boolean, if true, trims the leading "/" from gNMI paths before converting
    # "/" to "_", avoiding a leading "_" in path-derived metric names.
    # defaults to false.
    strip-leading-underscore: false
    # boolean, if true, string values are exported as gauge metrics with value=1
    # and the string stored as a data point attribute named "value".
    # if false, string values are dropped.
    strings-as-attributes: false
    # list of tag keys to place as OTLP Resource attributes.
    # these tags are excluded from data point attributes.
    # defaults to empty (all tags become data point attributes).
    resource-tag-keys:
      # - device
      # - vendor
      # - model
      # - site
      # - source
    # list of regex patterns matched against the value key (metric path).
    # if any pattern matches, the metric is exported as a monotonic cumulative Sum.
    # unmatched metrics are exported as Gauges.
    counter-patterns:
      # - "counter"
      # - "octets|packets|bytes"
      # - "errors|discards|drops"
    # map of string:string, additional static attributes to add to the OTLP Resource.
    resource-attributes:
      # service.name: gnmic
    # map of string:string, HTTP headers or gRPC metadata to include with every export request.
    headers:
      # X-Scope-OrgID: my-tenant
    # integer, defaults to 1.
    # number of workers processing events.
    num-workers: 1
    # boolean, defaults to false.
    # enables debug logging.
    debug: false
    # boolean, defaults to false.
    # enables the collection and export (via prometheus) of output specific metrics.
    enable-metrics: false
    # list of processors to apply on the message before writing.
    event-processors:
```

## Transport

The `protocol` field selects the OTLP transport:

| Protocol | Endpoint form | Notes |
|---|---|---|
| `grpc` | bare `host:port`, or a gRPC target URI such as `dns:///collector:4317` | default transport |
| `http` | bare `host:port`, or a full `http://` or `https://` URL | sends protobuf-encoded OTLP metrics to `/v1/metrics` by default |

Bare endpoints must include an explicit port. For example, use `collector:4317` for OTLP/gRPC or `collector:4318` for OTLP/HTTP.

For OTLP/HTTP, a bare `host:port` endpoint is converted to a full metrics URL. If a `tls` block is configured the scheme is `https`; otherwise the scheme is `http`. The path defaults to `/v1/metrics`.

```yaml
outputs:
  otlp-http:
    type: otlp
    protocol: http
    endpoint: collector:4318
```

With the configuration above, `gnmic` posts OTLP metrics to:

```text
http://collector:4318/v1/metrics
```

When a full OTLP/HTTP URL is configured, `gnmic` preserves the scheme and path. If the path is empty or `/`, it defaults to `/v1/metrics`.

```yaml
outputs:
  otlp-http:
    type: otlp
    protocol: http
    endpoint: https://collector.example.com/otlp/v1/metrics
```

Only `http` and `https` URL schemes are accepted for OTLP/HTTP. URLs with user information, such as `https://user:pass@example.com`, are rejected; configure authentication data with `headers` instead.

### TLS

The `tls` block applies to both OTLP/gRPC and OTLP/HTTP.

For OTLP/HTTP, `gnmic` rejects `http://` URLs when a `tls` block is configured. Use `https://` or a bare `host:port` endpoint when TLS is required.

To use mutual TLS, configure `ca-file`, `cert-file`, and `key-file`:

```yaml
outputs:
  otlp-http-mtls:
    type: otlp
    protocol: http
    endpoint: collector.example.com:4318
    tls:
      ca-file: /etc/gnmic/certs/ca.crt
      cert-file: /etc/gnmic/certs/client.crt
      key-file: /etc/gnmic/certs/client.key
```

### Compression

When `protocol: http` is used, request body compression defaults to `gzip`.

Set `compression: none` to disable HTTP request compression:

```yaml
outputs:
  otlp-http:
    type: otlp
    protocol: http
    endpoint: collector:4318
    compression: none
```

The `compression` field is ignored by the OTLP/gRPC transport.

### Retries

The `max-retries` field controls how many times a batch is retried after the initial export attempt.

For OTLP/HTTP, `gnmic` follows the OTLP retryable HTTP status code list: `429`, `502`, `503`, and `504`. Other HTTP status codes are treated as permanent failures. When a retryable response includes a `Retry-After` header, `gnmic` honors it up to an internal cap of 30 seconds; otherwise it uses exponential backoff with jitter.

OTLP `PartialSuccess` responses with rejected data points are not retried for either OTLP/gRPC or OTLP/HTTP. When output metrics are enabled, rejected data points are counted by `gnmic_otlp_output_rejected_data_points_total`.

## Metric Naming

The metric name is built from up to three parts joined by underscores:

1. The value of `metric-prefix`, if configured.
2. The subscription name, if `append-subscription-name` is `true`.
3. The gNMI path (value key), with `/` and `-` replaced by `_`.

For example, a gNMI update from subscription `port-stats` with path:

```text
/interfaces/interface[name=1/1/1]/state/counters/in-octets
```

with `metric-prefix: gnmic` and `append-subscription-name: true`, produces a metric named:

```text
gnmic_port_stats_interfaces_interface_state_counters_in_octets
```

If `strip-leading-underscore` is `true`, a leading `/` is removed from the path before `/` is converted to `_`.

## Metric Type Detection

Metrics are classified based on the `counter-patterns` configuration:

- **Sum (monotonic counter)**: if the value key matches any regex in `counter-patterns`.
- **Gauge**: all other numeric values.

By default `counter-patterns` is empty, so all metrics are exported as Gauges. To classify counter-like metrics, configure the patterns explicitly:

```yaml
counter-patterns:
  - "counter|octets|packets|bytes"
  - "errors|discards|drops"
```

Each pattern is a Go [regexp](https://pkg.go.dev/regexp/syntax) matched against the value key, before metric name transformation.

## Resource and Data Point Attributes

Event tags are split between the OTLP Resource and data point attributes based on the `resource-tag-keys` configuration:

- Tags whose keys appear in `resource-tag-keys` are placed as Resource attributes and excluded from data point attributes.
- All remaining tags become data point attributes.

By default `resource-tag-keys` is empty, so all tags become data point attributes. To move device-level metadata to the OTLP Resource, configure it explicitly:

```yaml
resource-tag-keys:
  - device
  - vendor
  - model
  - site
  - source
```

Additional static attributes can be added to every Resource using `resource-attributes`:

```yaml
resource-attributes:
  service.name: gnmic
  deployment.environment: production
```

Events are grouped by their `source` tag. Each unique source becomes a separate OTLP `ResourceMetrics` entry with its own Resource attributes.

## Headers

The `headers` field attaches key/value pairs to every export request. They are sent as gRPC metadata when using `protocol: grpc`, and as HTTP headers when using `protocol: http`.

This is commonly used by multi-tenant OTLP backends that require an organization or tenant identifier:

```yaml
outputs:
  otlp-mimir:
    type: otlp
    protocol: http
    endpoint: mimir.example.com:4318
    headers:
      X-Scope-OrgID: my-tenant-id
```

## OTLP Output Metrics

When `enable-metrics` is set to `true`, the OTLP output exposes the following Prometheus metrics:

| Metric Name | Type | Description |
|---|---|---|
| `gnmic_otlp_output_number_of_sent_events_total` | Counter | Number of events successfully sent to the OTLP endpoint |
| `gnmic_otlp_output_number_of_failed_events_total` | Counter | Number of events that failed to send |
| `gnmic_otlp_output_send_duration_seconds` | Histogram | Duration of sending batches to the OTLP endpoint |
| `gnmic_otlp_output_rejected_data_points_total` | Counter | Number of data points rejected by the OTLP endpoint in `PartialSuccess` responses |

## Examples

### OTLP/gRPC

```yaml
outputs:
  otlp-grpc:
    type: otlp
    protocol: grpc
    endpoint: collector.example.com:4317
    timeout: 10s
    batch-size: 1000
    interval: 5s
```

### OTLP/HTTP with mTLS

```yaml
outputs:
  otlp-http:
    type: otlp
    protocol: http
    endpoint: collector.example.com:4318
    timeout: 10s
    tls:
      ca-file: /etc/gnmic/certs/ca.crt
      cert-file: /etc/gnmic/certs/client.crt
      key-file: /etc/gnmic/certs/client.key
    compression: gzip
    batch-size: 1000
    interval: 5s
    headers:
      X-Scope-OrgID: my-tenant-id
```

### OTLP/HTTP with a Kubernetes service

```yaml
outputs:
  otlp-http:
    type: otlp
    protocol: http
    endpoint: collector.observability.svc.cluster.local:4318
    tls:
      ca-file: /var/run/secrets/otlp/ca.crt
      cert-file: /var/run/secrets/gnmic/tls.crt
      key-file: /var/run/secrets/gnmic/tls.key
```
