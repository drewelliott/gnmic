# © 2022 Nokia.
#
# This code is a Contribution to the gNMIc project (“Work”) made under the Google Software Grant and Corporate Contributor License Agreement (“CLA”) and governed by the Apache License 2.0.
# No other rights or licenses in or to any of Nokia’s intellectual property are granted for any other purpose.
# This code is provided on an “as is” basis without any warranties of any kind.
#
# SPDX-License-Identifier: Apache-2.0

FROM golang:1.24.12 AS builder

WORKDIR /build

COPY go.mod go.sum /build/
COPY pkg/api/go.mod pkg/api/go.sum /build/pkg/api/
COPY pkg/cache/go.mod pkg/cache/go.sum /build/pkg/cache/
RUN go mod download

ADD . /build

# Build-time metadata baked into the binary so `gnmic --version` identifies
# the exact build. Defaults reflect a from-source build with no metadata
# provided; CI/Docker invocations should pass --build-arg for each.
ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_DATE=unknown
ARG GIT_URL=https://github.com/openconfig/gnmic

RUN CGO_ENABLED=0 go build \
    -ldflags="-s -w \
        -X github.com/openconfig/gnmic/pkg/version.Version=${VERSION} \
        -X github.com/openconfig/gnmic/pkg/version.Commit=${COMMIT} \
        -X github.com/openconfig/gnmic/pkg/version.Date=${BUILD_DATE} \
        -X github.com/openconfig/gnmic/pkg/version.GitURL=${GIT_URL}" \
    -o gnmic .

FROM alpine
LABEL org.opencontainers.image.source=https://github.com/openconfig/gnmic
COPY --from=builder /build/gnmic /app/
WORKDIR /app
ENTRYPOINT [ "/app/gnmic" ]
CMD [ "help" ]
