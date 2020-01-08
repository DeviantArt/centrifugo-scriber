FROM golang:1.13.4-buster as builder

ARG GOPATH=/go

COPY . /go/src/github.com/DeviantArt/centrifugo-scriber

WORKDIR /go/src/github.com/DeviantArt/centrifugo-scriber

RUN go build -o /centrifugo-scriber

FROM debian:buster-slim

COPY --from=builder /centrifugo-scriber /usr/bin/centrifugo-scriber

EXPOSE 1463

ENTRYPOINT ["/usr/bin/centrifugo-scriber"]