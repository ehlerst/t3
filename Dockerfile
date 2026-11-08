FROM gcr.io/distroless/cc-debian12:latest

ARG TARGETARCH

WORKDIR /app
COPY docker-bin/${TARGETARCH}/t3 /usr/local/bin/t3

ENTRYPOINT ["/usr/local/bin/t3"]
CMD ["--help"]
