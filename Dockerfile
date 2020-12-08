# Use the official Golang image to create a build artifact.
# This is based on Debian and sets the GOPATH to /go.
# https://hub.docker.com/_/golang
FROM golang:1.15 AS builder

WORKDIR /src

# Copy local code to the container image.
COPY . ./open-saves

WORKDIR /src/open-saves

ENV GO111MODULE=on \
  CGO_ENABLED=0 \
  GOOS=linux \
  GOARCH=amd64

# Build the binary.
RUN go build -o build/server cmd/server/main.go 

# Use the official Alpine image for a lean production container.
# https://hub.docker.com/_/alpine
# https://docs.docker.com/develop/develop-images/multistage-build/#use-multi-stage-builds
FROM alpine:3
RUN apk add --no-cache ca-certificates

# Copy the binary to the production image from the builder stage.
COPY --from=builder /src/open-saves/build/server /server

# Run the web service on container startup.
CMD ["/server"]
