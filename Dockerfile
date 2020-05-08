FROM gcr.io/triton-for-games-dev/triton-builder-base:latest

ENV GO_VERSION=1.14.2
ENV GOPATH=/go
ENV SOURCE_DIR=/app
ENV BUILD_DIR=/build
ENV DEBIAN_FRONTEND="noninteractive"

COPY . ${SOURCE_DIR}

WORKDIR ${SOURCE_DIR}
# RUN go get ./...

RUN echo "export PATH=/usr/local/go/bin:/go/bin/:\$PATH" >> /root/.bashrc
RUN echo "export CXX=clang++-10" >> /root/.bashrc
RUN echo "export GOPATH=/go" >> /root/.bashrc
RUN mkdir ${BUILD_DIR}
WORKDIR ${BUILD_DIR}

ENV CXX=clang++-10
RUN cmake -S ${SOURCE_DIR}

