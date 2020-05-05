#!/bin/sh

set -e

mkdir -p build
cd build
cmake ../
cmake --build .

cd ..

go build -o build/server cmd/server/main.go
