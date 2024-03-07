#!/usr/bin/env bash

set -euo pipefail

docker-compose up -d
sleep 3
godotenv -f test-local.env go test -race ./...
godotenv -f test-local-alt.env go test -race ./...
docker-compose down
