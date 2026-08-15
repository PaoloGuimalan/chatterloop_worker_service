## Worker Service for Chatterloop

## Getting Started

Installation

- Download go installation and run

Project Creation

- run go mod init worker_service (for empty project)

Build (Debug)

- run go build main.go
- or go build ./cmd/worker/main.go

Docker Build & Run

- docker build -t worker-service .
- docker run --rm -p 8880:8880 --env-file .env worker-service
