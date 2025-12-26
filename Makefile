-include .env
export
version=
name=

all: build test

build:
	@go build -o main.exe cmd/app/main.go

run:
	@go run cmd/app/main.go

docker-run-infra:
	@docker compose up -d postgres

docker-run-app:
	@docker compose up aloh --build

docker-run-all: test docker-migrate-up docker-run-app

docker-down:
	@docker compose down

docker-build:
	@docker compose build --no-cache

test:
	@go test ./... -v
clean:
	@rm -f main.exe

docker-migrate-up:
	@docker-compose run --rm migrate up

docker-migrate-down:
	@docker-compose run --rm migrate down


create:
	@goose -dir=$(MIGRATIONS_PATH) create $(NAME) sql

down-to:
	@docker-compose run --rm migrate down-to $(VERSION)

status:
	@docker-compose run --rm migrate status

reset:
	@docker-compose run --rm migrate reset