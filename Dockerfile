FROM golang:1.25-alpine AS builder

WORKDIR /usr/local/src

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN go build -o ./bin/app cmd/app/main.go

FROM alpine AS runner

WORKDIR /app

COPY --from=builder /usr/local/src/bin/app .

COPY configs /configs

EXPOSE 1234

CMD ["./app"]