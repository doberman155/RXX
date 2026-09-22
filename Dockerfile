# Единый Dockerfile для всех сервисов монорепозитория.
# Конкретный сервис выбирается аргументом сборки SERVICE, например:
#   docker build --build-arg SERVICE=order-service -t shop/order-service .

FROM golang:1.26-alpine AS builder

WORKDIR /src

# Слой зависимостей кешируется отдельно от исходников.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG SERVICE
RUN test -n "$SERVICE" || (echo "не задан build-arg SERVICE" && exit 1)

# CGO не нужен: получаем статический бинарник для scratch-подобного окружения.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${SERVICE}

FROM alpine:3.21 AS runtime

# wget из busybox используется healthcheck-ом, ca-certificates — на будущее.
RUN apk add --no-cache ca-certificates tzdata \
    && adduser -D -u 10001 app

COPY --from=builder /out/app /usr/local/bin/app

USER app
WORKDIR /home/app

ENTRYPOINT ["/usr/local/bin/app"]
