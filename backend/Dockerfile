FROM golang:1.21-alpine AS build
WORKDIR /app
COPY backend/go.mod backend/go.sum ./backend/
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY backend ./backend
WORKDIR /app/backend
RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 go build -o /app/backend/server

FROM alpine:3.19
WORKDIR /app
COPY --from=build /app/backend/server ./server
EXPOSE 9999
ENTRYPOINT ["/app/server"]
