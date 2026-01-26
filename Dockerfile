# Build Stage
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git

# Copy gomod
COPY go.mod ./
# COPY go.sum ./ # Uncomment if go.sum exists
RUN go mod download

# Copy source
COPY . .

# Build
RUN CGO_ENABLED=0 GOOS=linux go build -o mole ./cmd/mole

# Final Stage
FROM alpine:latest

WORKDIR /root/

COPY --from=builder /app/mole .

# Expose default port
EXPOSE 7379

# Bind to 0.0.0.0 to allow external access
CMD ["./mole", "-addr", "0.0.0.0:7379"]
