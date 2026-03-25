FROM golang:1.21-alpine
WORKDIR /app
COPY . .

# 1. Delete any Windows-generated module files
RUN rm -f go.mod go.sum

# 2. Initialize a fresh Linux-native module
RUN go mod init helios/registry

# 3. Fetch the exact dependencies needed by main.go
RUN go mod tidy

# 4. Build the application
RUN go build -o main .

EXPOSE 8081
CMD ["./main"]