# Stage 1: Build the Go application
FROM golang:1.20-alpine AS builder

# Set the Current Working Directory inside the container
WORKDIR /email-scheduler

# Copy the go.mod and go.sum files to the container
COPY go.mod go.sum ./

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN go mod download

# Copy the source code into the container
COPY . .

# Build the Go app
RUN go build -o scheduler

# Stage 2: Run the application
FROM alpine:latest

# Set the Current Working Directory inside the container
WORKDIR /email-scheduler

# Copy the Pre-built binary file from the previous stage
COPY --from=builder /email_scheduler/scheduler .

# Expose port 8080 to the outside world
EXPOSE 8080

# Command to run the executable
CMD ["./scheduler"]
