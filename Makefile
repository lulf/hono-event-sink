all: build 

container_build: build
	podman build -t event-sink:latest .

build: builddir
	GOOS=linux GOARCH=amd64 go build -o build/event-sink cmd/event-sink/main.go
	GOOS=linux GOARCH=amd64 go build -o build/event-generator cmd/event-generator/main.go

test:
	go test -v ./...

builddir:
	mkdir -p build

clean:
	rm -rf build
