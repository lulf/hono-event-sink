all: build 

container_build: build
	podman build -t event-sink:latest .

build: builddir
	#CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o build/event-sink cmd/event-sink/main.go
	GOOS=linux GOARCH=amd64 go build -o build/event-sink cmd/event-sink/main.go

builddir:
	mkdir -p build

clean:
	rm -rf build
