FROM fedora-minimal:latest

ADD build/event-sink /

ENTRYPOINT ["/event-sink"]
