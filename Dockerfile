FROM fedora:latest

RUN dnf -y update && dnf -y install qpid-proton-c && dnf -y clean all
ADD build/event-sink /

ENTRYPOINT ["/event-sink"]
