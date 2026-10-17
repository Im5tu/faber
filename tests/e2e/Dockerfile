# Node image for tests/e2e/run.sh: Ubuntu 24.04 with systemd as PID 1.
# Run it --privileged --cgroupns=host with tmpfs on /run and /run/lock.
#
#   CONTAINERD_IO=1  also installs Docker's containerd.io from
#                    download.docker.com, to test install.sh on a host that
#                    already runs Docker's containerd.
FROM ubuntu:24.04

ARG CONTAINERD_IO=0
ENV container=docker

# iputils-ping is only for the test's ping over the mesh.
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      systemd systemd-sysv dbus iproute2 curl ca-certificates iputils-ping \
 && rm -rf /var/lib/apt/lists/*

RUN if [ "$CONTAINERD_IO" = 1 ]; then \
      install -m 0755 -d /etc/apt/keyrings \
   && curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc \
   && chmod a+r /etc/apt/keyrings/docker.asc \
   && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu noble stable" \
        > /etc/apt/sources.list.d/docker.list \
   && apt-get update \
   && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends containerd.io \
   && rm -rf /var/lib/apt/lists/*; \
    fi

STOPSIGNAL SIGRTMIN+3
VOLUME ["/var/lib/containerd"]
CMD ["/sbin/init"]
