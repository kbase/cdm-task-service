FROM htcondor/mini:24.11.2-el9

# set default docker GID
ARG DOCKER_GID=999

# Create docker group before installing Docker, owise GID won't match host
RUN groupadd -g ${DOCKER_GID} docker

RUN dnf config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo \
    && dnf install -y \
       docker-ce \
       docker-ce-cli \
       containerd.io \
       docker-buildx-plugin \
       docker-compose-plugin \
    && dnf clean all
