ARG PYTHON_VERSION=3.12.13-slim
ARG CRANE_VER=v0.21.2

FROM python:${PYTHON_VERSION} AS build

ENV CRANE_VER=${CRANE_VER}

RUN apt-get update && apt-get install -y --no-install-recommends curl git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /craneinstall

# crane install docs: https://github.com/google/go-containerregistry/blob/main/cmd/crane/README.md
RUN curl -sL https://github.com/google/go-containerregistry/releases/download/$CRANE_VER/go-containerregistry_Linux_x86_64.tar.gz > go-containerregistry.tar.gz \
    && curl -sL https://github.com/google/go-containerregistry/releases/download/$CRANE_VER/checksums.txt > checksums.txt \
    && grep "go-containerregistry_Linux_x86_64.tar.gz" checksums.txt | sha256sum -c - \
    && tar -zxvf go-containerregistry.tar.gz

# Write the git commit for the service
WORKDIR /git
COPY .git /git
RUN GITCOMMIT=$(git rev-parse HEAD) && echo "GIT_COMMIT=\"$GITCOMMIT\"" > /git/git_commit.py


FROM python:${PYTHON_VERSION}

RUN apt-get update \
    && apt-get install -y --no-install-recommends tini curl \
    && rm -rf /var/lib/apt/lists/*

# install uv
RUN pip install --upgrade pip && \
    pip install uv

# install deps
RUN mkdir /uvinstall
WORKDIR /uvinstall
COPY pyproject.toml uv.lock .python-version .
ENV UV_PROJECT_ENVIRONMENT=/usr/local/
ARG UV_DEV_ARGUMENT=--no-dev
RUN uv sync --locked --inexact $UV_DEV_ARGUMENT

# install the actual code
RUN mkdir /cts
COPY cdmtaskservice /cts/cdmtaskservice
COPY scripts/* /cts
COPY cdmtaskservice_*config.toml.jinja /cts
COPY --from=build /craneinstall/crane /cts
COPY --from=build /git/git_commit.py /cts/cdmtaskservice/
ENV KBCTS_CRANE_PATH=/cts/crane

WORKDIR /cts

# build the code archive
RUN tar -czf cts.tgz --exclude="*/__pycache__*" cdmtaskservice -C /uvinstall .
ENV KBCTS_CODE_ARCHIVE_PATH=/cts/cts.tgz
ENV KBCTS_HTC_EXE_PATH=/cts/cdmtaskservice/condor/run_job.sh

ENTRYPOINT ["tini", "--", "/cts/entrypoint.sh"]
