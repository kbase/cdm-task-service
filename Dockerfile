FROM python:3.12 AS build

ENV CRANE_VER=v0.20.2

# Install crane

WORKDIR /craneinstall

# crane install docs: https://github.com/google/go-containerregistry/blob/main/cmd/crane/README.md
# Note that the provenance verification step is broken, which wasted an hour or two of time
# https://github.com/google/go-containerregistry/issues/1982
RUN curl -sL https://github.com/google/go-containerregistry/releases/download/$CRANE_VER/go-containerregistry_Linux_x86_64.tar.gz > go-containerregistry.tar.gz \ 
    && tar -zxvf go-containerregistry.tar.gz

# Write the git commit for the service

WORKDIR /git
COPY .git /git
RUN GITCOMMIT=$(git rev-parse HEAD) && echo "GIT_COMMIT=\"$GITCOMMIT\"" > /git/git_commit.py

FROM python:3.12

RUN apt update \
    && apt install -y tini \
    && rm -rf /var/lib/apt/lists/* 

# install uv
RUN pip install --upgrade pip && \
    pip install uv	

# install deps
ARG UV_DEV_ARGUMENT=--no-dev
RUN mkdir /uvinstall
WORKDIR /uvinstall
COPY pyproject.toml uv.lock .python-version .
ENV UV_PROJECT_ENVIRONMENT=/usr/local/
RUN uv sync --locked --inexact $UV_DEV_ARGUMENT

# install the actual code
RUN mkdir /cts
COPY cdmtaskservice /cts/cdmtaskservice
COPY scripts/* /cts
COPY cdmtaskservice_config.toml.jinja /cts

COPY --from=build /craneinstall/crane /cts
COPY --from=build /git/git_commit.py /cts/cdmtaskservice/
ENV KBCTS_CRANE_PATH=/cts/crane

WORKDIR /cts

# build the job runner archive

RUN tar -czf cts.tgz --exclude="*/__pycache__*" cdmtaskservice -C /uvinstall .
ENV KBCTS_JOB_RUNNER_ARCHIVE_PATH=/cts/cts.tgz
ENV KBCTS_HTC_EXE_PATH=/cts/cdmtaskservice/condor/run_job.sh

ENTRYPOINT ["tini", "--", "/cts/entrypoint.sh"]

