FROM python:3.11 as build

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

FROM python:3.11

# install pipenv
RUN pip install --upgrade pip && \
    pip install pipenv

WORKDIR /pip

# install deps
COPY Pipfile* ./
RUN pipenv sync --system

RUN mkdir /cts
COPY cdmtaskservice /cts/cdmtaskservice
COPY scripts/* /cts
COPY cdmtaskservice_config.toml.jinja /cts

COPY --from=build /craneinstall/crane /cts
COPY --from=build /git/git_commit.py /cts/cdmtaskservice/
ENV KBCTS_CRANE_PATH=/cts/crane

WORKDIR /cts

ENTRYPOINT ["/cts/entrypoint.sh"]

