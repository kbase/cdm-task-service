FROM python:3.13

ENV CRANE_VER=v0.20.2

RUN mkdir -p /cdmtaskservice && mkdir -p /craneinstall

# Install crane

WORKDIR /craneinstall

# crane install docs: https://github.com/google/go-containerregistry/blob/main/cmd/crane/README.md
# Note that the provenance verification step is broken, which wasted an hour or two of time
# https://github.com/google/go-containerregistry/issues/1982
RUN curl -sL https://github.com/google/go-containerregistry/releases/download/$CRANE_VER/go-containerregistry_Linux_x86_64.tar.gz > go-containerregistry.tar.gz \ 
    && tar -zxvf go-containerregistry.tar.gz \
    && mv ./crane /cdmtaskservice \
    && rm -r /craneinstall

ENV CDM_TASK_SERVICE_CRANE_EXE=/cdmtaskservice/crane

# install pipenv
RUN pip install --upgrade pip && \
    pip install pipenv

WORKDIR /cdmtaskservice

# install deps
COPY Pipfile* ./
RUN pipenv sync --system

COPY ./ /cdmtaskservice

# Write the git commit for the service
ARG VCS_REF=no_git_commit_passed_to_build
RUN echo "GIT_COMMIT=\"$VCS_REF\"" > cdmtaskservice/git_commit.py

ENTRYPOINT ["scripts/entrypoint.sh"]

