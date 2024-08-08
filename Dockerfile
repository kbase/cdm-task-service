FROM python:3.11

RUN mkdir -p /cdmtaskservice
WORKDIR /cdmtaskservice

# install pipenv
RUN pip install --upgrade pip && \
    pip install pipenv

# install deps
COPY Pipfile* ./
RUN pipenv sync --system

COPY ./ /cdmtaskservice

# Write the git commit for the service
ARG VCS_REF=no_git_commit_passed_to_build
RUN echo "GIT_COMMIT=\"$VCS_REF\"" > cdmtaskservice/git_commit.py

ENTRYPOINT ["scripts/entrypoint.sh"]

