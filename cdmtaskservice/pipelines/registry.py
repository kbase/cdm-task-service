"""
An in-process catalog of available pipeline versions, built once at service startup by
registering a static set of PipelineDefinitions. There is no dynamic registration - adding,
removing, or changing a pipeline version means changing the code/config that registers pipelines
at startup and restarting the service.
"""

import semver

from cdmtaskservice.arg_checkers import not_falsy as _not_falsy
from cdmtaskservice.pipelines.definition import PipelineDefinition


class NoSuchPipelineError(Exception):
    """ No pipeline, or no version of a pipeline, matching the given criteria is registered. """


class PipelineExistsError(Exception):
    """ A pipeline with the same name and version is already registered. """


class PipelineRegistry:
    """ A lookup table of registered pipeline versions. """

    def __init__(self):
        """ Create the registry. """
        self._pipelines: dict[str, dict[semver.Version, PipelineDefinition]] = {}
        self._latest: dict[str, PipelineDefinition] = {}

    def register(self, pipeline: PipelineDefinition):
        """
        Register a pipeline version.

        Raises PipelineExistsError if a pipeline with the same name and version is already
        registered.
        """
        _not_falsy(pipeline, "pipeline")
        name = pipeline.name
        version = pipeline.version
        versions = self._pipelines.setdefault(name, {})
        if version in versions:
            raise PipelineExistsError(
                f"A pipeline named '{name}' with version '{version}' is already registered"
            )
        versions[version] = pipeline
        current_latest = self._latest.get(name)
        if not current_latest or pipeline.version > current_latest.version:
            self._latest[name] = pipeline

    def get(self, name: str, version: semver.Version = None) -> PipelineDefinition:
        """
        Look up a pipeline version by name.

        name - the pipeline name.
        version - the specific version to look up. If omitted, the latest version by semantic
            versioning is returned.

        Raises NoSuchPipelineError if no pipeline with the given name, or no matching version,
        is registered.
        """
        versions = self._pipelines.get(name)
        if not versions:
            raise NoSuchPipelineError(f"No pipeline named '{name}' is registered")
        if version is not None:
            if version not in versions:
                raise NoSuchPipelineError(
                    f"No version '{version}' of pipeline '{name}' is registered"
                )
            return versions[version]
        return self._latest[name]

    def list_versions(self, name: str) -> list[PipelineDefinition]:
        """
        List every registered version of a pipeline, sorted oldest to newest by semantic
        versioning.

        Raises NoSuchPipelineError if no pipeline with the given name is registered.
        """
        versions = self._pipelines.get(name)
        if not versions:
            raise NoSuchPipelineError(f"No pipeline named '{name}' is registered")
        return sorted(versions.values(), key=lambda p: p.version)

    def list_latest(self) -> list[PipelineDefinition]:
        """
        List the latest registered version of every pipeline, by semantic versioning, sorted by
        pipeline name.
        """
        return sorted(self._latest.values(), key=lambda p: p.name)
