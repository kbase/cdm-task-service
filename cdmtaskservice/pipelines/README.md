# Pipelines

Experimental support for running pre-written, possibly multi-WDL pipelines at NERSC via JAWS.

Unlike registered Docker images, pipelines are not registered dynamically at runtime - adding
or updating one requires adding code/config under this package and restarting the service.
See `definition.py` for the contract each pipeline version implements and `registry.py` for
how pipeline versions are looked up.

Each pipeline version module exposes an `init()` function that builds and returns an
instance of `PipelineDefinition`.
