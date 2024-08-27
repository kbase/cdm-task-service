# CDM Task Service

**This is currently a prototype**

Enables running jobs on remote compute from the KBase CDM cluster.

## Nomenclature

* CDM: Central Data Model
  * The KBase data model
* CTS: CDM Task Service

## Service Requirements

* Python 3.11+
* [crane](https://github.com/google/go-containerregistry/blob/main/cmd/crane/README.md)
* An s3 instance for use as a file store, but see "S3 requirements" below

### S3 requirements

* Any objects provided to the servcie that were created with multipart uploads **must** use the
  same part size for all parts except the last.
* The service does not support objects encrypted with customer supplied keys or with the
  AWS key management service.
* The provided credentials must enable listing buckets, as the service performs that operation
  to check the host and credentials on startup
* If using Minio, the minimum version is `2024-08-17T01-24-54Z` and the server must be run
  in `--compat` mode.

## Development

### Adding code

* In this alpha / prototype stage, we will be PRing (do not push directly) to `main`. In the
  future, once we want to deploy beyond CI, we will add a `develop` branch.
* The PR creator merges the PR and deletes branches (after builds / tests / linters complete).

### Code requirements for prototype code

* Any code committed must at least have a test file that imports it and runs a noop test so that
  the code is shown with no coverage in the coverage statistics. This will make it clear what
  code needs tests when we move beyond the prototype stage.
* Each module should have its own test file. Eventually these will be expanded into unit tests
  (or integration tests in the case of app.py)
* Any code committed must have regular code and user documentation so that future devs
  converting the code to production can understand it.
* Release notes are not strictly necessary while deploying to CI, but a concrete version (e.g.
  no `-dev*` or `-prototype*` suffix) will be required outside of that environment. On a case by
  case basis, add release notes and bump the prototype version (e.g. 0.1.0-prototype3 ->
  0.1.0-prototype4) for changes that should be documented.

### Running tests

Copy `test.cfg.example` to `test.cfg` and fill it in appropriately.

```
pipenv sync --dev  # only the first time or when Pipfile.lock changes
pipenv shell
PYTHONPATH=. pytest test
```

### Exit from prototype status

* Coverage badge in Readme
* Run through all code, refactor to production quality
* Add tests where missing (which is a lot) and inspect current tests for completeness and quality
  * E.g. don't assume existing tests are any good
  * Async testing help
    https://tonybaloney.github.io/posts/async-test-patterns-for-pytest-and-unittest.html