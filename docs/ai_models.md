# Using AI models in a job

This is an experimental API and is subject to change without notice.

The CTS can deliver pre-staged AI models (currently from HuggingFace) directly into a job's
containers, so that container code doesn't have to download the same, potentially very large,
model weights over and over - once for every job, and once per container within a job. The
model is staged once at each compute site and then reused by every job and container that
requests it. This doc assumes you're already familiar with submitting jobs to the CTS - see
[the examples](./examples.md) for the basics of job submission if not.

## Concepts

* AI models are registered by a service administrator, not by users - see the request to make
  below if the model you need isn't yet registered.
* Each registered AI model is an immutable snapshot identified by a `source` (e.g.
  `huggingface`), a `model` identifier (for `huggingface`, the `org/name` repo ID), and a
  `version` (for `huggingface`, a resolved git commit hash).
* A model may also have one or more mutable *tags* pointing at a version (e.g. `main`,
  `latest`), similar to a Docker image tag. When you reference a model you can use either a tag
  or a literal version.
* You reference a model with a combined `model@ref` string, e.g. `org/name@main` or
  `org/name@abc123def456`. The part after `@` (`ref`) can be a tag or a literal version; the
  CTS tries a literal version match first, then falls back to resolving it as a tag.
* A job can request any number of models, each a separate reference (e.g. one environment
  variable per model if several reasoning tiers are needed). Each one resolves and is delivered
  independently.
* A job cannot use both regular reference data (`refdata`) and the AI model store at the same
  time - they share a single mount point per job.

## Finding available models

Browse the catalog with `GET /ai-models`, or look up one model with
`GET /ai-models/{source}/{model_ref}`, where `model_ref` is the combined `model@ref` string
(URL-encode the `/` and `@` characters as needed). Both routes are public and show each model's
staging status per compute site - a model must be in the `complete` state for the cluster a job
targets before that job can use it.

```
GET /ai-models/huggingface/org%2Fname@main
```

If the model you need isn't listed, or isn't staged at the cluster you want to run on, ask a
service administrator to register it.

## Requesting a model in a job

Declare each wanted model as its own `Parameter` of type `ai_model` in the job's `args` or
`environment`, with `ai_model_source` set to the model's source and `ai_model` set to the
`model@ref` reference. At submission time the CTS resolves the reference to its exact version,
confirms it's staged for the job's target cluster (failing the submission otherwise), and
rewrites the parameter to use the resolved version.

You don't need to specify a mount point for the AI model store - since each model's path is
delivered to the container via its own parameter, the exact mount location doesn't matter. If
you don't set one explicitly (via `params.staged_data_mount_point` or the image's default mount
point), the CTS mounts the store at `/ai_models`.

Each `ai_model` parameter inserts a path of the form

```
<mount point>/<source>/<model>/<version>
```

into the command line or environment variable where it's used - e.g.
`/ai_models/huggingface/org/name/abc123def456`.

### Example: Python `requests`

This example submits a job to the KBase CI environment that runs a container with two models
available: an embedding model passed on the command line, and a classifier model passed via an
environment variable.

```python
import requests

res = requests.post(
    "https://ci.kbase.us/services/cts/jobs/",
    headers={"Authorization": f"Bearer {token_ci}"},
    json={
        "cluster": "perlmutter-jaws",
        "image": "ghcr.io/kbasetest/my_ai_image:0.1.0",
        "params": {
            "input_mount_point": "/in",
            "output_mount_point": "/out",
            "args": [
                "--embedding-model",
                {
                    "type": "ai_model",
                    "ai_model_source": "huggingface",
                    "ai_model": "sentence-transformers/all-MiniLM-L6-v2@main",
                },
                "--output-dir", "/out",
                {
                    "type": "input_files",
                    "input_files_format": "space_separated_list",
                },
            ],
            "environment": {
                "CLASSIFIER_MODEL_PATH": {
                    "type": "ai_model",
                    "ai_model_source": "huggingface",
                    "ai_model": "org/my-classifier@v2",
                },
            },
        },
        "input_files": [
            "test-bucket/my_ai_job/sample1.fasta",
            "test-bucket/my_ai_job/sample2.fasta",
        ],
        "output_dir": "cts-output/my_ai_job_out",
        "runtime": "PT30M",
        "cpus": 4,
        "gpus": 1,
        "num_containers": 1,
        "memory": "16GB",
    },
)

print(res.json())  # {'job_id': '...'}
```

* `token_ci` is your KBase CI environment token.
* `--embedding-model` is a literal flag; the following dictionary resolves to the embedding
  model's staged filesystem path at job submission time.
* `CLASSIFIER_MODEL_PATH` is an environment variable whose value becomes the classifier model's
  staged filesystem path.
* Both models are delivered under the same default mount point, `/ai_models`, since no mount
  point was specified.
* `gpus` requests GPUs for the job's containers. This is unrelated to AI model delivery itself -
  the model files are placed on disk the same way regardless of `gpus` - but is relevant to most
  AI workloads. It defaults to `0`, and a job requesting `0` GPUs is never scheduled onto a
  GPU-capable node, so if a container expects a GPU to be available this must set
  explicitly or it will silently run on CPU-only nodes instead.

### Example: command line (`curl`)

The same job submission using `curl`:

```bash
curl -X POST "https://ci.kbase.us/services/cts/jobs/" \
    -H "Authorization: Bearer ${TOKEN_CI}" \
    -H "Content-Type: application/json" \
    -d '{
        "cluster": "perlmutter-jaws",
        "image": "ghcr.io/kbasetest/my_ai_image:0.1.0",
        "params": {
            "input_mount_point": "/in",
            "output_mount_point": "/out",
            "args": [
                "--embedding-model",
                {
                    "type": "ai_model",
                    "ai_model_source": "huggingface",
                    "ai_model": "sentence-transformers/all-MiniLM-L6-v2@main"
                },
                "--output-dir", "/out",
                {"type": "input_files", "input_files_format": "space_separated_list"}
            ],
            "environment": {
                "CLASSIFIER_MODEL_PATH": {
                    "type": "ai_model",
                    "ai_model_source": "huggingface",
                    "ai_model": "org/my-classifier@v2"
                }
            }
        },
        "input_files": [
            "test-bucket/my_ai_job/sample1.fasta",
            "test-bucket/my_ai_job/sample2.fasta"
        ],
        "output_dir": "cts-output/my_ai_job_out",
        "runtime": "PT30M",
        "cpus": 4,
        "gpus": 1,
        "num_containers": 1,
        "memory": "16GB"
    }'
```

You can check the job on the command line the same way as any other CTS job, e.g.
`curl -H "Authorization: Bearer ${TOKEN_CI}" "https://ci.kbase.us/services/cts/jobs/<job_id>"`.

## Loading a delivered model in the container (`transformers` example)

This section assumes the container code uses the HuggingFace `transformers` library to load the
model; the delivery mechanism itself (a plain local path) is the same regardless of what library
the container uses to load it.

Inside the container, the entrypoint receives the resolved, staged model path exactly as it was
rendered into the command line or environment - a plain local directory, not a HuggingFace repo
ID, and not something that requires any network access or HuggingFace Hub authentication to
load. Pass that path straight to `transformers`' `from_pretrained`. Since the path already
points at one exact, resolved snapshot, don't pass a `revision` (or pass `revision=None`), and
don't pass the original `model@ref` reference or repo ID anywhere in the loading call - the
directory on disk *is* the resolved revision. `transformers` already loads straight from a local
directory without contacting the Hub, so `local_files_only=True` isn't required, but passing it
anyway is good practice: it makes the no-network-access intent explicit and turns a wrong or
unexpected path into an immediate error instead of a potential silent network attempt.

Continuing the example above, where `--embedding-model` is passed as a CLI argument and
`CLASSIFIER_MODEL_PATH` as an environment variable:

```python
import os
import sys

from transformers import AutoModel, AutoTokenizer, AutoModelForSequenceClassification

def main():
    embedding_model_path = sys.argv[sys.argv.index("--embedding-model") + 1]
    classifier_model_path = os.environ["CLASSIFIER_MODEL_PATH"]

    embed_tokenizer = AutoTokenizer.from_pretrained(embedding_model_path, local_files_only=True)
    embed_model = AutoModel.from_pretrained(embedding_model_path, local_files_only=True)

    classifier_tokenizer = AutoTokenizer.from_pretrained(
        classifier_model_path, local_files_only=True
    )
    classifier_model = AutoModelForSequenceClassification.from_pretrained(
        classifier_model_path, local_files_only=True
    )

    # ... run inference using the loaded models and tokenizers ...

if __name__ == "__main__":
    main()
```

A few points specific to running under the CTS:

* The path passed in points directly at the snapshot's files (there's no nested HuggingFace
  cache structure to navigate, and no `refs`/`snapshots`/`blobs` layout) - treat it like any
  other local model directory.
* Since the files are already local, `from_pretrained` won't attempt to contact
  huggingface.co, so the container doesn't need external network access to load the model (only
  the CTS's own staging process at the compute site needs that access, before the job runs).
* If your container needs several differently-sized/capability models (e.g. a cheap, a medium,
  and a high-reasoning model), give each its own `ai_model`-typed `Parameter` with its own
  argument position or environment variable, as shown above.
