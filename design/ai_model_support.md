# AI Model Support — Implementation Plan

## Context

CTS should let job containers use pre-staged AI models (starting with HuggingFace) without
violating two hard constraints: JAWS/Globus can't transfer softlinks, and JAWS can only mount a
single dynamic directory (today used for refdata) per job. Through design discussion we settled
on the following shape, which this plan implements:

- AI models are **admin-registered only** (no user-triggered downloads — avoids arbitrary/unvetted
  content entering the system).
- Each registered AI model is an **immutable snapshot** identified by `(model, version)` — source-
  generic field names (see the naming note below) that for HuggingFace mean `(repo_id, resolved
  commit hash)`. Registration takes a single `ref` input (Chunk 5) — either a literal resolved
  hash or a real ref that exists at the provider (an HF branch or git tag). A mutable friendly tag
  can end up pointing at a snapshot for convenience, derived automatically from `ref` when it
  wasn't already a bare hash (Chunk 5). The physical staged data is never mutated in place —
  re-registering a different ref that resolves to the same already-staged snapshot simply moves
  the pointer (or adds a second one) without re-staging.
- All registered AI models live in **one shared, ever-growing "AI model store" directory per
  cluster**, populated incrementally (adding model #50 never touches models #1–49). An image opts
  into mounting the whole store as a single mount — mutually exclusive with a regular refdata
  mount, since JAWS only supports one dynamic mount.
- The on-disk layout is **not** HuggingFace's cache format (which relies on symlinks for
  cross-revision dedup) — each snapshot is stored as real files. We also don't try to preserve
  bare `from_pretrained("org/model")` resolution, because that depends on a mutable `refs/<name>`
  file, which is racy: the container may resolve it long after CTS resolved it at submission
  time, and a concurrent registration could change what it points to. Container code is required
  to reference the exact resolved hash (delivered by CTS), never the mutable name — this mirrors
  how this codebase already pins Docker images to `digest`, not `tag`.
- A job declares each wanted AI model by friendly reference (`model@ref`, where `ref` is
  a tag CTS has recorded a pointer for or a literal resolved version — the same `ref`/`version`
  distinction used at registration (Chunk 5): `ref` is whatever the caller supplies, tag or
  hash; `version` is always the resolved hash — see the naming-format note below) as its own
  dedicated `Parameter` in `args`/`environment` — each `Parameter` holds exactly one model
  reference, but a job may include several such `Parameter`s at once (e.g. separate env vars for a
  cheap/medium/high-reasoning model, each its own `Parameter`), unlike `INPUT_FILES`/`MANIFEST_FILE`,
  which are capped at one per job. CTS resolves each one's reference to an immutable hash at
  submission time, validates
  it's staged for the job's target cluster (fail fast otherwise), rewrites that `Parameter` with
  the resolved value, and delivers that one AI model's filesystem path into the container via the
  existing `Parameter` system (the same mechanism already used for `INPUT_FILES`/`MANIFEST_FILE`).
- **AI model bytes are pulled directly from HuggingFace at each cluster's staging location** —
  Perlmutter (NERSC) and the KBase refserv host both have confirmed egress to huggingface.co, so
  each fetches independently via `huggingface_hub.snapshot_download`, using shared staging code.
  There is **no S3 relay and no involvement of the CTS API server process in transferring model
  bytes** — this preserves the existing invariant that the always-on CTS server never touches
  bulk file content (refdata follows the same rule: users upload directly to S3, and all cluster
  staging is S3↔cluster, performed by NERSC/refserv/LRC, never by the CTS server itself).
  Lawrencium is currently disabled cluster-wide (see Chunk 6) and out of scope for this plan; if
  reactivated, it would not talk to HuggingFace at all, instead relying on whatever
  content-agnostic sync mechanism refdata uses for LRC at that time.
- We rely on `huggingface_hub.snapshot_download`'s own integrity verification (per-file hash/ETag
  checking against the hub, with retry on mismatch) rather than adding CTS-side checksumming —
  unlike refdata, this content never transits S3, so CRC64NVME (an S3-object-specific mechanism)
  doesn't apply here.
- Deliberately deferred (not in this plan): store garbage collection/retention, and any AI model
  source besides HuggingFace.

The plan keeps AI models as a **distinct type and entry points** from refdata — a new `AIModel`
type distinct from `ReferenceData`, new `stage_ai_model` runner methods distinct from
`stage_refdata` — rather than branching inside the existing refdata code paths on a type flag.
This follows the codebase's own precedent (`LawrenciumJAWSRunner` subclasses `NERSCJAWSRunner`
rather than adding conditionals) and keeps the already-working, well-tested refdata paths free of
new conditionals that could regress them.

That said, this separation is about the *public shape* (types, method names, call sites) — not a
mandate to duplicate logic. Where refdata and AI model staging genuinely do the same thing
(resolving a path, syncing bytes to a dtn, polling a remote job, atomically updating per-cluster
status), pull the shared logic into a helper function both paths call, rather than writing it
twice. `ReferenceDataStatus`/`AdminReferenceDataStatus` being **renamed to source-neutral
`StagingStatus`/`AdminStagingStatus`** and reused by both refdata and AI models (Chunk 1) and
`huggingface_client.py` being a shared module called from both the NERSC and refserv staging paths
(Chunks 3–4) are the model to follow. Chunk authors should actively look for this kind of
extraction opportunity as they implement each chunk, not just take the chunk descriptions below as
license to copy-paste the refdata equivalents.

### Naming notes

"Model" is heavily overloaded in this codebase (Pydantic models in `models.py`,
`model_construct()`, etc.). Every identifier introduced by this plan that refers to an AI model is
therefore prefixed `AIModel`/`ai_model_` — never bare `Model`/`model_` — to keep the two concepts
unambiguous at every call site. See the naming table at the end of this section.

**Field naming is source-generic, not HuggingFace-specific.** `AIModel`'s two identifying fields
are named `model` and `version` rather than `repo_id`/`revision_hash`, so that a future second
source isn't forced to misuse HF-flavored field names for a differently-shaped identifier (e.g. a
numeric model/version ID pair, which has no "repo" or "commit hash" at all). `model` is a
free-form string meaningful only in combination with `source` (for `HUGGINGFACE`, an `org/name`
repo id; a future source might use a numeric id, a URL, etc.) and `version` is likewise
source-defined (for `HUGGINGFACE`, a resolved git commit hash). Only Chunk 0's thin
`huggingface_client.py` adapter keeps the HF-native parameter names `repo_id`/`revision` /
`revision_hash`, since those are literally `huggingface_hub`'s own argument names, not CTS schema
— everything above that adapter (data model, Mongo, routes, job-submission resolution, rendering)
uses `model`/`version`. Chunk 1 adds a doc-string/description on both fields spelling out this
per-`source` meaning, since the fields alone don't carry it. A per-`source` format validator
(`match self.source: case HUGGINGFACE: ...`, the same branching pattern `Parameter._check_fields`
already uses on `self.type`) is possible and would be straightforward to add once there's a second
source to validate against; with only `HUGGINGFACE` registered today there's nothing to
distinguish yet, so Chunk 1 does not add one — a future chunk introducing a second source should
add the match-on-`source` validator at that point rather than leaving the fields unchecked. This is
intentionally the only forward-looking
generalization in this plan — the resolution/staging *mechanism* (resolve a mutable ref to an
immutable, hash-verified snapshot via `huggingface_hub`) remains HF-specific and is not abstracted,
since no second source is in scope (see "Explicitly deferred" at the end).

**`ref` vs. `version`.** CTS's own concept of "whatever the caller/admin supplied, possibly
unresolved" is named `ref`, distinct from `huggingface_hub`'s own `revision` kwarg (Chunk 0 keeps
that exact HF-native name for its own thin wrapper, `resolve_revision(repo_id, revision)` — that
one stays as-is, since it's HF's own API surface, not CTS's). `ref` borrows git's own vocabulary —
a git "ref" is precisely "a branch, tag, or commit-ish," exactly what this field holds — and reads
as clearly distinct from `version`, unlike "revision"/"version," which are near-synonyms in
everyday English. So: **`ref`** = whatever the caller/admin supplies (a branch name, a git tag, or
a literal hash — unresolved, possibly mutable if it's a branch or tag).
**`version`** = the resolved hash
only, always immutable, stored as `AIModel.version` (Chunk 1) and never used for anything else.
Chunk 0's `resolve_revision` is the one place `ref` gets translated into HF's own `revision` kwarg
name, right at the call site — every other mention of "revision" in this plan refers to that
HF-native wrapper, not to CTS's own `ref` concept.

### Key existing code confirmed reusable as-is (no changes needed)

- `jaws/wdl.py::generate_wdl`/`_generate_wdl` already take `relative_refdata_path` as an opaque
  `Path` computed by the caller — mounting the AI model store just means passing a different,
  **constant** (not per-job) relative path here. Since only one dynamic mount is ever active per
  job (see Chunk 7), no WDL-templating change is needed at all — the existing `dynamic_refdata`
  runtime line is reused unchanged regardless of which source is mounted.
- `Parameter`/`ParameterType` rendering pipeline (`argument_generator.py`) already has the exact
  shape needed for a new type (`INPUT_FILES`/`MANIFEST_FILE` are the templates to follow).
- HTCondor's refdata mount (`externalexecution/executor.py::_build_mounts`) is a plain
  `{host_path: (container_path, ro)}` bind mount of a live, NFS-shared directory — a
  shared, growing directory works today with zero transport changes, since nothing is copied
  per-job.
- The existing Lawrencium sync primitives (`setup_refdata_transfer_callback`/
  `get_refdata_transfer_result`, backed by JAWS/Globus) are content-agnostic — they transfer
  whatever is at a given NERSC-side path to LRC and are reusable unchanged for AI model
  subdirectories, exactly as for refdata.
  = Note, however, that LRC is currently disabled and out of scope. See below.
- The remote-code deployment mechanism NERSC already uses to ship small pure-Python modules and a
  pip-dependency install step to Perlmutter at server startup (`manager.py::_setup_remote_code`/
  `_upload_file_to_nersc`/`_install_pip_dependencies`) is reusable to also ship the new shared
  stager module and install `huggingface_hub` remotely — no new deployment mechanism needed.
- NERSC's `xfer`-QOS Slurm submission path (`manager.py::_process_manifest`, today used for
  refdata/job file transfers) is reusable as the execution vehicle for a remote AI-model-staging
  task — it already exists specifically because transfers can run long past SFAPI's 10-minute
  async task cap, which applies here too.

### Path-nesting change required everywhere staging writes to disk

Today, one refdata record's content always lands directly under `<cluster refdata
root>/<refdata_id>/` (confirmed in `nersc/manager.py::_get_relative_refdata_loc` and
`refserv/refdata_manager.py::_stage_refdata`, both hardcoded to a single `id` segment) and is
mounted 1:1 per image (`executor.py::_build_mounts` joins `refdata_host_path` directly with
`image.refdata_id`). The AI model store instead needs every model's content to land under a
**shared, fixed subdirectory** (e.g. `_ai_models/<source>/<model>/<version>/` — no opaque id, see
Chunk 1) so the *whole* `_ai_models/` directory can be mounted as one thing regardless of how many
models are in it. This requires new
sibling path-building helpers/methods (not edits to the existing ones) in `nersc/manager.py`,
`refserv/refdata_manager.py`, and `executor.py`.

### Naming table

| Concept | Identifier |
|---|---|
| Source enum | `AIModelSource` (`HUGGINGFACE`) |
| Per-cluster staging state enum | `StagingState` (renamed from `ReferenceDataState`, shared by both refdata and AI models — see Chunk 1) |
| Per-cluster staging status classes | `StagingStatus`/`AdminStagingStatus` (renamed from `ReferenceDataStatus`/`AdminReferenceDataStatus`, shared by both refdata and AI models — see Chunk 1) |
| Registered AI model record (internal / admin view) | `AIModel` / `AdminAIModel` — **no opaque id**; natural key is `(source, model, version)` — source-generic field names, see the naming note above (Chunk 1) |
| Natural key, passed around in place of three loose strings | `AIModelKey` (`NamedTuple`: `source`, `model`, `version`; `__str__` → `f"{source.value}/{model}/{version}"`), with `AIModel`/`AdminAIModel.key` as a derived property returning it, mirroring `JobImage.name_with_digest` (Chunk 1) |
| Mongo collection | `ai_models`, primary key `(source, model, version)` |
| Tag pointer collection | `ai_model_tags`, `{source, model, tag, version, tagged_by, tagged_on}` — `tagged_by`/`tagged_on` are overwritten each time the tag moves, not history |
| Registration/orchestration service | `AIModelRegistry` (`cdmtaskservice/ai_models/registry.py`) |
| Shared HuggingFace client (resolve + stage) | `cdmtaskservice/ai_models/huggingface_client.py` — HF-native `repo_id`/`revision` naming, per the naming note above |
| Runner method | `stage_ai_model(...)` |
| Completion callback | `ai_model_complete(key: AIModelKey)` — own route and payload shape, **not** shared with `refdata_complete`/`refdata_download_complete` (see Chunk 3) |
| NERSC path helpers | `_AI_MODEL_ROOT_DIR = "_ai_models"` (DTN-root subdirectory, `self._ai_model_root_loc`, `_get_relative_ai_model_loc(key: AIModelKey)`) and `_AI_MODEL_SCRATCH_DIR = "ai_models"` (Perlmutter-local scratch subdirectory, `_get_ai_model_scratch(key: AIModelKey)`) — two distinct directories, not one shared name; both take `AIModelKey` rather than three loose strings (see Chunk 3) |
| Shared job-flow state-update plumbing (extended, shared with refdata) | `EntityType.AI_MODEL` added alongside `JOB`/`PIPELINE_JOB`/`REFDATA`; `JobFlowStateUpdates.handle_exception`/`save_error`'s `entity_id` widens to `str \| AIModelKey`; new `JobFlowStateUpdates.update_ai_model_state(key: AIModelKey, update: StagingUpdate)` and `mongo.update_ai_model_state(cluster, key: AIModelKey, update, time)`, using `update_state.StagingUpdate` and `submitted_nersc_staging_download`/`staging_complete`/`staging_error` factories; new `InvalidAIModelStateError` (`exceptions.py`) mapped to new `ErrorType.INVALID_AI_MODEL_STATE`, used as `NERSCJAWSRunner._get_transfer_result`'s `errcls` for the `AI_MODEL` case alongside `InvalidJobStateError`/`InvalidReferenceDataStateError`; new `logfields.AI_MODEL_ID` (see Chunk 2) |
| Job input method (derived, not stored) | `JobInput.uses_ai_model_store()` (`bool(job_input.params.get_ai_model_parameters())`) |
| Image field (renamed, shared with refdata) | `JobImage.default_staged_data_mount_point` (was `default_refdata_mount_point`, kept as a deprecated alias) |
| Parameter field (renamed, shared with refdata) | `Parameters.staged_data_mount_point` (was `refdata_mount_point`, kept as a deprecated alias) |
| Job accessor (renamed, shared with refdata) | `Job.get_staged_data_mount_point()` (was `get_refdata_mount_point()`) |
| Parameter fields (new) | `Parameter.ai_model_source` (`AIModelSource \| None`) + `Parameter.ai_model` (`str \| None`, the `model@ref` part only), both required when `type is AI_MODEL` — same two-part split as the GET routes' `{source}/{model_ref:path}` (Chunk 10); `ai_model` is rewritten in place to the resolved `model@<version>` form at submission time (`ref` before resolution, `version` after — same distinction as Chunk 5's registration) |
| Parameter type | `ParameterType.AI_MODEL` — unlike `INPUT_FILES`/`MANIFEST_FILE`, multiple `AI_MODEL` `Parameter`s may appear in one job's `args`/`environment` (e.g. separate env vars for a cheap/medium/high-reasoning model), each resolving independently |
| `Parameters` method (new) | `get_ai_model_parameters(self) -> list[Parameter]` — collects every `AI_MODEL`-typed `Parameter` across `args`/`environment`, analogous to `get_file_parameter()` but multi-valued (`CONTAINTER_NUMBER`-style exemption from the "at most one" check in `_check_parameter`) |
| Routes | Admin routes on the existing shared `ROUTER_ADMIN`, no dedicated admin router — `POST /admin/ai-models/{source}/{model_ref:path}` (registration, `model_ref` here is `model@ref`, no request body), `GET /admin/ai-models/{source}/{model_ref:path}`, `PUT /admin/ai-models/{source}/{model:path}/{version}/{cluster}/recover`, `DELETE /admin/ai-models/{source}/{model:path}/{version}/{cluster}/clean` (mirroring refdata's `routes.py:670,986,1004,1026`; recover/clean's bare `{version}` segment is always a resolved hash, never `ref` — see Chunk 3's safety note). Public routes get their own new `ROUTER_AI_MODELS` (`tags=["AI Models - Experimental"]`, `prefix="/ai-models"`), `include_router`'d directly after `ROUTER_IMAGES` and before `ROUTER_REFDATA` in `app.py` so the OpenAPI UI section lands between "Images" and "Reference Data" — `GET /ai-models` and `GET /ai-models/{source}/{model_ref:path}` (mirroring `GET /images/{image_id:path}`); see Chunk 10 |

---

## Chunks

Each chunk below is independently implementable, reviewable, and testable. Chunks are numbered in
landing order. Rough dependency order: 0 and 1 first (no dependents block them); 2 (the
`AIModelKey`-consuming extension of the shared job-flow state-update plumbing) depends on 1, since
it needs `AIModelKey` and `StagingState`; 3 (NERSC staging) and 4 (KBase staging) depend on 1 and 2
and are independent of each other; 5 (the
`AIModelRegistry` service) depends on 3 and 4, not just 1 and 2 — it dispatches to
`flow.stage_ai_model(ai_model)` on the job flow objects, and `stage_ai_model` is the method Chunks
3 and 4 add. So Chunk 5 is sequenced after them, even though it's independently *testable* earlier
via a mocked `flow_manager` (see Chunk 5 below); 6 has no dependencies (a standalone doc-only TODO,
can land anytime); 7
depends on 3 and 4 (needs the NERSC/KBase mount wiring in place) and defines
`ParameterType.AI_MODEL`/`Parameter.ai_model_source`/`Parameter.ai_model`/
`get_ai_model_parameters()`/`uses_ai_model_store()`; 8 depends on 7 (resolves and rewrites the
`AI_MODEL` `Parameter`s 7
defines); 9 depends on 7 and 8; 10 depends on 1 and 5.

### Chunk 0 — Shared HuggingFace client (resolve + stage)
New module `cdmtaskservice/ai_models/huggingface_client.py` with two functions, both thin
wrappers around `huggingface_hub`:
- `resolve_revision(repo_id: str, revision: str) -> str` wraps
  `HfApi().model_info(repo_id, revision=revision)` to resolve a repo id + optional branch/tag to
  a commit hash, with no download.
- `stage_snapshot(repo_id: str, revision_hash: str, dest_dir: Path) -> None` wraps
  `huggingface_hub.snapshot_download(repo_id, revision=revision_hash, local_dir=dest_dir)` (real
  files, no cache-dir symlink layout — `local_dir` alone is sufficient as of `huggingface_hub`
  2.x, which no longer accepts the now-removed `local_dir_use_symlinks` kwarg). No checksumming —
  `snapshot_download` already verifies each file against the hub's recorded hash/ETag on
  download, and nothing here ever touches S3.

**Error translation happens here, at the source, not in a caller.** `resolve_revision` catches
every `huggingface_hub` exception that means "the caller supplied a bad `(repo_id, revision)`
pair" and re-raises it as a single new `AIModelResolutionError`, defined in this module — this is
the only place CTS ever calls into `huggingface_hub` for resolution, so putting the translation
here means every future caller (today, `AIModelRegistry.register_ai_model`, Chunk 5) gets it for
free rather than repeating the same `try`/`except` at each call site. Three exception types are
caught: `huggingface_hub.errors.RepositoryNotFoundError` (a nonexistent or inaccessible repo;
`GatedRepoError` is a subclass, so a gated repo is covered too), `RevisionNotFoundError` (a repo
that exists but a `ref` that doesn't), and `huggingface_hub.errors.HFValidationError` (a
syntactically malformed `repo_id`, rejected by `huggingface_hub` client-side before any HTTP call
is even made) — the third is included alongside the first two because a malformed `repo_id`
string is just as much a bad caller input as a well-formed one that happens not to exist; there's
no principled reason to translate one and not the other. Every other exception `model_info` can
raise (`HfHubHTTPError` for a 5XX/unexpected HF response, an `httpx2` transport/timeout/connection
error, HF being otherwise unreachable) is let through unchanged — none of those are the caller's
fault, and none of them should be reported to the admin as a 400; whatever catches them further up
the stack (today, `error_mapping.py`'s unmapped-exception fallback) treats them as an opaque 500,
which is the correct status for an infra failure. `stage_snapshot` does no such translation — by
the time it runs, `revision_hash` is always an already-resolved, CTS-verified hash (Chunk 5 step
1), so a not-found error at staging time would indicate a genuine infra problem (the snapshot
vanished from the hub between resolution and staging), not a bad caller input, and is let through
unchanged for the same reason.

**Tested against the real huggingface.co service, not mocked** — a tiny (~280kB), stable, public
test repo (`julien-c/dummy-unknown`) stands in for a real AI model, the same way
`image_remote_lookup_test.py` already exercises the real ghcr.io/Docker Hub registries rather
than mocking `crane`. Covers: resolving a bare (HEAD) hash and a branch name (`main`) to the same
known commit hash, plus a second, older commit hash on the same repo resolving to itself — proving
`revision` is actually honored rather than the lookup silently always resolving to HEAD;
`AIModelResolutionError` for a nonexistent repo, a nonexistent revision on a real repo, and a
syntactically malformed repo id; a genuine network failure (an unreachable HF endpoint, injected by
constructing `HfApi` with a bogus `endpoint` — the HTTP call itself is real, not mocked) passing
through unchanged, proving the `except` clause doesn't over-broadly swallow every failure mode; and
`stage_snapshot` actually placing every expected file from the repo on disk at its expected size,
as a real (non-symlink) file. Fast and small enough to run as part of the
normal automated suite rather than `test_manual`.

### Chunk 1 — Data model + Mongo DAO for registered AI models
Unlike `ReferenceData`, which gets a synthetic `uuid4` `id` (`refdata.py:100`) because an
arbitrary uploaded S3 file has no inherent stable name, a registered AI model's `(source, model,
version)` triple is *already* a natural, permanent, unique key by this plan's own invariant
(immutable snapshots identified by model + resolved version) — Chunk 1 puts a unique Mongo index
on exactly that triple. So `AIModel`/`AdminAIModel` have **no opaque id field** at all; the triple
is both the Mongo document key and the public identifier, the same shape `Image`/`JobImage`
already use (`(name, digest)`/`(name, tag)`, no synthetic id — see `images.py`/`mongo.py`'s
`get_image`/`save_image`), not the refdata shape. `model`/`version` are the source-generic field
names from the top-level naming note — for `HUGGINGFACE` these hold the HF repo id and resolved
commit hash respectively, but the field names themselves don't bake in that assumption.

In `models.py`: new `AIModelSource` enum (`HUGGINGFACE`), and `AIModel`/`AdminAIModel` (source,
model, version, registered_by, registered_on, `statuses`, and a derived `tags: list[str]` field —
not stored, populated at read time via reverse lookup; see below), with field
descriptions explaining that `model`/`version`'s concrete meaning is `source`-dependent (for
`HUGGINGFACE`: a `org/name` repo id and a resolved git commit hash).

**`AIModelKey`.** `(source, model, version)` is passed around constantly from this chunk on — Mongo
lookups and updates, NERSC/refserv path helpers, callback URLs, route handlers — so rather than
threading three loose strings through every one of those call sites, `models.py` adds

```python
class AIModelKey(NamedTuple):
    """ Uniquely identifies a registered AI model snapshot. """
    source: AIModelSource
    model: str
    version: str

    def __str__(self) -> str:
        return f"{self.source.value}/{self.model}/{self.version}"
```

and every later chunk that would otherwise take `source, model, version` as three parameters takes
a single `key: AIModelKey` instead (NERSC's `_get_relative_ai_model_loc`/`_get_ai_model_scratch`,
the completion callback builder/handler, `update_ai_model_state`, `recover_ai_model`, refserv's
staging/update methods). The two exceptions, where there's no key yet to build: `get_ai_model`
(takes `ref`, not yet a resolved `version`) and FastAPI route path params (the framework requires
separate path segments; the route handler builds an `AIModelKey` immediately and passes that down).
`AIModel`/`AdminAIModel` get a derived `key` property returning `AIModelKey(self.source, self.model,
self.version)`, the same pattern as `JobImage.name_with_digest`. `__str__`'s `source/model/version`
join is the one join convention used everywhere a composite AI-model identifier needs to become a
string — `get_ai_model_download_complete_callback`'s callback-URL path segment (Chunk 3) and Chunk
2's log messages both just call `str(key)` rather than reimplementing the join. `AIModelKey` is
defined in this chunk, alongside `AIModelSource`/`AIModel`, because Chunk 1's own Mongo DAO methods
(`get_ai_model`, `update_ai_model_state`) already need it; Chunk 2 is where it starts getting used
outside this chunk, in the shared job-flow state-update plumbing.

**`ReferenceDataState`/`ReferenceDataStatus`/`AdminReferenceDataStatus` are renamed to
`StagingState`/`StagingStatus`/`AdminStagingStatus`, shared by both refdata and AI models, not
reused verbatim under their refdata-flavored names.** Both the state's meaning (`CREATED,
DOWNLOAD_SUBMITTED, COMPLETE, ERROR`) and the status classes' fields are already generic across
staging source types, not refdata-specific — a distinct `AIModelState`/`AIModel`-flavored status
pair with the same values/fields would be pure duplication. But reusing them verbatim under their
existing refdata-specific names, while fine internally, would leak into the public API: FastAPI
derives OpenAPI component names from the Python class, so `AIModel`/`AdminAIModel`'s schema would
show nested `ReferenceDataState`/`ReferenceDataStatus` components with no textual link to "AI
model" — confusing for a consumer who has never touched refdata. A field-level schema `title`
doesn't fix this, since the component itself (not just the referencing field) is what's named in
`$defs` and shared by every place that `$ref`s it. The actual fix is a plain rename at the
source — `ReferenceDataState` → `StagingState`, `ReferenceDataStatus` → `StagingStatus`,
`AdminReferenceDataStatus` → `AdminStagingStatus` — in `models.py`, updating every existing
call site (`mongo.py`, `refdata.py`, `routes.py`, `job_state.py`, `update_state.py`,
`error_mapping.py`, the jobflow/refserv modules, and their tests) alongside adding the new AI-model
ones. This is a one-time, mechanical, repo-wide rename (not a new parallel type), does not change
any class's fields, values, or behavior, and both refdata's and AI models' public schemas end up
showing the same neutral, accurate names. Add a minimal
separate pointer collection/model for tags: `{source, model, tag, version, tagged_by, tagged_on}`,
upserted (replacing any prior pointer) on each registration under that tag. `tagged_by`/`tagged_on`
record who pointed this tag at its current `version` and when — the pointer doc's own equivalent of
`AIModel.registered_by`/`registered_on`, needed because a tag can move to a new model/version long
after the original model registration, at which point the model doc's own
`registered_by`/`registered_on` (which never change) would no longer reflect who's responsible for
*this* tag's current meaning. This also makes the pointer collection itself the durable record of
"who pointed this tag here," rather than relying solely on a log line (see `save_ai_model` below
and Chunk 5).

The `tags` field on `AIModel`/`AdminAIModel` is populated via a **reverse lookup** against the tag
pointer collection at read time, opt-in via `get_ai_model`'s `populate_tags` flag (see below) — it
is not stored directly on the `ai_models` document, since a tag can be repointed to a different
version at any time and the pointer collection is the single source of truth for "what does this
tag mean right now." A model with no tag pointing at it returns `tags: []` when populated; when
`populate_tags=False`, `tags` is left unset rather than queried.

`get_ai_models` (the list method backing `GET /ai-models`) populates `tags` for every result the
same way `get_refdata`/`get_images` cap results today (`mongo.py:345`, `mongo.py:1649`'s
`.limit(1000)`) — load up to 1000 `ai_models` docs plus every `ai_model_tags` doc for the matching
`(source, model)` pairs, and resolve `tags` for each in-process, rather than a per-result DB round
trip. `# TODO AI_MODEL` once the catalog grows past the point this in-process join is cheap
enough, `get_ai_models` will likely need real pagination (not just a higher limit) — out of scope
for this plan, called out here since none of the existing `get_refdata`/`get_images` precedents
this chunk mirrors have it either.

In `mongo.py`: new `ai_models` collection, keyed by `(source, model, version)`, with a unique
index on that triple; a separate `ai_model_tags` pointer collection, `{source, model, tag} ->
version`, with a unique index on `(source, model, tag)` (the forward, resolve-a-tag direction) and
a second, non-unique index on `(source, model, version)` (the reverse direction, `version` leading
since `tag` isn't a prefix of the first index's key order) to support the `tags` reverse lookup
below without a collection scan.

Four DAO methods, mirroring the refdata ones:
- `save_ai_model(ai_model, tag: Tag | None = None)` (full behavior below), where `Tag` is a small
  frozen dataclass/`NamedTuple` bundling the three fields an upsert needs together: `tag: str`,
  `user: str`, `time: datetime.datetime`. The timestamp is caller-supplied, not computed inside
  `save_ai_model` — `mongo.py` methods never generate their own timestamps (every state-transition
  method takes `time: datetime.datetime` from the caller, e.g. `mongo.py:652,690,750`), and
  `save_ai_model` shouldn't be the exception. The caller (`AIModelRegistry.register_ai_model`,
  Chunk 5) computes it once via `utcdatetime()` (mirroring `refdata.py:105`) before calling.
- `get_ai_model(source, model, ref, resolve_tag: bool = False, populate_tags: bool =
  False)` (full behavior below).
- `get_ai_models()`: list method backing `GET /ai-models` (behavior below).
- `update_ai_model_state(...)`: atomic per-cluster state update, using the same `array_filters`
  pattern as `update_refdata_state`.

`save_ai_model` persists the `AIModel` doc and, when `tag` is given, also upserts the corresponding
`ai_model_tags` pointer doc in the same call — one method writing to both collections.
Deliberately named `tag` (singular), not `tags`, and kept separate from the `AIModel.tags` field on
the object being saved: `register_ai_model` (Chunk 5) only ever assigns one tag per registration,
and more importantly `AIModel.tags` is a derived, read-only view computed from the `ai_model_tags`
collection at read time (see below) — it has no DB-backed meaning yet on a not-yet-persisted
`AIModel`, so it can't double as the write instruction here without conflating "what tag to assign"
with "what tags currently resolve to this model."

Unlike `save_refdata` (strictly insert-only — raises `ReferenceDataExistsError` on any existing
doc, no update path at all), `save_ai_model` must tolerate being called against an
already-existing `(source, model, version)`, since that's exactly the path Chunk 5's
already-registered branch uses. Behavior when the `ai_models` doc already exists:
- The `ai_models` write itself is always a no-op in this case (`$setOnInsert`-style — same
  document content, nothing to change) regardless of `tag`; `AIModel.registered_by`/
  `registered_on` are never overwritten by a later call, preserving the original registration's
  attribution.
- `tag=None`: no tag-pointer write either. A no-op call overall (the already-registered branch
  with no new tag to add). Nothing to log, no `tagged_by`/`tagged_on` change.
- `tag` given and the pointer `{source, model, tag.tag}` already resolves to this same `version`:
  also a no-op on the tag pointer, **including `tagged_by`/`tagged_on`** — re-asserting an existing
  mapping changes nothing, full stop, so the pointer doc's existing `tagged_by`/`tagged_on` (from
  whoever originally pointed it there) are left untouched rather than refreshed to `tag.user`/
  `tag.time`. Nothing to log (idempotent re-registration under a tag the model already has, not a
  new fact).
- `tag` given and the pointer `{source, model, tag.tag}` doesn't exist yet, or currently resolves
  to a *different* version: upserts the pointer to now resolve to this `version`, setting
  `tagged_by=tag.user` and `tagged_on=tag.time` — this is the "move the pointer" case from
  the plan's opening section (a tag is mutable; re-registering under an existing tag name repoints
  it, it does not create a second pointer or error). **This is the only sub-case that changes
  durable state on an already-registered model, and the only one Chunk 5 logs.** `tagged_by`/
  `tagged_on` on the pointer doc record only the *current* tagging event — each move overwrites the
  previous mover's name/time, so Mongo alone can never show that a tag passed through two or three
  admins' hands before landing where it is now. **The log line is therefore not redundant with the
  stored fields — it is the only durable history of intermediate tag moves**; without it, an
  intermediate move (and who made it) is lost the moment the next move overwrites the pointer doc.
  `save_ai_model` itself returns enough information for the caller
  (`AIModelRegistry.register_ai_model`, Chunk 5) to know this sub-case occurred (e.g. whether the
  tag pointer was newly created/changed vs. left untouched), since `save_ai_model` is a plain DAO
  method and doesn't log on its own — logging is the registry's responsibility, consistent with how
  other service-layer logging in this codebase happens above the DAO layer, not inside it.

The tag-pointer upsert itself should use a conditional aggregation-pipeline update (MongoDB's
pipeline-style `update_one`, already used elsewhere in this codebase, e.g. `mongo.py:970-995`) so
that `tagged_by`/`tagged_on` are only overwritten when the pointer is newly created or its
`version` actually changes — leaving them untouched when an existing pointer is reasserted with the
same `version` — all in a single atomic upsert, rather than a separate read-then-decide step that
would be racy under concurrent registrations.

`get_ai_model`'s two boolean flags mirror how `get_refdata_by_id`/`get_image` already use mode
switches (`as_admin: bool = False`) rather than separate method names for closely-related lookup
behavior. The flags are independent, since they answer different questions (how to find the
record vs. what to attach to it once found):
- `resolve_tag=False` (the default): exact-triple lookup only, against the unique `(source, model,
  version)` index. Used wherever the caller already has a resolved version and wants to assert an
  exact match — e.g. the registry's already-registered check (Chunk 5) and
  `update_ai_model_state`, which must target one exact document, not silently reinterpret an
  unresolved tag.
- `resolve_tag=True`: on a miss from the exact-triple lookup, falls back to a query against the tag
  pointer's `(source, model, tag)` index to get a version, then retries the exact-triple lookup
  with that version — version first, tag second, always in that order (never the reverse), because
  trying the tag first would let someone register a tag string that happens to collide with a
  real version value and have it silently shadow that version for every caller (full rationale in
  Chunk 8). Used by job-submission resolution (Chunk 8) and the GET routes (Chunk 10), which only
  have a raw user-supplied string and don't know in advance whether it's a version or a tag.
- `populate_tags=False` (the default): skip the reverse-lookup query below entirely and return
  `AIModel`/`AdminAIModel` with `tags` unset. Most callers don't need it — the registry's
  already-registered check only needs existence, `update_ai_model_state` only needs to target
  a document, and Chunk 8's job-submission resolution only needs `status`/`version` — so defaulting
  it off avoids a wasted query on every `AI_MODEL` `Parameter` resolved per job submission.
- `populate_tags=True`: once a record is found (by either `resolve_tag` branch), also run the
  separate reverse lookup against the tag pointer's `(source, model, version)` index to fill in
  `tags` — always its own query, not just "the tag that resolved the reference" (when
  `resolve_tag=True` found a match via the tag branch), since other tags may also point at the
  same version and the reverse lookup returns all of them. Used only by the GET routes (Chunk 10)
  and `get_ai_models` (the list method), the two places that actually surface `tags` to a caller.

With both flags set, this is up to three sequential queries against two collections/indexes; a
single aggregation pipeline (`$lookup` from `ai_models` into `ai_model_tags` twice — once
correlated on `tag`, once on `version`) could do this as one round trip instead, but since this
isn't a per-job hot path (Chunk 8 resolves at most a handful of `AI_MODEL` `Parameter`s per job
submission, and always with `populate_tags=False`), the simpler sequential-query form is preferred
for readability/testability, consistent with every other DAO method in this plan and in
`refdata.py`'s equivalents.

Fully testable against the existing Mongo test fixture, no cluster/staging logic involved.

### Chunk 2 — Extend shared job-flow state-update plumbing for AI models
The existing shared job-flow error/state-update helpers (`jobflows/state_updates.py`) predate this
plan and only know about two kinds of entity: `EntityType.JOB`/`PIPELINE_JOB` (plain job IDs,
routed to `update_job_state`) and `EntityType.REFDATA` (refdata IDs, routed to
`update_refdata_state`) — both assume a single opaque ID string. AI models have no such ID (Chunk
1), so `EntityType` gains a third member, `AI_MODEL`, and the shared methods widen to accept either
shape, using Chunk 1's `AIModelKey` and `StagingState`:

- `JobFlowStateUpdates.handle_exception`/`save_error`: `entity_id: str | AIModelKey`. Logging
  (`extra={logfield: str(entity_id)}`) works unchanged for both shapes given `AIModelKey.__str__`
  (Chunk 1) — `str` is already a no-op, so no branching is needed there. `handle_exception`'s
  `entity_desc` dict (used to build the "Error {erraction} {entity_desc}." log sentence) gains
  `EntityType.AI_MODEL: "AI model"` alongside the existing `"job"`/`"pipeline job"`/`"refdata"`
  entries. `save_error`'s dispatch gains a third arm: `EntityType.AI_MODEL` routes to a new
  `update_ai_model_state` (below) instead of `update_refdata_state`/`update_job_state`.
- New `JobFlowStateUpdates.update_ai_model_state(key: AIModelKey, update: StagingUpdate)`, parallel
  to `update_refdata_state`, forwarding to a new `mongo.update_ai_model_state(cluster, key, update,
  time)` (the DAO method Chunk 1's naming table and later chunks already call for, now specified to
  take `AIModelKey` rather than three positional strings).
- `logfields.py` gains `AI_MODEL_ID = "ai_model_id"`, selected alongside `REFDATA_ID`/`JOB_ID`
  wherever `handle_exception`/`NERSCJAWSRunner._get_transfer_result` pick a logfield based on
  `entity_type`.
- `NERSCJAWSRunner._get_transfer_result` (`nersc_jaws.py`; inherited as-is by
  `LawrenciumJAWSRunner`, which has no override — `kbase.py`'s `KBaseRunner` has no equivalent
  method at all, since its HTCondor completion path doesn't poll a remote task the same way)
  currently picks between `InvalidReferenceDataStateError`/`InvalidJobStateError` for its `errcls`
  based on `entity_type`; this becomes a third option. New `exceptions.InvalidAIModelStateError`
  (sibling to `InvalidReferenceDataStateError`, not a reuse of it — the state-error exceptions are
  per-resource control-flow signals, not shared schema like `StagingState` was, so there's no reuse
  pressure, and leaving `InvalidReferenceDataStateError`/`ErrorType.INVALID_REFDATA_STATE`'s
  existing numeric code untouched avoids relitigating the public error-code contract), mapped via
  `error_mapping.py` to a new `ErrorType.INVALID_AI_MODEL_STATE = (30420, "Invalid AI model state")`
  in `errors.py`, placed after `NO_REFDATA_CLUSTER_STATUS` in the 304xx block.

Chunks 3 and 4's `stage_ai_model`/`ai_model_complete` methods pass an `AIModelKey` wherever today's
refdata code passes `refdata.id`, and use this plumbing for error handling exactly as
`stage_refdata`/`refdata_complete` already do for refdata — no separate error-handling path is
invented for AI models.

Testable in isolation against `jobflows/state_updates.py`'s existing test conventions (mocked
Mongo/Kafka), no cluster/staging logic involved — the same shape as testing today's
refdata/job branches of `handle_exception`/`save_error`.

### Chunk 3 — NERSC/Perlmutter AI model staging
Two genuinely distinct NERSC directories are in play here, and they need two distinct constants,
not one shared name:

**DTN-root subdirectory** (under `self._refdata_root`, i.e. `jaws_refdata_root_dir` — the shared
filesystem both JAWS-mounted jobs and staging processes read/write final content from, rooted at
e.g. `/global/dna/kbase/reference/jaws/cdm_task_service/<site>/`). Refdata has **no** directory
constant at this level at all — `_get_relative_refdata_loc(refdata_id) = self._work_loc /
refdata_id` puts refdata IDs **directly** under the site-string root, flat, with no intervening
`refdata/` folder (confirmed: a live `localdev` root contains only bare refdata UUID directories,
nothing else). AI models need their own clearly-named subdirectory here specifically so they don't
end up sitting flat in that same pile of refdata UUIDs — new constant `_AI_MODEL_ROOT_DIR =
"_ai_models"` (leading underscore to stand out from the flat UUID directories it sits alongside),
precomputed in `__init__` as `self._ai_model_root_loc = self._work_loc / _AI_MODEL_ROOT_DIR`
(mirroring how `self._refdata_root` is already a plain attribute, not a zero-arg method). The
constant mount target (no per-job id) is just `self._ai_model_root_loc` directly. Since AI models have no opaque
id (Chunk 1), the per-model path segment under it is `<source>/<model>/<version>`, not just
`<model>/<version>` — `source` must lead, since the Mongo unique key is `(source, model, version)`,
not `(model, version)` (Chunk 1): a future second source could register a `model` string that
collides with an existing `HUGGINGFACE` one (e.g. both happen to use `org/model`), and dropping
`source` from the on-disk path would silently collide two different models' staged bytes into the
same directory — reintroducing on disk exactly the ambiguity the Mongo key was designed to prevent.
For `HUGGINGFACE`, `model` itself already contains a `/` (e.g. `org/model`), so this is simply two
extra natural directory levels, not a different kind of path:
`_get_relative_ai_model_loc(key: AIModelKey) → self._ai_model_root_loc / key.source / key.model /
key.version` — takes the `AIModelKey` from Chunk 1 rather than three loose parameters, per that
chunk's note.

**Perlmutter-local scratch subdirectory** (under `self._perlmutter_scratch`, i.e. Perlmutter's
`$SCRATCH` — ephemeral staging-in-progress space, unrelated to the DTN root above). This is
sibling to the existing `_JOBS_DIR = "jobs"`/`_REFDATA_DIR = "refdata"` scratch constants
(`manager.py:88-89`), which have no underscore prefix, so the new one matches that style rather
than the DTN-root constant's: `_AI_MODEL_SCRATCH_DIR = "ai_models"`. New
`_get_ai_model_scratch(key: AIModelKey) → self._perlmutter_scratch / self._work_loc /
_AI_MODEL_SCRATCH_DIR / key.source / key.model / key.version`, for staging an individual snapshot —
parallel to `_get_refdata_scratch`, also taking `AIModelKey`, and carrying the same `source` segment
as `_get_relative_ai_model_loc` above, for the same collision-avoidance reason.

(Neither of the above is related to the unrelated `ai_models` Mongo collection or the
`cdmtaskservice/ai_models/` Python package, which keep their existing names as-is.)

Staging itself runs remotely, not on the CTS server: `nersc/remote.py` imports
`huggingface_client.py` (Chunk 0) unconditionally at module load time so that
`manager.py::_get_dependencies`'s import-tree walk (seeded from `remote.py`, already how every
other remote-side module and pip dependency is discovered — see `_CTS_DEPENDENCIES`/
`_PIP_DEPENDENCIES`) picks up both the new module (shipped to Perlmutter automatically via
`_setup_remote_code`/`_upload_file_to_nersc`) and `huggingface_hub` itself (installed
automatically via `_install_pip_dependencies`) with no manual dependency-list edit. Because the
walk inspects live `sys.modules` entries via `inspect.getmembers`, the import must be a plain,
unconditional top-level import in `remote.py` — not deferred/lazy — or it won't be found.
`jobflows/nersc_jaws.py::stage_ai_model(ai_model)` submits a
remote `xfer`-QOS Slurm job (via the existing `_process_manifest` submission path, parametrized
for this new task instead of a file-manifest download) that runs `huggingface_client.stage_snapshot`
directly against huggingface.co on the Perlmutter login node in a temporary location and syncs to
the dtns in the same way as refdata, then transitions state via
`update_state.submitted_nersc_staging_download` → (on the resulting callback)
`ai_model_complete` (which itself calls `update_state.staging_complete`).

**Completion callback is a new, dedicated route, not a reuse of refdata's.** Refdata's callback
(`get_refdata_download_complete_callback`, `callback_url_paths.py`) bakes a single opaque
`refdata_id` path segment into the callback URL
(`/callback/refdata_download/{refdata_id}/{cluster}`), which round-trips through the remote Slurm
job and back via the HTTP GET that reports completion (`routes.py::refdata_download_complete` →
`runner.refdata_complete(refdata_id)`). Since AI models have no equivalent single opaque id, the
callback needs a different path shape carrying `(model, version, cluster)` instead — e.g.
`get_ai_model_download_complete_callback` building
`/callback/ai_model_download/{source}/{model:path}/{version}/{cluster}`, mirroring the
admin registration route's existing `{source}/{model:path}` pattern for slash-safe model ids. A
`:path` converter followed by further fixed/`[^/]+` segments is not ambiguous in practice — unlike
every *other* `:path` use in this codebase (`routes.py:508,593,653,671`), which are all the final
route segment, this one has `{version}`/`{cluster}` after it, but Starlette's `:path` converter
compiles to a greedy-but-backtracking `.*`, and since neither trailing segment is itself another
`:path`, the regex backtracks to the unambiguous split (confirmed by compiling the route pattern
and matching a slash-containing example `model` value against it). **This relies on `version`
never containing a `/` wherever it appears as a bare path segment** — true for a resolved hash (for
`HUGGINGFACE`, a git commit hash, never slash-containing) but *not* true for a tag: git tag names
are explicitly allowed to contain `/` (e.g. `releases/v1.0`), so if a raw tag string were ever
passed as a bare `{version}` segment instead of a resolved hash, the greedy `{model:path}` would
backtrack to the wrong split silently (confirmed: `.../org/my-model/releases/v1.0/<cluster>` with
`version="releases/v1.0"` parses as `model="org/my-model/releases", version="v1.0"`). Every
`{version}`-as-bare-segment route in this plan (this callback, the external-exec state-update
route in Chunk 4, and the admin recover/clean routes in Chunk 10) is safe only because `version` is
always an already-resolved hash at those call sites, produced by CTS itself (`resolve_revision`,
or `get_ai_model(..., resolve_tag=True)`'s internal tag resolution), never a raw user-supplied tag
— the two GET routes
that *do* accept a possibly-ambiguous user string bundle it into one `model_ref:path` segment
specifically to sidestep this (Chunk 10). Any future route that takes a tag directly as a bare path
segment (rather than through `model_ref:path` or an already-resolved hash) would need to either
also use the combined `:path` form or forbid `/` in registered tags. New
`routes.py::ai_model_download_complete` route and `nersc_jaws.py::ai_model_complete(key:
AIModelKey)` (analogous to `refdata_complete(refdata_id)`, `nersc_jaws.py:718-742`; `AIModelKey`
from Chunk 1, not three loose parameters), rather than overloading the existing refdata
callback/route with a type discriminator.

**Building the callback URL, not just matching it.** The route pattern matching correctly (above)
only covers the receiving side. `callback_url_paths.py`'s existing `_get_callback(subpath, root_url,
entity_id, cluster)` helper has only two variable-content slots — `entity_id` and `cluster` — which
is enough for refdata's single `refdata_id`, but the AI model route needs **three** variable
segments before `cluster` (`source`, `model`, `version`), not one. `entity_id` must therefore carry
all three joined together, not just `model`/`version`: `get_ai_model_download_complete_callback(key:
AIModelKey, ...)` passes `entity_id=str(key)` (Chunk 1's `AIModelKey.__str__`, the one join
convention used everywhere a composite AI-model identifier needs to become a string) to
`_get_callback` — `_get_callback` itself needs no change, since it already just does
`os.path.join(..., entity_id, ...)` and doesn't care whether `entity_id` contains embedded `/`
characters, or how many logical segments got joined into it.

Egress to huggingface.co from Perlmutter is already confirmed (`curl --head
https://huggingface.co` from a Perlmutter login node returns `200`), and installation of
`huggingface_hub` itself is handled automatically by the existing dependency-discovery mechanism
described above — no separate infra step needed.

### Chunk 4 — KBase/HTCondor AI model staging
`RefdataManager` is constructed with two independently-configured paths, `refdata_path`
(`self._refpath`) and `refdata_meta_path` (`self._metapath`) — in production these are sibling
directories, `data/` and `metadata/`, under a shared root (confirmed: a live deployment host shows
exactly these two top-level entries). `self._refpath`/`data/` is where individual `refdata_id`
directories already live directly (`refdata_manager.py:70,111`), so it's refdata's own content
root, not a generic parent shared with unrelated content types.

`_ai_models` is conceptually a special kind of refdata — one shared blob mounted the same way a
regular refdata mount is (Chunk 7), mutually exclusive with it for the same JAWS/HTCondor
single-dynamic-mount reason — so it belongs *inside* `self._refpath`/`data/` alongside the
`refdata_id` directories, not as a third top-level sibling to `data`/`metadata`. This isn't just a
stylistic choice: `externalexecution/executor.py`'s HTCondor mount (Chunk 7) builds
`Path(self._cfg.refdata_host_path) / "_ai_models"`, and `refdata_host_path` is CTS's own
independently-configured belief about where the execute host sees `self._refpath`/`data/` mounted
(`config.py:78`) — a different config surface entirely from `refdata_path`/`refdata_meta_path`,
coordinated only by convention, not derived from the refserv service's own config. Since
`refdata_host_path` only ever corresponds to `self._refpath`/`data/`, not `self._metapath`/
`metadata/` or any new top-level directory, `_ai_models` has to land directly under `self._refpath`
to be reachable at `Path(refdata_host_path) / "_ai_models"` on the execute-host side — a sibling
directory to `data/` would be invisible to that mount path entirely.

New `_stage_ai_model` calling the shared `huggingface_client.stage_snapshot` (Chunk 0) directly
against `self._refpath / "_ai_models" / ai_model.source / ai_model.model / ai_model.version` — the
same `_ai_models` name as NERSC's `_AI_MODEL_ROOT_DIR` (Chunk 3), not its `_AI_MODEL_SCRATCH_DIR`
— no opaque id, per Chunk 1, same nested `<source>/<model>/<version>` layout (and the same reason
for including `source`: avoiding a collision between two different sources' models that happen to
share a `model` string) as the NERSC side — no archive/unpack step is needed here (unlike refdata)
since
`snapshot_download` writes final files directly, so none of `_unpack_refdata`'s
tar/checksum-manifest logic applies. New refserv route + `RefdataServiceClient.stage_ai_model()` +
`KBaseRunner.stage_ai_model()` delegating to it, mirroring today's `stage_refdata` delegation.
Refserv's existing outbound egress to huggingface.co is assumed fine per current setup; if not,
that's an infra fix, not a design change.

Refdata's state-update route, `PUT /external_exec/refdata/{refdata_id}/{cluster}/update/{new_state}`
(`routes.py:1339-1361`), is keyed by the single opaque `refdata_id` path segment — the same
id-shaped assumption the completion callback makes (see Chunk 3). Since AI models have no id, this
needs its own route with `(model, version)` in place of `refdata_id` — e.g. `PUT
/external_exec/ai-models/{source}/{model:path}/{version}/{cluster}/update/{new_state}` — whose
handler builds an `AIModelKey` (Chunk 1) from the three path segments immediately, then calls a
corresponding `KBaseRunner.update_ai_model_state(key: AIModelKey, new_state, update)` alongside the
existing `update_refdata_state`, rather than overloading the refdata route.

In `externalexecution/executor.py::_build_mounts`: new branch — when
`job.job_input.uses_ai_model_store()`, mount `Path(self._cfg.refdata_host_path) / "_ai_models"`
(constant, not keyed by an id) at `job.get_staged_data_mount_point()` (see Chunk 7).

Testable independently of Chunk 3.

### Chunk 5 — `AIModelRegistry` service (admin registration orchestration)
New `cdmtaskservice/ai_models/registry.py`, shaped like `refdata.py::Refdata`:
`register_ai_model(source, model, ref, user)` — `ref` is the
*only* registration input, and it must be either a literal resolved hash or a real ref that exists
at the provider (an HF branch or git tag) — never a CTS-invented name. Whether a tag gets recorded,
and what it's called, is *derived* from `ref` itself, not supplied separately:
1. Resolve `ref` to a version (Chunk 0's `resolve_revision(repo_id=model, revision=ref)` for
   `HUGGINGFACE` — this is the one call site that translates CTS's `ref` into HF's own `revision`
   kwarg name; see the naming note near the top of this section). **If resolution itself fails
   because the repo doesn't exist or the given `ref` isn't a real branch/tag/hash on it**,
   `register_ai_model` fails the request before any Mongo write — no `AIModel` doc, no per-cluster
   `CREATED` statuses, no tag pointer — the same "fail before persisting anything" shape as every
   other precondition check in this step. `resolve_revision` itself (Chunk 0) is what raises this
   as a single `AIModelResolutionError`, since that's the one place CTS calls into
   `huggingface_hub` for resolution — `register_ai_model` doesn't catch any HF-specific exception
   itself, it just lets `AIModelResolutionError` propagate. `error_mapping.py` maps
   `AIModelResolutionError` to a new `ErrorType.AI_MODEL_RESOLUTION_FAILED` and
   `HTTP_400_BAD_REQUEST`, mirroring the existing
   `image_remote_lookup.py::ImageInfoFetchError`/`ErrorType.IMAGE_FETCH` precedent for a bad image
   reference. **If HF itself is unreachable, or returns an unexpected error, resolution instead
   raises unchanged** (an `httpx2` transport/timeout error, or `huggingface_hub`'s own
   `HfHubHTTPError` for anything that isn't "repo not found"/"revision not found") — that's a
   CTS/network-side condition, not a bad `ref`, so there's nothing to translate: it propagates
   through `register_ai_model` and out to `error_mapping.py`'s unmapped-exception fallback, which
   already reports an opaque `HTTP_500_INTERNAL_SERVER_ERROR` for any exception with no entry in
   its map — the correct status for an infra failure, with no new code needed to get it.
2. Determine whether `ref` was already a bare hash or a named ref: compare the resolved
   version against the literal `ref` string (case-insensitive). If they match, `ref` was
   already the hash — nothing to tag. If they differ, `ref` was a named ref (branch/tag) that
   HF just resolved for us — proof it's real — so `ref` itself becomes the tag to record
   (`tag = ref` in the steps below, `None` otherwise). No extra network call: this reuses the
   result `resolve_revision` already returned in step 1.
3. **No separate existence check before writing.** Calling `get_ai_model(source, model, version)`
   first and branching on its result before deciding whether to call `save_ai_model` would be a
   check-then-act race: two concurrent `register_ai_model` calls for the same `(source, model,
   version)` could both see "not registered," both proceed down the new-registration path, both
   construct an `AIModel` doc, and both call `flow.stage_ai_model` (double-staging the same bytes)
   before either write lands. Instead, bootstrap per-cluster
   `CREATED` statuses via `flow_manager.list_usable_clusters()` (mirroring `create_refdata`),
   construct `Tag(tag=tag, user=user, time=utcdatetime())` (Chunk 1) when step 2 derived a `tag`,
   and call `save_ai_model(ai_model, tag=that_tag_or_None)` **unconditionally** — its own write to
   `ai_models` is already a single atomic `$setOnInsert`-style upsert (Chunk 1), so exactly one of
   two concurrent calls for the same triple wins the insert; the request order no longer matters.
   `save_ai_model` returns whether its `ai_models` write was a genuine insert or hit an existing
   doc (mirroring the `did_upsert`/`upserted_id` signal `save_refdata` already uses,
   `mongo.py:1577-1584`), and the registry branches on *that* returned flag, not on a separate
   pre-check:
   - **Newly inserted**: this call's registration is the one that "won" — continue to step 4 to
     kick off staging.
   - **Already existed**: this call's registration lost the race (or simply came after a prior
     registration) — this is not an error. Multiple tags may legitimately point at the same
     `(source, model, version)` over time (e.g. the same commit gets re-registered later via a
     different branch name that now also resolves to it), and since re-staging already-present
     bytes would be wasted work, this path's only remaining effect is whatever `save_ai_model`
     already did to the tag pointer as part of the same call (Chunk 1's no-op/upsert sub-cases) —
     step 4's `stage_ai_model` calls are skipped entirely, since the snapshot is already staged or
     staging. There's no separate "add a tag" method or route, and no second `save_ai_model` call
     needed here — the single unconditional call in this step already did everything required for
     both outcomes; this is the same `register_ai_model`/`POST /admin/ai-models/...` entry point
     handling both cases, since from the caller's perspective both are just "register this model,"
     whether or not the underlying bytes need staging.

     **Log only when `save_ai_model` actually moved/created the tag pointer** — i.e. Chunk 1's
     last `save_ai_model` sub-case (pointer absent or pointing elsewhere), not every call on this
     already-registered branch: no derived `tag`, or a `tag` that already resolves to this same
     `version`, changes nothing and has nothing worth logging (Chunk 1). This sub-case is also the
     only one where the pointer doc's `tagged_by`/`tagged_on` (Chunk 1) get set/updated, but those
     fields only ever hold the *current* tagging event — each later move overwrites the previous
     one, so Mongo alone loses any intermediate mover's identity/time the moment a tag changes
     hands again. The log line is the only durable record of those intermediate moves, not a
     redundant echo of what's already in Mongo. `AIModelRegistry.register_ai_model` logs this
     specific sub-case explicitly (mirroring the existing
     `logging.getLogger(__name__).info(msg, extra={...})` convention, e.g. `job_state.py:420`):
     who (`user`), what tag, and which `(source, model, version)` it now points to.
4. Call `flow.stage_ai_model(ai_model)` per cluster via the existing `CoroutineWrangler` — each
   call submits work elsewhere (a remote Slurm job at NERSC, an HTTP call to refserv) and returns
   promptly; the coroutine scheduled on the CTS server never itself transfers AI model bytes,
   mirroring `create_refdata`/`stage_refdata` exactly. This is why this chunk is sequenced after
   Chunks 3 and 4: `stage_ai_model` is the method they add to the NERSC/KBase job flow classes —
   it doesn't exist on those objects yet for any chunk landing before them, though this chunk
   remains independently *testable* earlier via a mocked `flow_manager`. Only reached on the
   not-already-registered path (step 3).

Testable with mocked `flow_manager`/`coro_manager`/mongo, same shape as testing
`Refdata.create_refdata` today.

### Chunk 6 — Lawrencium: no-op, TODO only
Lawrencium is currently disabled (`SubmittableCluster` excludes it, and `LawrenciumJAWSRunner` is
not instantiated or registered with the flow manager — see the reactivation notes in `sites.py`,
`jobflows/jaws_flows_provider.py`, and `app_state.py`). Since `list_usable_clusters()` therefore
never returns it, no LRC-specific AI model code has a live path to run today, so this plan does no
LRC implementation work. Add a `# TODO AI_MODEL` comment in `lawrencium_jaws.py` near the
existing `stage_refdata` override noting that if Lawrencium is reactivated,
`LawrenciumJAWSRunner.stage_ai_model` should be added mirroring whatever mechanism
`stage_refdata`/`nersc_refdata_complete`/`refdata_complete` use for refdata at that time (today
that's the generic JAWS Globus-transfer primitives, parametrized by the AI model's snapshot
subdirectory instead of a refdata id's path) — i.e. keep AI model sync content-agnostic and
consistent with however refdata sync works when LRC comes back, rather than deciding that
mechanism now.

### Chunk 7 — Unify the dynamic mount point, then wire in the AI model store
Since JAWS only ever mounts a single dynamic directory per job, refdata and the AI model store
are inherently mutually exclusive at the *job* level. Unlike refdata, though, whether a given job
uses the AI model store is a job-submission-time fact, not an image-registration-time one: one
image can run some jobs with AI models requested and others without, since the store is one
shared global pool rather than a per-image-registered blob. So there's no
`JobImage.uses_ai_model_store` field at all — that would wrongly bake a per-job choice into the
image. Instead:

- Unlike refdata (exactly one per image, so no `Parameter` is needed to name it — see the
  existing `refdata_mount_point`/`default_refdata_mount_point` fields, which only ever carry a
  mount point, never an identifier) or `input_files`/`MANIFEST_FILE` (one designated `Parameter`
  per job, enforced by `_check_parameter`'s "at most one" rule), a job may need *several
  independent* AI models delivered to different, arbitrary argv/env positions — e.g. separate env
  vars pointing at a cheap/medium/high-reasoning model. So AI models are **not** a `JobInput`-level
  list; each one is named directly by its own `Parameter`, and a job may contain multiple
  `AI_MODEL`-typed `Parameter`s simultaneously. `Parameter` gains two new fields, both required
  (and validated together in `_check_fields`, alongside the existing `input_files_format`/
  `manifest_file_format` requirements) when `type is AI_MODEL`:
  - `ai_model_source: AIModelSource | None` — explicit from day one, deliberately not optional
    with an implicit default, so users are never trained to omit it and then broken when a second
    source is added later (`AIModelSource` has only one value today, `HUGGINGFACE`, but the field
    is still mandatory).
  - `ai_model: str | None` — holds the `model@ref` reference only (`ref` a tag CTS has
    recorded a pointer for — itself always a real ref name from the provider, e.g. an HF branch or
    git tag, never a CTS-invented alias, per Chunk 5 — or a literal resolved version; see Chunk 8's
    naming-format note); `source` is deliberately *not* folded into this string.

  This two-field split mirrors the two-segment GET route shape (`{source}/{model_ref:path}`, Chunk
  10) exactly — same leading `source`, same `model@ref` remainder — so there's one consistent
  split used everywhere a model reference appears, rather than a different convention for
  `Parameter` fields than for routes. `AIModelSource` is a real enum, not a free-text segment a
  user could mistype, which is also why `source` is its own field rather than a string prefix
  needing a custom delimiter.

  Both `ParameterType.AI_MODEL` and `ai_model`/`ai_model_source`'s field descriptions are marked
  experimental/subject-to-change, the same "This is an experimental API and is subject to change
  without notice." language `pipelines/routes.py` uses on its routes (see Chunk 10) — since unlike
  the routes, this isn't route-level and can't use a router tag, it goes directly in the enum
  member's/fields' own description text instead.
- `Parameters._check_parameter`: exempt `ParameterType.AI_MODEL` from the "at most one `Parameter`
  with file input" check, the same way `ParameterType.CONTAINTER_NUMBER` already is (models.py:
  445-446) — any number of `AI_MODEL` `Parameter`s may coexist across `args`/`environment`.
- `Parameters`: add `get_ai_model_parameters(self) -> list[Parameter]`, scanning `args` and
  `environment` and collecting every `AI_MODEL`-typed `Parameter` (as opposed to
  `get_file_parameter()`, which returns at most one) — memoized the same way via a private
  attribute.
- `JobInput`: add `uses_ai_model_store(self) -> bool` returning
  `bool(self.params.get_ai_model_parameters())` — a derived method, not a stored field, mirroring
  how `get_file_parameter()`-derived state is never duplicated into `JobInput`. This is `True`
  whenever at least one `AI_MODEL` `Parameter` is present; the store always mounts as a whole
  once any model is requested, regardless of how many `AI_MODEL` `Parameter`s there are.
- `JobImage`: unchanged — keep `refdata_id: str | None` as-is, no new field. Rename
  `default_refdata_mount_point` → `default_staged_data_mount_point: str | None` (used for whichever
  of the two sources — refdata or the AI model store — ends up mounted for a given job). Both
  directions need to keep working, not just input: a request body or legacy record supplying
  `default_refdata_mount_point` should populate `default_staged_data_mount_point` transparently
  (add a `model_validator(mode="before")`, same mechanism as the existing `_promote_path_string`
  pattern, `models.py:490`, that copies `default_refdata_mount_point` into
  `default_staged_data_mount_point` before the rest of validation runs if the former is present and
  the latter isn't); and existing clients that read `default_refdata_mount_point` out of a response
  must keep seeing it, so it is **not** `exclude=True` — it stays serialized on output, kept in sync
  with `default_staged_data_mount_point` (computed from the same underlying value, the same way
  `download_task_id`/`nersc_download_task_id` remain visible in output indefinitely once deprecated,
  `models.py:1191-1196,1460-1465` — deprecated in this codebase means flagged, not hidden). Both
  fields always reflect the same value; there is only ever one underlying value to keep in sync,
  `default_staged_data_mount_point`'s.
- **`images.py::register`'s existing "mount point implies refdata ID" check must be loosened, not
  just renamed.** Today it rejects registering an image with `default_refdata_mount_point` set but
  no `refdata_id` (`images.py:77-81`) — correct under the old field, which only ever meant "where
  refdata goes." After the rename, `default_staged_data_mount_point` may legitimately be set for an
  image with no `refdata_id` at all, to serve as the default mount point for jobs that will request
  AI models later — per this chunk, whether a given job uses the AI model store is a
  job-submission-time fact, not something `JobImage` tracks, so at registration time there's no way
  to know yet which source a `default_staged_data_mount_point` will end up serving. The check
  becomes: `default_staged_data_mount_point` may be set unconditionally (no `refdata_id` requirement
  at registration time); the absolute-path format check (`images.py:82-85`) stays, since that's
  orthogonal to which source ends up using the mount point.
- `Parameters`: rename `refdata_mount_point` → `staged_data_mount_point: str | None` (note: unlike
  the image field, there is currently no `default_` prefix on this one — confirmed in `models.py`,
  it's bare `refdata_mount_point` — so the new name doesn't gain one either, for consistency with
  the existing naming choice at this level), with the same two-way treatment as
  `JobImage.default_staged_data_mount_point` above — `refdata_mount_point` accepted on input via the
  same `model_validator(mode="before")` pattern, and still serialized on output, kept in sync with
  `staged_data_mount_point` rather than hidden.
- `models.py`: rename `Job.get_refdata_mount_point()` → `Job.get_staged_data_mount_point()`,
  delegating to `resolve_staged_data_mount_point` (below) rather than inlining the
  `params.staged_data_mount_point or image.default_staged_data_mount_point` fallback itself; update
  its two call sites (`jaws/wdl.py`, `externalexecution/executor.py`) accordingly. No separate
  `get_ai_model_mount_point()` is needed — there's only ever one mount point active per job,
  regardless of source.
- **The AI model store gets a default mount point; reference data does not.** Since a job
  requesting AI models always delivers each model's filesystem path to the container via its own
  `Parameter` (Chunk 9), the user genuinely doesn't care where the store itself is mounted — unlike
  reference data, which has no equivalent per-reference `Parameter` to point at, so the container
  entrypoint has to already know, by convention, where to find it. New module-level
  `models.DEFAULT_AI_MODEL_MOUNT_POINT = "/ai_models"` and
  `models.resolve_staged_data_mount_point(params: Parameters, default_mount_point: str | None) ->
  str | None`: resolves to `params.staged_data_mount_point or default_mount_point` as before, but
  if that's still unset and `params.get_ai_model_parameters()` is non-empty, falls back to
  `DEFAULT_AI_MODEL_MOUNT_POINT` rather than leaving the mount point unset. There is no equivalent
  fallback for reference data. `JobPreview.get_staged_data_mount_point()` (and every other caller
  that previously inlined the `params.staged_data_mount_point or
  image.default_staged_data_mount_point` fallback) now delegates to this shared function, so the
  default applies everywhere a job's effective mount point is read, not just at submission-time
  validation.
- `job_state.py::_check_refdata`: adds a validator rejecting a job where both `image.refdata_id`
  and `job_input.uses_ai_model_store()` are truthy. This check belongs at job-submission time, not
  image-registration time: whether a given job uses the AI model store is a per-job-submission fact
  (an image with `refdata_id` set can still run jobs that don't use AI models), so the actual
  conflict — a job trying to use both a regular refdata mount and the AI model store at once — can
  only be detected once the job input is known. The existing "requires a mount point" validation
  still applies to the reference-data case (via `resolve_staged_data_mount_point` returning `None`),
  but can no longer fire for the AI-model case, since that case always resolves to a mount point.
  `_check_refdata` also rejects a resolved mount point that collides with the job's
  `input_mount_point`/`output_mount_point` — a real risk once `DEFAULT_AI_MODEL_MOUNT_POINT` can be
  silently assigned without the user having chosen it.
- `jaws/wdl.py`: no new parameter needed. `generate_wdl`/`_generate_wdl` already take
  `relative_refdata_path` as an opaque, caller-computed `Path`; `nersc_jaws.py` (Chunk 3) just
  passes the AI model store's constant relative path instead of a refdata path when
  `job.job_input.uses_ai_model_store()`, and the existing `dynamic_refdata: "path:mount"` WDL
  runtime line is emitted unchanged — Cromwell only sees a mount, not which source it came from,
  so no JAWS-side config change or new custom runtime attribute is needed at all.
- `executor.py`: extend `_build_mounts`'s existing `if job.image.refdata_id:` branch with an
  `elif job.job_input.uses_ai_model_store():` mounting
  `Path(self._cfg.refdata_host_path) / "_ai_models"` (Chunk 4) instead of `.../ {refdata_id}`, both
  writing to the same `job.get_staged_data_mount_point()` target.

Pure unit tests on WDL template output strings and mount dicts — no cluster dependency. No JAWS
verification step needed (see above).

### Chunk 8 — Job-submission-time AI model resolution + validation
**Reference naming format.** HuggingFace itself has no combined `repo@revision` string — the
`huggingface_hub` API and HF's own URLs always keep repo and revision as separate fields/path
segments, and HF's `revision` kwarg is resolved identically whether it's a branch, a git tag, or a
commit hash (Chunk 0's `resolve_revision` already reflects this: one `revision` argument, no
branch/tag/hash distinction). So there's no HF-native format to mirror, and unlike Docker, HF gives
us no syntax (`:` vs `@`) to tell a mutable ref from an immutable version apart. The reference
format is therefore a single separator, `model@ref` (`@` chosen as a common general convention for
"thing at this revision" — e.g. Go modules' `module@version`, Package URL's `pkg:type/name@version`
— not a claim of HF or Docker syntax; `model`/`ref` are the source-generic CTS field names from the
top-level naming note, not HF's own `repo_id`/`revision` — see the `ref` vs. `version` naming note
near the top of this section). The part after `@` uses `ref`, not `version`, for exactly the same
reason registration does (Chunk 5): it's unresolved, caller-supplied input that may be either a tag
CTS has recorded a pointer for (Chunk 1/5's `ai_model_tags` — the tag itself is always the real
branch/tag name the admin passed as `ref` at registration, never a CTS-invented alias) or a literal
resolved version (for `HUGGINGFACE`, a commit hash); `version` is reserved exclusively for the
already-resolved hash (`AIModel.version`, Chunk 1), never for this pre-resolution string.

Since nothing in the string distinguishes the two, resolution tries **version first, tag second**
— this is exactly `mongo.py`'s `get_ai_model(source, model, ref, resolve_tag=True)` from
Chunk 1: look up `(source, model, version)` directly against `ai_models`, treating the given
`ref` as a literal version, first; only on a miss, fall back to the `ai_model_tags` lookup.
Version-first (not tag-first) specifically closes off a registration-time trick where someone
registers a tag string that happens to collide with a legitimate version value, which would
otherwise let that tag silently shadow the real snapshot for every job referencing the version
directly.

- `models.py`: add `ParameterType.AI_MODEL` and the `Parameter.ai_model_source`/`Parameter.ai_model`
  fields (Chunk 7), both required when `type is AI_MODEL`, with `ai_model` validated against the
  `model@ref` format above. `ai_model_source` is required explicitly rather than assumed —
  even though `AIModelSource` has only one value today, requiring it up front means users are never
  trained on an implicit single-source default that would break when a second source is added.
- In `job_state.py`'s submission validation (alongside `_check_and_update_files`/
  `_check_and_update_script`, which already rewrite `input_files`/`script` with service-resolved
  values post-validation — e.g. `S3FileWithDataID.model_construct(...)` at job_state.py:277-279):
  new `_check_and_update_ai_models`, called once per `submit()` after `_check_refdata`, for every
  `Parameter` returned by `job_input.params.get_ai_model_parameters()`. For each one, parse
  `ai_model` into `(model, ref)` via `models.split_ai_model_ref` and call
  `mongo.get_ai_model(p.ai_model_source, model, ref, resolve_tag=True)` (Chunk 1), confirm the
  resulting `AIModel`'s status for the job's target cluster is `COMPLETE` — fail fast otherwise with
  the existing `InvalidAIModelStateError` (Chunk 2), the same way `_check_refdata` already uses the
  existing `InvalidReferenceDataStateError` for the equivalent refdata check; a lookup miss (no such
  model/tag at all) is likewise reported by the existing `NoSuchAIModelError` (Chunk 1) raised
  inside `get_ai_model` itself, so no new exception type is needed for either case. The matched
  `Parameter` is then rewritten with `ai_model` set to the resolved `model@<version>` form (not the
  original tag, if a tag was given — the version is what determines the on-disk directory name, per
  Chunk 1/3/4's `(source, model, version)` layout; `ai_model_source` itself needs no rewriting, since
  `AIModelSource` values are already the resolved, canonical form), the same "validate once at
  submission, persist the resolved form" pattern already used for `input_files`/`script`. No
  separate `JobInput` field is needed to hold resolved values — the `Parameter`s themselves, as
  already persisted on the job record, are the single source of truth.

  Both the rewritten `Parameter` and the `Parameters` object containing it are rebuilt via
  `model_construct` from the original object's own field values (`type(p).model_construct(**{
  **vars(p), "ai_model": resolved})`, mirroring the existing `JobInput.model_construct(**{**vars(
  job_input), ...})` pattern at `routes.py::submit_job`) rather than `model_copy`. This isn't just
  style: `Parameters.get_ai_model_parameters()`/`get_file_parameter()` memoize their result as a
  plain instance attribute (not a declared field) on the `Parameters` object the first time either
  is called — which already happens earlier in `submit()`, via `_check_refdata`'s
  `job_input.uses_ai_model_store()` call — so `job_input.params` already carries a cache keyed to
  the *original*, unresolved `Parameter`s by the time `_check_and_update_ai_models` runs.
  `model_copy` would carry that stale cache forward onto the rewritten `Parameters` object (it
  shallow-copies the whole instance, private attributes included); `model_construct` only reads
  declared fields out of the supplied mapping, silently dropping the extra cache key, so the
  returned `Parameters` has no memoized state and a later `get_ai_model_parameters()` call against
  it (e.g. Chunk 9's rendering) correctly recomputes against the resolved `Parameter`s instead of
  returning the stale, pre-resolution list.

Testable in isolation with mocked Mongo/registry lookups, following existing `job_state.py` test
conventions.

### Chunk 9 — `ParameterType.AI_MODEL` delivery
- `argument_generator.py::_process_parameter`: new case rendering a single `AI_MODEL` `Parameter`
  — the one passed into this call, already holding `ai_model_source` and a resolved `model@version`
  `ai_model` reference after Chunk 8 — into its one arg/env slot as
  `<staged_data_mount_point>/<ai_model_source>/<model>/<version>/` (using the shared
  `job.get_staged_data_mount_point()` from Chunk 7; the path order matches the on-disk
  `<source>/<model>/<version>` layout from Chunks
  1, 3–4). No manifest file is needed here, unlike
  `MANIFEST_FILE`: each `AI_MODEL`
  `Parameter` already pins down exactly one model and one destination, so there's nothing to
  aggregate across files/containers the way `_join_files`/`_handle_manifest` do. A job with several
  `AI_MODEL` `Parameter`s (e.g. one env var per reasoning tier) simply has `_process_parameter`
  invoked once per `Parameter`, as already happens today for every `args`/`environment` entry.
- Container-side contract: `from_pretrained(path, revision=None)` — no HF Hub network/cache
  lookup involved, since the path already points at the correct snapshot directory.

Pure unit tests on rendered arguments/manifest content, extending existing
`argument_generator` tests.

`# TODO AI_MODEL` `externalexecution/executor_test.py` has no coverage of the AI model store mount
(`_build_mounts`'s `elif job.job_input.uses_ai_model_store()` branch, Chunk 7) through the public
`execute()` entry point, since until this chunk lands, `ArgumentGenerator` has no case for
`ParameterType.AI_MODEL` — including an `ai_model` `Parameter` in `job_input.params.environment`
makes `execute()` raise before mount-building is ever reached. Add that coverage once this chunk
gives `ArgumentGenerator` a case for `ParameterType.AI_MODEL`, so the test can go through `execute()`
like the existing refdata mount tests do, rather than calling the private `_build_mounts` directly.

### Chunk 10 — Admin + public routes
Since AI models have no opaque id (Chunk 1), the single-item GET routes can't take the refdata
shape (`GET /refdata/{refdata_id}`). Images establish the precedent to follow instead: `GET
/images/{image_id:path}` (`routes.py:508`) takes the *entire* `name:tag@digest` reference as one
combined path segment, parsed service-side (`image_remote_lookup.parse_image_name`) — not a path
param plus a query param for the `ref` part. The public and admin single-item GETs mirror that
shape, taking the full `model@ref` reference (Chunk 8's format) as one path segment.

**No separate AI-model admin router.** Refdata's 4 admin routes
(`routes.py:670,986,1004,1026` — register, admin-view GET, recover, clean) all live on the single
shared `ROUTER_ADMIN` (`prefix="/admin"`), not a dedicated `ROUTER_ADMIN_REFDATA`.
AI models get the same 4-route
shape, added to that same `ROUTER_ADMIN`, placed directly after the existing refdata admin block
(`routes.py:670-1050`) in file order:

- `POST /admin/ai-models/{source}/{model_ref:path}` (admin-gated via existing `_ensure_admin`, no
  request body) → `AIModelRegistry.register_ai_model`, where `model_ref` is the combined
  `model@ref` string — the same format and the same single greedy `:path` capture as the GET
  routes' `model_ref` (below), parsed service-side by splitting on the last `@`. `ref` is
  folded into this one path segment rather than taken separately (as a second path segment or a
  request body) because `model` and `ref` are two independent values with no natural
  delimiter between them if kept apart — `model` can itself contain `/` (e.g. `org/model`) and
  `ref` can too (it's raw, unresolved admin input, and per Chunk 5 must be a real provider
  ref, which git permits to contain `/`), so two separate path segments would be ambiguous (the
  same greedy-capture problem already identified for `version` after `{model:path}` elsewhere in
  this plan, Chunk 3's note). The `@` separator sidesteps that the same way it already does for
  `model_ref` on the GET routes: one `:path` capture, split by the service, not by the router.
  No `tag` field anywhere in this route — per Chunk 5, any tag is derived from `ref` itself,
  never supplied independently.
- `GET /admin/ai-models/{source}/{model_ref:path}` (as-admin view, returns `AdminAIModel` with
  full status/error detail), where `model_ref` is the full `model@ref` string — resolved via
  `get_ai_model(source, model, ref, resolve_tag=True, populate_tags=True)` (Chunk 1;
  the same `resolve_tag` resolution submission-time validation uses, Chunk 8, but with
  `populate_tags=True` since this route surfaces `tags` to the caller). Mirrors
  `GET /admin/refdata/{refdata_id}` (`routes.py:986`).
- `PUT /admin/ai-models/{source}/{model:path}/{version}/{cluster}/recover` — recovers a stalled
  per-cluster staging record, mirroring `PUT /admin/refdata/{refdata_id}/{cluster}/recover`
  (`routes.py:1004`) via a new `AIModelRegistry.recover_ai_model(key: AIModelKey, cluster)` (shaped
  like `Refdata.recover_refdata`; the route handler builds `key` from the three path segments).
  Takes the already-resolved `(model, version)`, not a `model_ref` string, since recovery targets
  one exact registered snapshot, not an ambiguous/resolvable reference — no `resolve_tag` needed
  here, consistent with `get_ai_model`'s default (Chunk 1).
- `DELETE /admin/ai-models/{source}/{model:path}/{version}/{cluster}/clean` — cleans up staging
  intermediate files for a cluster, mirroring `DELETE /admin/refdata/{refdata_id}/{cluster}/clean`
  (`routes.py:1026`) via a new `flow_cleaner.clean_ai_model(ai_model, cluster, user, force=...)`
  (mirroring `flow_cleaner.clean_refdata`), which in turn calls a new
  `NERSCManager.clean_ai_model(key: AIModelKey)` (parallel to `NERSCManager.clean_refdata`,
  `nersc/manager.py:1409`).

  **Unlike `clean_refdata`, this must also prune now-empty ancestor directories.** Refdata's
  `_get_refdata_scratch(refdata_id)` is a single flat segment directly under the scratch root, so
  deleting it leaves nothing behind. An AI model's scratch path
  (`_get_ai_model_scratch(key)`, Chunk 3) nests three levels — `<source>/<model>/<version>/` — and
  `<model>` can itself contain further path segments (e.g. `org/name`) shared by every other
  version ever staged for that model, so deleting just the leaf `<version>` directory the way
  `clean_refdata` deletes its leaf would leave `<source>/<model>/` (and any intermediate
  `org/`-style segments) sitting empty afterward. Recursively deleting those parents outright is
  not safe either: a sibling version of the same model could be mid-staging in a sibling
  `<version>` directory at the same time, so naively `rm -rf`-ing `<model>` on one version's
  cleanup could destroy another version's in-progress scratch content.

  The fix is a new `_delete_path_and_prune_empty_ancestors(self, path: Path, stop_at: Path)`
  method on `NERSCManager`, sibling to `_delete_paths` (`nersc/manager.py:524`) but distinct from
  it rather than a parameter added to it, since pruning is AI-model-specific and every other
  caller of `_delete_paths` (`clean_job`, `clean_refdata`) has nothing to prune. It extends
  `_delete_paths`'s existing single-shell-snippet-per-path approach (one `perlmutter.run()` call,
  no Python-side sequencing) with a new `_REMOVE_PATH_AND_PRUNE_TEMPLATE` sibling to
  `_REMOVE_PATH_TEMPLATE` (`nersc/manager.py:224`): after the existing `rm -rf -- "{path}"`, the
  script walks upward via repeated `dirname`, calling `rmdir` on each ancestor and stopping at the
  first failure (a `rmdir` on a non-empty directory fails harmlessly, so this naturally stops
  exactly when a sibling version/model's content is still present) or at `{stop_at}` (exclusive),
  whichever comes first. `clean_ai_model` calls this with `path=self._get_ai_model_scratch(key)`
  and `stop_at` equal to the AI-model scratch root one level above the `<source>/<model>/<version>`
  nesting (pulled out as its own `_get_ai_model_scratch_root()` helper, with
  `_get_ai_model_scratch(key)` becoming `self._get_ai_model_scratch_root() / key.source.value /
  key.model / key.version`, mirroring how `_get_relative_ai_model_loc` already builds on
  `self._ai_model_root_loc`), so pruning never walks up past the point where AI-model-specific
  nesting begins.

  This only addresses the Perlmutter-scratch side. The DTN-root final store
  (`_get_ai_model_loc`/`_get_relative_ai_model_loc`) is never touched by `clean_ai_model` at all,
  matching `clean_refdata`'s own scope (it also only ever deletes scratch, never
  `_get_refdata_loc`) — store garbage collection/retention for the ever-growing final store
  remains explicitly out of scope for this plan (see "Explicitly deferred"), so empty
  `<source>/<model>/` directories left behind there after every version of a model is eventually
  retired are an accepted consequence of that deferral, not a gap this chunk fixes.

All four admin routes above use `(source, model, version)` directly rather than the combined
`model_ref:path` form, except the admin GET — consistent with the registry/`update_ai_model_state`
precedent in Chunks 2–5 of using `resolve_tag=False`-style exact triples wherever the operation
targets one already-known snapshot, reserving `model_ref`/tag resolution for cases where the
caller only has a possibly-ambiguous user-supplied string (the two GETs).

**The public (non-admin) routes get their own new router, unlike the admin routes.** New
`ROUTER_AI_MODELS = APIRouter(tags=["AI Models - Experimental"], prefix="/ai-models")` in
`routes.py`, mirroring `ROUTER_REFDATA = APIRouter(tags=["Reference Data"], prefix="/refdata")` —
the router-level tag is both this router's OpenAPI grouping label and (via the `- Experimental`
suffix) its experimental marking, the same mechanism `pipelines/routes.py`'s
`ROUTER_PIPELINES`/`ROUTER_ADMIN_PIPELINES` already use. In `app.py`, `app.include_router(
routes.ROUTER_AI_MODELS)` is added **directly after** `app.include_router(routes.ROUTER_IMAGES)`
and before `app.include_router(routes.ROUTER_REFDATA)` — `include_router` call order is what
determines section order in the OpenAPI UI, so this places the new "AI Models" section between
"Images" and "Reference Data", keeping the two related, mutually-exclusive-mount concepts adjacent in the
docs:
- `GET /ai-models` (mirrors `GET /refdata`, `routes.py:552`) for catalog browsing — this one stays
  public since the catalog itself (which models exist, their staging status) isn't sensitive,
  matching why refdata's own list/get-by-id are public while only registration/recovery/cleanup are
  admin-gated.
- `GET /ai-models/{source}/{model_ref:path}` (mirrors `GET /refdata/{refdata_id}`, `routes.py:548`),
  same resolution as the admin GET above but returning the public `AIModel` (no `AdminAIModel`-only
  fields).

The admin routes, by contrast, get **no** dedicated router — per the "No separate AI-model admin
router" note above, they land on the existing shared `ROUTER_ADMIN`, the same way refdata's own
admin routes do, not a new `ROUTER_ADMIN_AI_MODELS`. `ROUTER_ADMIN` is shared with every other
admin-gated feature in the service and can't carry an AI-model-specific tag, so the four admin
routes rely on per-route marking alone for their experimental disclosure, where `ROUTER_AI_MODELS`'s
tag gives the two public routes a second, router-level signal on top of the same per-route marking
— but **all six routes get the identical per-route treatment regardless of which router they're
on**: each route's `description` gets the same boilerplate sentence `pipelines/routes.py` repeats
on each of its routes, `"This is an experimental API and is subject to change without notice."`
The router tag is additive documentation-grouping sugar for the two public routes, not a substitute
for the per-route sentence — nothing about being on `ROUTER_AI_MODELS` instead of `ROUTER_ADMIN`
exempts a route from carrying its own explicit disclosure text. This is a "subject to change"
disclosure, not a feature flag — all six routes above are otherwise fully implemented and usable,
matching how the pipelines routes are already marked experimental while live.

Thin routing layer; testable via existing FastAPI TestClient + mocked-service-layer route test
conventions.

---

## Verification

- Each chunk ships with its own tests following the existing conventions
  (`create_autospec(..., spec_set=True, instance=True)`, `model_construct()` for output-only
  Pydantic fixtures, `assert_called_once_with` with full args, injectable timestamp/UUID
  functions) and should be run via `PYTHONPATH=. uv run pytest test`.
- End-to-end verification (post Chunk 10) needs a real registered AI model staged to at least one
  live cluster and a job that declares it, run against `docker-compose-local.yaml` for the
  KBase/HTCondor path; the NERSC/JAWS path requires the existing manual test setup
  (`test_manual/`) with live credentials, same as today's refdata manual tests. Lawrencium is out
  of scope (see Chunk 6) and has nothing to verify.
- Confirm the container-side contract works with a trivial job: image running `transformers`, an
  `AI_MODEL`-typed `Parameter` with `ai_model_source: "HUGGINGFACE"`, `ai_model:
  "org/small-test-model@main"`, and a script that
  reads the delivered path and calls `from_pretrained(path)`.

## Explicitly deferred

- Store garbage collection / retention policy for the ever-growing AI model store.
- Any AI model source other than HuggingFace. The **schema** doesn't block a second source —
  `model`/`version` are named generically (see the top-level naming note) specifically so they can
  hold whatever identifier shape a different source uses, including a non-git one like Civitai's
  numeric model/version IDs, with no field changes. What's deferred is the **resolution/staging
  mechanism**: `resolve_revision`, the version-first/tag-second lookup order, and
  `huggingface_client.py`'s `stage_snapshot` are all built around HF's git-based ref model (resolve
  a mutable ref to an immutable commit hash), which a git-shaped second source like ModelScope
  (whose API mirrors `huggingface_hub`'s `repo_id`/`revision` shape closely) could plausibly reuse
  via a new `AIModelSource` member and a new thin client module parallel to
  `huggingface_client.py` — but a non-git source like Civitai has no ref/tag/hash distinction to
  resolve at all, so none of that mechanism applies, and it would need its own resolution/staging
  design built from scratch, not just a new enum value and client module. For that case, the
  expected answer today is for the user to mirror the model to HuggingFace rather than CTS adding a
  bespoke adapter.
- Lawrencium AI model staging — LRC is currently disabled cluster-wide; Chunk 6 leaves only a
  TODO for if/when it's reactivated.
- **Corruption-at-rest detection via a stored checksum manifest.** KBase's existing refdata path
  writes a checksum manifest at unpack time specifically because zip contents carry no checksums of
  their own — the manifest is the only way to later notice silent on-disk corruption/truncation,
  independent of how the file got there. HF's `HfApi().model_info(repo_id, revision=...)` response
  includes a `siblings` list with a per-file hash (SHA256 for LFS-tracked files, which most model
  weights are), which is the data an equivalent AI-model manifest would need. That call already
  happens once per registration on the **CTS server** (`resolve_revision`, Chunk 0) — but that's the
  wrong place to source manifest data from: CTS resolving the hash centrally (so every cluster
  stages the same immutable commit) is a separate concern from writing a per-file manifest next to
  staged content, which happens on **refserv**/NERSC, not on CTS. Reusing CTS's `model_info` result
  would mean threading `siblings` data through the staging request as a new field, an unnecessary
  cross-service dependency; the straightforward approach is for refserv/NERSC staging to make its
  own `model_info` call at staging time (a second, independent fetch, not a shared one) when it
  needs manifest data, the same way it presumably doesn't rely on CTS for anything else beyond
  `(source, model, version)` today. Out of scope for this plan; `resolve_revision` as specced here
  only returns the resolved commit hash, not `siblings`' per-file data, so adding this would mean a
  new HF call and a new manifest-writing step in staging itself (Chunks 3/4), not just reusing an
  existing call site. If this
  gets built for AI models, the same gap exists on the NERSC refdata path today (the equivalent
  manifest step was never added there, not for a design reason, just time) and should get the same
  treatment at that point, rather than leaving regular refdata less defended against corruption-at-
  rest than the AI model store it shares infrastructure with.
