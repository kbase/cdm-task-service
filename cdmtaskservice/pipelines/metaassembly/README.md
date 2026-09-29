# Metagenome Assembly

The Metagenome Assembly (`jgi_metaAssembly`) pipeline:
https://github.com/kbaseincubator/metaAssembly

One module per version lives under this package, e.g. `v0_1_0.py`.

This version only supports the short-read (Illumina) path (`shortRead=true`); the long-read
(PacBio/Flye) path is not exposed.

## Why `threads` is hardcoded to `"16"` and `memory` is left unset

The workflow's own `memory`/`threads` inputs (`String?`, no default) only reach a handful of
BBTools calls in the short-read path, and even there their effect is limited - full detail is in
the upstream repo's `memory_and_threads.md` and `bbtools_shortreadss_instances.md` (as of commit
`6aad3c9`, the version this pipeline definition is pinned to):

* **`memory`**: every task that consumes it only uses it to set a JVM `-Xmx` heap flag
  (`bbcms`, `create_agp`, `read_mapping_pairs`), defaulting to `-Xmx105G` if unset. Each of those
  tasks' actual container resource reservation (`runtime.memory`) is separately **hardcoded to
  `120 GiB`** regardless of the input, so the default heap already sits safely under the real
  ceiling. The one task with real dynamic memory sizing, `assy` (metaSPAdes), ignores the
  `memory` input entirely - its resources come from `predict_memory`, which fits a curve to the
  k-mer count. Setting `memory` therefore changes nothing that matters: raising it risks
  exceeding the 120 GiB container cap, lowering it has no upside. **Leave it unset.**

* **`threads`**: most BBTools calls in the short-read path (`bbcms`, `create_agp`, `stage`) don't
  even expose a `threads` parameter - concurrency there is whatever BBTools decides on its own
  (`threads=auto`, i.e. all cores the JVM can see, or a hardcoded literal that matches the task's
  `runtime.cpu`). The one task that *does* thread the workflow's `threads` input through,
  `read_mapping_pairs` (bbmap.sh coverage mapping), falls back to
  `select_first([threads, system_cpu])` if `threads` is unset, where `system_cpu` is the full
  core count visible via `/proc/cpuinfo` - **not** the `16` CPUs actually reserved for that task
  via `runtime.cpu: 16`. On a shared NERSC node, an unset `threads` therefore lets that task
  oversubscribe the node, spawning far more BBTools/`samtools sort` threads than were scheduled
  for it. Passing `threads="16"` closes that gap by matching the task's real CPU reservation.
  It does not, and cannot, control `assy` (metaSPAdes), which always gets its thread count from
  `predict_memory`'s k-mer based tier (16 or 32 depending on estimated complexity) rather than the
  workflow's `threads` input - that's a related but separate oversubscription risk `threads` alone
  does not close, since `assy`'s CPU request already matches what it asks for.

**Conclusion**: `threads` must be set to `"16"` to close the one real oversubscription risk in
`read_mapping_pairs`; `memory` is vestigial in the short-read path and is left unset. Both are
handled internally by this pipeline version (`v0_1_0.py`) - callers only supply `read_mode`,
`output_prefix`, and their input file(s).

## Inputs exposed via the CTS API

Per the "Implication for an API that only supports short reads" section of the upstream
`memory_and_threads.md`, a wrapping API only needs to accept the project name and input files -
everything else (`shortRead`, `threads`) is fixed or hardcoded by this pipeline version. This
version additionally exposes `read_mode` (mirroring the ReadsQC pipeline's convention) so callers
can supply either a single interleaved file or a forward/reverse pair without having to
interleave the pair themselves first.

The short-read WDL path (via `make_interleaved_reads.wdl`) only safely supports exactly one
interleaved file, or exactly one forward + one reverse file interleaved positionally
(`input_files[0]`/`input_files[1]`) - additional files are silently ignored by the upstream WDL.
This version therefore exposes `input_file`/`input_file2` as single files rather than lists,
so that footgun can't be exposed through the API.

All other workflow inputs (container images, tool parameters) have workflow-level defaults and
are irrelevant once `shortRead=true`, since the long-read branch never executes.
