"""
Methods for dealing with manifest files for jobs.
"""

import os

from cdmtaskservice import models
from cdmtaskservice.arg_checkers import not_falsy as _not_falsy
from cdmtaskservice.input_file_locations import determine_file_locations


def generate_manifest_files(job: models.Job) -> list[str]:
    """
    Generate a set of manifest files as strings for a job, or return an empty list if no
    manifest files are specified.
    """
    manifests = []
    mani_spec = _not_falsy(job, "job").job_input.params.get_file_parameter()
    if not mani_spec or mani_spec.type is not models.ParameterType.MANIFEST_FILE:
        return manifests
    file_to_rel_path = determine_file_locations(job.job_input)
    for files in job.job_input.get_files_per_container().files:
        manifest = ""
        if mani_spec.manifest_file_header:
            manifest = f"{mani_spec.manifest_file_header}\n"
        for f in files:
            match mani_spec.manifest_file_format:
                case models.ManifestFileFormat.DATA_IDS:
                    manifest += f"{f.data_id}\n"
                case models.ManifestFileFormat.FILES:
                    f = os.path.join(
                        job.job_input.params.input_mount_point, file_to_rel_path[f]
                    )
                    manifest += f"{f}\n"
                case _:
                    # Can't currently happen but for future safety
                    raise ValueError(
                        f"Unknown manifest file format: {manifest.manifest_file_format}"
                    )
        manifests.append(manifest)
    return manifests
