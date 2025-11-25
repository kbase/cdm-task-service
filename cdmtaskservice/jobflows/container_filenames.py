"""
Get the stdout and std err filenames for a container that should be used in S3.
"""
# Note - this file is part of the set of files pushed to NERSC. Keep the imports minimal.


def get_filenames_for_container(container_num: int) -> tuple[str, str]:
    """
    Given a container number, get the stdout and stderr filenames in that order as a tuple.
    """
    if container_num < 0:
        raise ValueError("container_num must be >= 0")
    return (
        f"container-{container_num}-stdout.txt",
        f"container-{container_num}-stderr.txt",
    )
