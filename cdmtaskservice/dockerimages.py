"""
Contains code for querying container information without access to a docker server
"""

from pathlib import Path

class DockerImageInfo:
    """
    Provides information about a remote docker image
    """
    
    def __init__(self, crane_absolute_path: Path):
        """
        Initialize the image info provider.
        
        crane_absolute_path - the absolute path to the crane executable.
        """
        # Require an absolute path to avoid various malicious attacks
        # TODO check is absolute path
        # TODO run crane sync and check version, store path

    def get_digest_from_name(self, image_name: str):
        """
        Get an image digest given the image name.
        """
        # TODO validate container_name
        # TODO run crane asynchronously - watch out for shell injection
        # TODO throw an exception if something goes wrong
        return "sha256:fake"

