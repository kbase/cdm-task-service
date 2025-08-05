"""
A data class for paths used by the NERSC manager.

"""

from dataclasses import dataclass, field
from pathlib import Path

from cdmtaskservice.arg_checkers import require_string as _require_string


def _validate_and_convert_path(value: str, field_name: str) -> Path:
    path = Path(_require_string(value, field_name))
    if not path.is_absolute():
        raise ValueError(f"{field_name} must be an absolute path: got '{value}'")
    return path.resolve()


@dataclass(frozen=True)
class NERSCPaths:
    """ The set of configurable paths upon which the NERSC manager operates. """
    
    code_path: Path = field(init=False)
    """The path in which to store remote code at NERSC. It is advised to
    include version information in the path to avoid code conflicts.
    """

    jaws_refdata_root_dir: Path = field(init=False)
    """The root directory for reference data configured for the JAWS 'kbase` site."""

    jaws_staging_dir_dtns: Path = field(init=False)
    """The JAWS staging directory for the `kbase` site on a NERSC DTN."""

    jaws_staging_dir_perlmutter: Path = field(init=False)
    """The JAWS staging directory for the `kbase` site on the NERSC Perlmutter system."""

    def __init__(
        self,
        code_path: str,
        jaws_refdata_root_dir: str,
        jaws_staging_dir_dtns: str,
        jaws_staging_dir_perlmutter: str
    ):
        fields = {
            'code_path': code_path,
            'jaws_refdata_root_dir': jaws_refdata_root_dir,
            'jaws_staging_dir_dtns': jaws_staging_dir_dtns,
            'jaws_staging_dir_perlmutter': jaws_staging_dir_perlmutter,
        }
        for name, value in fields.items():
            path = _validate_and_convert_path(value, name)
            object.__setattr__(self, name, path)  # bypass frozen
