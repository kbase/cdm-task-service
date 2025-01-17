'''
Contains common code for checking method arguments
'''
from typing import Any
import unicodedata


def not_falsy(obj: Any, name: str):
    """
    Check an argument is not falsy.
    
    obj - the argument to check.
    name - the name of the argument to use in exceptions.
    
    returns the object.
    """
    if not obj:
        raise ValueError(f"{name} is required")
    return obj


def require_string(string: str, name: str):
    """
    Check an argument is a non-whitespace only string.
    
    string - the string to check. Must be either falsy or a string.
    name - the name of the argument to use in exceptions.
    
    returns the stripped string.
    """
    if not string or not string.strip():
        raise ValueError(f"{name} is required")
    return string.strip()


def check_num(num: int | float, name: str, minimum: int | float = 1) -> int | float:
    """
    Check an integer or float argument is not None and is greater than some value.
    
    num - the number to check. Must None, a float, or an integer.
    name - the name of the argument to use in exceptions.
    minimum - the minimum allowed value of the number.
    
    returns the number.
    """
    if num is None:
        raise ValueError(f"{name} is required")
    if num < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return num


def contains_control_characters(string: str, allowed_chars: list[str] = None) -> int:
    '''
    Check if a string contains control characters, as denoted by the Unicode character category
    starting with a C.
    string - the string to check.
    allowed_chars - a list of control characters that will be ignored.
    returns -1 if no characters are control characters not in allowed_chars or the position
        of the first control character found otherwise.
    '''
    # See https://stackoverflow.com/questions/4324790/removing-control-characters-from-a-string-in-python  # noqa: E501
    allowed_chars = allowed_chars if allowed_chars else []
    for i, c in enumerate(string):
        if unicodedata.category(c)[0] == 'C' and c not in allowed_chars:
                return i
    return -1
