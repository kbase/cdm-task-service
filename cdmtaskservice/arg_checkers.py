'''
Contains common code for checking method arguments
'''
from typing import Any


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


def check_int(num: int, name: str, minimum: int = 1):
    """
    Check an integer argument is not None and is greater than some value.
    
    num - the number to check. Must None or an integer.
    name - the name of the argument to use in exceptions.
    minimum - the minimum allowed value of the integer.
    
    returns the integer.
    """
    # Converted this to float as well but realized we just want ints in all the current use
    # cases. Leave it as it for now. Not typechecking since we assume the programmer is reading
    # the type hints
    if num is None:
        raise ValueError(f"{name} is required")
    if num < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return num
