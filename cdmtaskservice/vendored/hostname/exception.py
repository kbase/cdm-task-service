# This is intended to wrap dns.exception

import dns.exception
import dns.name


class HostnameException(dns.exception.DNSException):
    """A generic exception abstraction"""

    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)  # type: ignore[no-untyped-call]


class UnderscoreError(HostnameException):
    """An underscore appeared in a label it shouldn't have."""


class NotASCIIError(HostnameException):
    """Non-ASCII when ALLOW_IDNA is not set"""


class NotAStringError(HostnameException):
    """Must be a string"""


class BadCharacterError(HostnameException):
    """A forbidden character was found in a label"""


class DigitOnlyError(HostnameException):
    """The rightmost label contains only digits"""


class NoLabelError(HostnameException):
    """Hostnames must have at least 1 label"""


class BadHyphenError(HostnameException):
    """A hyphen is used at the beginning or end of a label"""


# Looking at how dnspython handles INDA exceptions and doing
# that here wrt to DNS errors
class DomainNameException(HostnameException):
    """DNS Parsing raised an exception"""

    supp_kwargs = {"dns_exception"}
    fmt = "DNS syntax error: {dns_exception}"

    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)


# lifted from dnspython.dns.exception
class INDAException(HostnameException):
    """IDNA processing raised an exception."""

    supp_kwargs = {"idna_exception"}
    fmt = "IDNA processing exception: {idna_exception}"

    # We do this as otherwise mypy complains about unexpected keyword argument
    # idna_exception
    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)
