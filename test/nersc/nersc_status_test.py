# TODO TEST how to test this? Mock a nersc server?

import datetime

from cdmtaskservice.nersc.status import Status


def test_systems_available():
    dt1 = datetime.datetime(1984, 1, 26)
    dt2 = datetime.datetime(1984, 1, 27)
    _test_systems_available(None, None, None)
    _test_systems_available(dt1, None, dt1)
    _test_systems_available(None, dt2, dt2)
    _test_systems_available(dt1, dt2, dt2)
    _test_systems_available(dt2, dt1, dt2)


def _test_systems_available(
    perl: datetime.datetime, dtns: datetime.datetime, expected: datetime.datetime
):
    s = Status(
        ok=True,
        perlmutter_up=True,
        dtns_up=True,
        perlmutter_available=perl,
        dtns_available=dtns,
        perlmutter_description=None,
        dtns_description=None,
    )
    assert s.systems_available == expected
