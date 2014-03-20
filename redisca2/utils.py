# -*- coding: utf-8 -

from time import (
	time,
)

from random import (
	randint,
)

from sys import (
	version_info,
)


# If running on Python 3.x
PY3K = version_info[0] == 3


def intid ():
	""" Return pseudo-unique decimal id. """
	return int((time() - 1374000000) * 100000) * 100 + randint(0, 99)


def hexid ():
	""" Return pseudo-unique hexadecimal id. """
	return '%x' % intid()
