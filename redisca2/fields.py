# -*- coding: utf-8 -

import re

from datetime import (
	datetime,
)

from hashlib import (
	md5,
)

from .base import (
	Model,
	Field,
)

from .utils import (
	PY3K,
)


class IndexField (Field):
	""" Base class for fields with exact indexing. """

	def choice (self, val, count=1):
		""" Return *count* random model(s) from find() result. """

		assert self.index or self.unique

		return self.owner.getdb().choice(
			field=self,
			model_cls = self.owner,
			val=val,
			count=count,
		)


class RangeIndexField (Field):
	""" Base class for fields with range indexing. """
	pass


class Bool (IndexField):
	def to_db (self, val):
		return 1 if (val and val != '0') else 0

	def from_db (self, val):
		return val == '1' or val == 1


class String (IndexField):
	def __init__ (self, minlen=None, maxlen=None, **kw):
		super(String, self).__init__(**kw)

		if minlen is not None and maxlen is not None:
			assert minlen < maxlen

		self.minlen = minlen
		self.maxlen = maxlen

	def to_db (self, val):
		val = str(val) if PY3K else unicode(val)

		if self.minlen is not None and len(val) < self.minlen:
			raise Exception('Minimal length check failed')

		if self.maxlen is not None and len(val) > self.maxlen:
			raise Exception('Maximum length check failed')

		return val


class Email (IndexField):
	REGEXP = re.compile(r"^[a-z0-9]+[_a-z0-9-]*(\.[_a-z0-9-]+)*@[a-z0-9]+[\.a-z0-9-]*(\.[a-z]{2,4})$")

	def idx_key (self, prefix, val):
		if val is not None:
			val = val.lower()

		return super(Email, self).idx_key(prefix, val)

	def find (self, val, children=False):
		if val is not None:
			val = val.lower()

		return super(Email, self).find(val, children)

	def choice (self, val, count=1):
		if val is not None:
			val = val.lower()

		return super(Email, self).choice(val, count)

	def to_db (self, val):
		val = val.lower()

		if self.REGEXP.match(val) == None:
			raise Exception('Email validation failed')

		return val


class Integer (RangeIndexField):
	def __init__ (self, minval=None, maxval=None, **kw):
		super(Integer, self).__init__(**kw)

		if minval is not None and maxval is not None:
			assert minval < maxval

		self.minval = minval
		self.maxval = maxval

	def to_db (self, val):
		val = int(val)

		if self.minval is not None and val < self.minval:
			raise Exception('Minimal value check failed')

		if self.maxval is not None and val > self.maxval:
			raise Exception('Maximum value check failed')

		return val

	def from_db (self, val):
		return int(val)


class DateTime (RangeIndexField):
	def to_db (self, val):
		return int(val.strftime('%s') if type(val) is datetime else val)

	def from_db (self, val):
		return datetime.fromtimestamp(int(val))


class MD5Pass (String):
	def to_db (self, val):
		val = super(MD5Pass, self).to_db(val)
		return md5(val.encode('utf-8')).hexdigest()


class Reference (IndexField):
	def __init__ (self, cls, **kw):
		super(Reference, self).__init__(**kw)
		assert issubclass(cls, Model)
		self._cls = cls

	def to_db (self, val):
		return val._id if isinstance(val, Model) else val

	def from_db (self, val):
		return self._cls(val)
