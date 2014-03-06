# -*- coding: utf-8 -

import re

from datetime import datetime
from hashlib import md5

from .base import Model
from .base import Field
from .utils import PY3K


EMAIL_REGEXP = re.compile(r"^[a-z0-9]+[_a-z0-9-]*(\.[_a-z0-9-]+)*@[a-z0-9]+[\.a-z0-9-]*(\.[a-z]{2,4})$")


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

	def __set__ (self, model, value):
		if value is not None:
			value = str(value) if PY3K else unicode(value)

			if self.minlen is not None and len(value) < self.minlen:
				raise Exception('Minimal length check failed')

			if self.maxlen is not None and len(value) > self.maxlen:
				raise Exception('Maximum length check failed')

		model[self.name] = value


class Email (IndexField):
	def __set__ (self, model, value):
		if value is not None:
			value = value.lower()

			if EMAIL_REGEXP.match(value) == None:
				raise Exception('Email validation failed')

		return super(Email, self).__set__(model, value)

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
		return val.lower()


class Integer (RangeIndexField):
	def __init__ (self, minval=None, maxval=None, **kw):
		super(Integer, self).__init__(**kw)

		if minval is not None and maxval is not None:
			assert minval < maxval

		self.minval = minval
		self.maxval = maxval

	def __set__ (self, model, value):
		if value is not None:
			value = int(value)

			if self.minval is not None and value < self.minval:
				raise Exception('Minimal value check failed')

			if self.maxval is not None and value > self.maxval:
				raise Exception('Maximum value check failed')

		model[self.name] = value

	def to_db (self, val):
		return int(val)

	def from_db (self, val):
		return int(val)


class DateTime (RangeIndexField):
	def to_db (self, val):
		return int(val.strftime('%s') if type(val) is datetime else val)

	def from_db (self, val):
		return datetime.fromtimestamp(int(val))


class MD5Pass (String):
	def __set__ (self, model, value):
		super(MD5Pass, self).__set__(model, value)

		if value is not None:
			val = model[self.name]

			if PY3K:
				val = val.encode('utf-8')

			model[self.name] = md5(val).hexdigest()


class Reference (IndexField):
	def __init__ (self, cls, **kw):
		super(Reference, self).__init__(**kw)
		assert issubclass(cls, Model)
		self._cls = cls

	def to_db (self, val):
		return val._id if isinstance(val, Model) else val

	def from_db (self, val):
		return self._cls(val)
