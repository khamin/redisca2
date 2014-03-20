# -*- coding: utf-8 -

from inspect import (
	isbuiltin,
	isfunction,
	ismethod,
)

from .utils import (
	PY3K,
	hexid,
)


class Connector (object):
	pass


class conf (object):
	""" Configuration storage and model decorator. """

	db = None # Default connection.

	def __init__ (self, prefix=None, db=None):
		if db is not None:
			assert isinstance(db, Connector)

		self._prefix = prefix
		self._db = db

	def __call__ (self, cls):
		if self._db is not None:
			cls._db = self._db

		if self._prefix is not None:
			Model._cls2prefix[cls] = self._prefix

		return cls


class BExpr (object):
	EQ = '='
	GT = '>'
	LT = '<'
	GE = '>='
	LE = '<='

	def __init__ (self, operator, field, val):
		assert isinstance(field, Field)

		self.limit = None
		self.offset = 0
		self.models = None
		self.operator = operator
		self.model_cls = field.owner
		self.field = field
		self.val = val

	def __len__ (self):
		self.load()
		return len(self.models)

	def __getitem__ (self, key):
		self.load()
		return self.models[key]

	def __setitem__ (self, key, value):
		raise NotImplementedError()

	def __iter__ (self):
		self.load()
		return iter(self.models)

	def __contains__ (self, item):
		self.load()
		return item in self.models

	def loaded (self):
		return self.models is not None

	def unload (self):
		self.models = None

	def load (self):
		""" Load result into expression. """

		if self.loaded():
			return

		self.models = self.model_cls.getdb().find(self)


class Field (object):
	def __init__ (self, name, index=False, unique=False, new=None, none=None):
		self.new = new
		self.index = bool(index)
		self.unique = bool(unique)
		self.name = name
		self.none = none

	def __get__ (self, model, owner):
		self.owner = owner

		if model is None:
			return self

		return self.from_db(model[self.name]) \
			if self.name in model else self.none

	def __set__ (self, model, value):
		""" Warning: do not overwrite it in custom fields! """
		model[self.name] = None if value is None else self.to_db(value)

	def __lt__ (self, other):
		return BExpr(operator=BExpr.LT, field=self, val=other)

	def __le__ (self, other):
		return BExpr(operator=BExpr.LE, field=self, val=other)

	def __gt__ (self, other):
		return BExpr(operator=BExpr.GT, field=self, val=other)

	def __ge__ (self, other):
		return BExpr(operator=BExpr.GE, field=self, val=other)

	def __eq__ (self, other):
		return BExpr(operator=BExpr.EQ, field=self, val=other)

	def from_db (self, val):
		return val

	def to_db (self, val):
		return str(val) if PY3K else unicode(val)


class MetaModel (type):
	def __new__ (mcs, name, bases, dct):
		cls = super(MetaModel, mcs).__new__(mcs, name, bases, dct)
		cls._objects = dict() # id -> model objects registry.
		cls._fields = dict()

		for name in dir(cls):
			member = getattr(cls, name)

			if isinstance(member, Field):
				cls._fields[name] = member

		return cls

	def __setattr__ (cls, name, val):
		if isinstance(val, Field):
			cls._fields[name] = val

		super(MetaModel, cls).__setattr__(name, val)

	def __call__ (cls, model_id, *args, **kw):
		if model_id is None:
			model_id = ''

		elif PY3K and type(model_id) is bytes:
			model_id = model_id.decode('utf-8')

		else:
			model_id = str(model_id)

		if model_id not in cls._objects:
			cls._objects[model_id] = object.__new__(cls, *args, **kw)
			cls._objects[model_id].__init__(model_id)

		return cls._objects[model_id]


if PY3K:
	exec('class BaseModel (metaclass=MetaModel): pass')

else:
	exec('class BaseModel (object): __metaclass__ = MetaModel')


class Model (BaseModel):
	_cls2prefix = dict()

	def __init__ (self, model_id, must_exist=False):
		self._id = model_id
		self._exists = None
		self._diff = dict() # Local changes.
		self._dels = set()  # Removed field names.
		self._data = None   # Data from database.

		if must_exist and not self.exists():
			raise Exception('%s(%s) not found' % (
				self.__class__.__name__,
				self._id,
			))

	def __len__ (self):
		return len(self.getall())

	def __contains__ (self, name):
		if name in self._dels:
			return False

		if name in self._diff:
			return True

		self.load()
		return name in self._data

	def __getitem__ (self, name):
		if name in self._dels:
			raise KeyError(name)

		if name in self._diff:
			return self._diff[name]

		self.load()
		return self._data[name]

	def __setitem__ (self, name, value):
		if self.loaded() and name in self._data and self._data[name] == value:
			if name in self._diff:
				del self._diff[name]

			self._dels.discard(name)

		elif value is None:
			del self[name]

		else:
			self._diff[name] = value
			self._dels.discard(name)

	def __delitem__ (self, name):
		if self._exists is not False:
			self._dels.add(name)

		if name in self._diff:
			del self._diff[name]

	def get (self, name, default=None, origin=False, lite=False):
		""" Return value of model[name].
		Default value is returned if name not in model hash.
		Origin flag tells that local changes should be ignored.
		Lite mode will try to fetch single hash key instead of entire model
		loading (if connector supports that). """

		if not origin:
			if name in self._dels:
				return default

			if name in self._diff:
				return self._diff[name]

		if lite and not self.loaded():
			return self.getdb().get(
				model=self,
				name=name,
			)

		self.load()
		return self._data[name] if name in self._data else default

	def getall (self, origin=False):
		""" Return model data dict.
		Origin parameters tells that local changes should be ignored. """

		self.load()
		data = self._data.copy()

		if origin:
			return data

		else:
			data.update(self._diff)

			for name in self._dels:
				if name in data:
					del data[name]

			return data

	def pop (self, name, default=None):
		val = self.get(name, default)
		del self[name]
		return val

	def exists (self):
		""" Check if model key exists. """

		if self._exists is None:
			self._exists = self.getdb().exists(self)

		if self._exists is False and not self.loaded():
			self._data = dict()

		return self._exists

	def revert (self):
		""" Revert local changes. """
		self._diff = dict()
		self._dels = set()

	def getdiff (self):
		return self._diff.copy()

	@classmethod
	def getdb (cls):
		try:
			return cls._db
		
		except:
			return conf.db

	@classmethod
	def getfields (cls):
		""" Return name -> field dict of registered fields. """
		return cls._fields.copy()

	def getid (self):
		return self._id

	@classmethod
	def getprefix (cls):
		if cls not in Model._cls2prefix:
			Model._cls2prefix[cls] = cls.__name__.lower()

		return Model._cls2prefix[cls]

	@classmethod
	def new (cls, model_id=None):
		""" Return new model with given id and field.new values.
		If model id is None hexid() will be used instead.
		Exception raised if model already exists.

		Notice: if model with such id was initialized previously (already in
		registry) this method will overwrite it with field.new values. """

		if model_id is None:
			model_id = hexid()

		model = cls(model_id)

		if model.exists():
			raise Exception('%s(%s) already exists' % (cls.__name__, model_id))

		return model.fill_new()

	def fill_new (self):
		""" Fill model with *new* values. """

		for name, field in self.getfields().items():
			if field.new is None:
				continue

			val = field.new() if isfunction(field.new) or \
				ismethod(field.new) or isbuiltin(field.new) else field.new

			setattr(self, name, val)

		return self

	def export (self, keep_none=False):
		""" Export model fields data as dict. """

		data = dict()

		for name in self.getfields():
			val = getattr(self, name)

			if keep_none or val is not None:
				data[name] = val

		return data

	def load (self):
		""" Load data into hash if needed. """

		if self.loaded():
			return

		if self._exists is False:
			self._data = dict()
			return

		data = self.getdb().getall(self)
		self._load(data)

	def loaded (self):
		""" Check if model data is loaded. """
		return self._data is not None

	def unload (self):
		""" Unload model data. """
		self._data = None

	def delete (self, pipe=None):
		if self._exists is not False:
			self.getdb().delete(self, pipe)
			self._exists = False

		self._data = dict()
		self.revert()

	def save (self, pipe=None):
		if not len(self._diff) and not len(self._dels):
			return

		self.getdb().save(self, pipe)

		if self.loaded():
			self._data.update(self._diff)

			for name in self._dels:
				if name in self._data:
					del self._data[name]

		self._exists = True
		self.revert()

	@classmethod
	def save_all (cls, pipe=None):
		""" Save all known models. Deleted models ignored by empty diff. """

		if cls is not Model:
			_pipe = cls.getdb().getpipe(pipe)

			for model in cls._objects.values():
				model.save(_pipe)

			if pipe is None and len(_pipe):
				_pipe.execute()

		for child in cls.__subclasses__():
			child.save_all()

	def free (self):
		del self.__class__._objects[self._id]

	@classmethod
	def free_all (cls):
		""" Cleanup models registry. """

		cls._objects = dict()

		for child in cls.__subclasses__():
			child.free_all()

	@classmethod
	def inheritors (cls):
		""" Get model inheritors. """

		subclasses = set()
		classes = [cls]

		while classes:
			parent = classes.pop()

			for child in parent.__subclasses__():
				if child not in subclasses:
					subclasses.add(child)
					classes.append(child)

		return subclasses

	def _load (self, data):
		""" Load given data into model. """
		assert type(data) is dict

		self._data = data
		self._exists = bool(len(self._data))

		for k in self._data:
			if k in self._diff and self._data[k] == self._diff[k]:
				del self._diff[k]
