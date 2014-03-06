# -*- coding: utf-8 -

from __future__ import absolute_import

from redis import StrictRedis

from redisca2.base import BExpr
from redisca2.base import Connector
from redisca2.fields import IndexField
from redisca2.fields import RangeIndexField
from redisca2.utils import PY3K


class RedisConnector (Connector):
	def __init__ (self, *args, **kw):
		self.handler = StrictRedis(*args, **kw)

	def getkey (self, model):
		return ':'.join((model.getprefix(), model.getid()))

	def getpipe (self, pipe=None):
		return self.handler.pipeline(transaction=True) if pipe is None else pipe

	def save (self, model, pipe=None):
		_pipe = self.getpipe(pipe)

		for field in model.getfields().values():
			if not field.index and not field.unique:
				continue

			if field.name in model._dels:
				self._del_idx(
					model=model,
					field=field,
					pipe=_pipe,
				)

			elif field.name in model._diff:
				self._save_idx(field, model, _pipe)

		if model._exists is not False and len(model._dels):
			_pipe.hdel(self.getkey(model), *list(model._dels))

		if len(model._diff):
			_pipe.hmset(self.getkey(model), model._diff)

		if pipe is None and len(_pipe):
			_pipe.execute()

	def delete (self, model, pipe=None):
		""" Delete model within optionally given pipe. """

		_pipe = self.getpipe(pipe)

		for field in model.getfields().values():
			if field.index or field.unique:
				self._del_idx(field, model, _pipe)

		_pipe.delete(self.getkey(model))

		if pipe is None and len(_pipe):
			_pipe.execute()

	def exists (self, model):
		return self.handler.exists(self.getkey(model))

	def get (self, model, name):
		""" Return value of model hash key. """

		val = self.handler.hget(self.getkey(model), name)
		return val.decode('utf-8') if PY3K and val is not None else val

	def getall (self, model):
		""" Return model data (all hash keys). """

		data = dict()

		for k, v in self.handler.hgetall(self.getkey(model)).items():
			k = k.decode(encoding='UTF-8')
			v = v.decode(encoding='UTF-8')
			data[k] = v

		return data

	@staticmethod
	def idx_key (prefix, field_name, val):
		val = str(val) if PY3K else unicode(val)
		return ':'.join((prefix, field_name, val))

	@staticmethod
	def ridx_key (prefix, field_name):
		return ':'.join((prefix, field_name))

	def find (self, expr):
		assert isinstance(expr, BExpr)

		if isinstance(expr.field, IndexField):
			val = expr.field.to_db(expr.val)
			key = self.idx_key(expr.model_cls.getprefix(), expr.field.name, val)

			# IndexField supports EQ only. Ignore operator here.
			return [expr.model_cls(model_id) for model_id in self.handler.smembers(key)]

		elif isinstance(expr.field, RangeIndexField):
			key = self.ridx_key(expr.model_cls.getprefix(), expr.field.name)
			val = expr.field.to_db(expr.val)

			if expr.operator == expr.EQ:
				minval = val
				maxval = val

			elif expr.operator == expr.GT:
				minval = '(%d' % val
				maxval = '+inf'

			elif expr.operator == expr.GE:
				minval = val
				maxval = '+inf'

			elif expr.operator == expr.LT:
				minval = '-inf'
				maxval = '(%d' % val

			elif expr.operator == expr.LE:
				minval = '-inf'
				maxval = val

			else:
				raise Exception('Unsupported operator type given')

			start = expr.offset
			num = expr.limit

			if num is None and start == 0:
				start = None

			ids = self.handler.zrangebyscore(
				key,
				minval,
				maxval,
				start=start,
				num=num,
			)

			return [expr.model_cls(model_id) for model_id in ids]

	def choice (self, field, model_cls, val, count=1):
		key = self.idx_key(model_cls.getprefix(), field.name, val)
		ids = self.handler.srandmember(key, count)

		return None if not len(ids) else \
			[model_cls(model_id) for model_id in ids]

	def _save_idx (self, field, model, pipe=None):
		""" Save given model.field index. """

		if isinstance(field, IndexField):
			idx_key = self.idx_key(model.getprefix(), field.name, model[field.name])

			if field.unique:
				ids = self.handler.smembers(idx_key)

				if len(ids):
					ids.discard(bytes(model._id, 'utf-8') if PY3K else model._id)

					if len(ids):
						raise Exception('Duplicate key error')

			self._del_idx(field, model, pipe)
			pipe.sadd(idx_key, model._id)

		elif isinstance(field, RangeIndexField):
			val = model[field.name]

			if field.unique:
				models = field.find(val, val)

				if len(models) > 1 or len(models) == 1 and models[0] is not model:
					raise Exception('Duplicate key error')

			key = self.ridx_key(model.getprefix(), field.name)

			pipe.zadd(key, **{
				model._id: field.to_db(val)
			})

	def _del_idx (self, field, model, pipe=None):
		""" Delete db index value of model.field. """

		if model._exists is False:
			return

		if isinstance(field, IndexField):
			idx_val = model.get(
				name=field.name,
				origin=True,
				lite=True,
			)

			if idx_val is not None:
				idx_key = self.idx_key(model.getprefix(), field.name, idx_val)
				pipe.srem(idx_key, model._id)

		elif isinstance(field, RangeIndexField):
			key = self.ridx_key(model.getprefix(), field.name)
			pipe.zrem(key, model._id)
