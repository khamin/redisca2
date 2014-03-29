# -*- coding: utf-8 -

from redisca2.base import (
	Model,
	conf,
)

from .redis import (
	RedisConnector,
)


class FlaskRedisca (object):
	def __init__ (self, app=None, autosave=False):
		self.autosave = autosave

		if app is not None:
			self.init_app(app)

	def init_app (self, app):
		self.app = app

		conf.db = RedisConnector(**self.app.config['REDISCA'])
		self.app.teardown_request(self.after_request)

	def after_request (self, exc):
		if exc is None and self.autosave:
			Model.save_all()

		Model.free_all()
