# -*- coding: utf-8 -

from unittest import TestCase
from datetime import datetime
from time import time

from redisca2 import PY3K
from redisca2 import RedisConnector
from redisca2 import Model
from redisca2 import Field
from redisca2 import Bool
from redisca2 import Email
from redisca2 import Integer
from redisca2 import String
from redisca2 import MD5Pass
from redisca2 import DateTime
from redisca2 import Reference
from redisca2 import hexid
from redisca2 import intid
from redisca2 import conf

NOW_TS = int(time())
NOW = datetime.fromtimestamp(NOW_TS)


redis0 = RedisConnector(db=0)
redis1 = RedisConnector(db=1)

conf.db = redis0


class BaseModel (Model):
	created = DateTime(
		name='created',
		new=NOW,
	)


@conf(prefix='u')
class User (BaseModel):
	email = Email(
		name='eml',
		unique=True,
	)

	password = MD5Pass(
		name='pass',
		minlen=4,
	)

	name = String(
		name='name',
		minlen=4,
		maxlen=10,
		index=True,
	)

	age = Integer(
		name='age',
		minval=0,
		maxval=100,
		index=True,
	)


@conf(db=redis1)
class Language (BaseModel):
	created = DateTime(
		name='created',
		new=datetime.utcnow,
	)

	active = Bool (
		name='active',
		new=lambda: False,
		index=True,
	)

	name = String(
		name='name'
	)


User.lang = Reference(
	Language,
	name='lang',
	index=True,
)


class SubUser (User):
	pass


class SubLang (Language):
	pass


SubLang.flag = String(
	name='flag',
)

SubLang.foobar = 'foobar'


class ModelTestCase (TestCase):
	def setUp (self):
		redis0.handler.flushdb()
		redis1.handler.flushdb()

	def tearDown (self):
		User.free_all()
		Language.free_all()

	def test_prefix (self):
		self.assertEqual(User.getprefix(), 'u')
		self.assertEqual(SubUser.getprefix(), 'subuser')
		self.assertEqual(Language.getprefix(), 'language')
		self.assertEqual(SubLang.getprefix(), 'sublang')

	def test_db (self):
		self.assertTrue(User.getdb() is redis0)
		self.assertTrue(SubUser.getdb() is redis0)
		self.assertTrue(Language.getdb() is redis1)
		self.assertTrue(SubLang.getdb() is redis1)

	def test_registry (self):
		self.assertTrue(User(1) is User(1))
		self.assertTrue(User(1) is not User(2))

		self.assertTrue(Language(1) is Language(1))
		self.assertTrue(Language(1) is not Language(2))

		self.assertTrue(Language(1) is not User(1))
		self.assertTrue(User(1) is not Language(2))

		self.assertTrue(User(1).getid() in User._objects)
		self.assertTrue(User(2).getid() in User._objects)
		self.assertTrue(Language(1).getid() in Language._objects)
		self.assertTrue(Language(2).getid() in Language._objects)

		self.assertFalse(User(1).getid() in SubUser._objects)
		self.assertFalse(User(2).getid() in SubUser._objects)

	def test_attrs (self):
		user1 = User(1)
		user2 = User(2)

		self.assertFalse(user1.loaded())
		self.assertFalse(user2.loaded())
		self.assertEqual(user1.getdiff(), dict())
		self.assertEqual(user2.getdiff(), dict())
		self.assertTrue(user1.getdiff() is not user2.getdiff())

		with self.assertRaises(KeyError):
			user1['name']

		self.assertTrue(user1.loaded())
		self.assertFalse(user2.loaded())
		self.assertEqual(user1.getdiff(), dict())
		self.assertEqual(user2.getdiff(), dict())

		with self.assertRaises(KeyError):
			user2['name']

		self.assertTrue(user1.loaded())
		self.assertTrue(user2.loaded())
		self.assertEqual(user1.getdiff(), dict())
		self.assertEqual(user2.getdiff(), dict())

		user1['name'] = 'John Smith'

		self.assertEqual(user1['name'], 'John Smith')

		with self.assertRaises(KeyError):
			user2['name']

		self.assertEqual(user1.getdiff(), {'name': 'John Smith'})
		self.assertEqual(user2.getdiff(), dict())

		user2['name'] = 'Sarah Smith'

		self.assertEqual(user1['name'], 'John Smith')
		self.assertEqual(user2['name'], 'Sarah Smith')
		self.assertEqual(user1.getdiff(), {'name': 'John Smith'})
		self.assertEqual(user2.getdiff(), {'name': 'Sarah Smith'})

		user1.unload()
		self.assertFalse(user1.loaded())
		self.assertEqual(user1.getdiff(), {'name': 'John Smith'})
		self.assertEqual(user1['name'], 'John Smith')
		self.assertFalse(user1.loaded())

		user2.revert()
		self.assertTrue(user2.loaded())

		with self.assertRaises(KeyError):
			user2['name']

		self.assertTrue(user2.loaded())

	def test_getdiff (self):
		js = {'name': 'John Smith'}
		sg = {'name': 'Steve Gobs'}

		user = User(1)

		user.name = 'John Smith'
		self.assertEqual(user.getdiff(), js)

		user.save()
		self.assertEqual(user.getdiff(), dict())

		user.name = 'Steve Gobs'
		self.assertEqual(user.getdiff(), sg)

		user.save()
		self.assertEqual(user.getdiff(), dict())

	def test_new (self):
		user = User.new()

		self.assertEqual(type(user.getid()), str)
		self.assertTrue(len(user.getid()) > 0)

		self.assertEqual(user.name, None)
		self.assertEqual(user.created, NOW)

	def test_save_delete (self):
		user = User(1)
		user.name = 'John Smith'

		user.save()
		self.assertFalse(user.loaded())
		self.assertEqual(user.getdiff(), dict())
		self.assertTrue(redis0.handler.exists('u:1'))
		self.assertEqual(redis0.handler.hgetall('u:1'), {b'name': b'John Smith'})
		self.assertTrue(redis0.handler.exists('u:name:John Smith'))
		self.assertEqual(redis0.handler.smembers('u:name:John Smith'), set([b'1']))

		user.name = 'Steve Gobs'
		user.save()

		self.assertFalse(redis0.handler.exists('u:name:John Smith'))
		self.assertEqual(redis0.handler.smembers('u:name:Steve Gobs'), set([b'1']))

		user.delete()
		self.assertTrue(user.loaded())
		self.assertEqual(user.getdiff(), dict())

		self.assertFalse(redis0.handler.exists('u:1'))
		self.assertFalse(redis0.handler.exists('u:name:John Smith'))

	def test_reference (self):
		self.assertEqual(User.name.choice('John Smith'), None)

		user = User(1)
		user.name = 'John Smith'

		Language(1)['name'] = 'English'
		user.lang = Language(1)

		self.assertFalse(user.loaded())
		self.assertEqual(user['lang'], '1')
		self.assertEqual(user.lang, Language(1))

		user.save()

		# Check references
		self.assertEqual((User.name == 'John Smith')[0], User(1))
		self.assertEqual(User.name.choice('John Smith'), [User(1)])

		self.assertFalse(user.loaded())
		self.assertEqual(user['lang'], '1')
		self.assertEqual(user.lang, Language(1))
		self.assertEqual((User.lang == Language(1))[0], User(1))
		self.assertTrue(user.loaded())

		self.assertTrue(redis0.handler.exists('u:1'))
		self.assertEqual(redis0.handler.hget('u:1', 'lang'), b'1')
		self.assertFalse(redis1.handler.exists('language:1'))

		Language(1).save()

		self.assertTrue(redis1.handler.exists('language:1'))

		user.delete()
		Language(1).delete()

		self.assertFalse(redis0.handler.exists('u:1'))
		self.assertFalse(redis1.handler.exists('language:1'))

	def test_dupfield (self):
		user = User(1)
		user.email = 'foo@bar.com'
		user.save()

		with self.assertRaises(Exception):
			user = User(2)
			user.email = 'foo@bar.com'
			user.save()

		user = User(1)
		user.email = None

		self.assertTrue(user.email is None)

		with self.assertRaises(KeyError):
			user['eml']

		user.save()

		self.assertTrue(user.email is None)

		with self.assertRaises(KeyError):
			user['eml']

		self.assertFalse(redis0.handler.scard('u:eml:foo@bar.com'), 0)
		self.assertFalse(redis0.handler.exists('u:eml:None'))
		user.save()

	def test_email (self):
		user = User(1)
		user.email = 'foo@bar.com'
		self.assertEqual(user['eml'], 'foo@bar.com')

		with self.assertRaises(Exception):
			user.email = 'foo@@bar.com'

		self.assertEqual(user.email, 'foo@bar.com')

	def test_int (self):
		user = User(1)
		user.age = '26'
		self.assertEqual(user['age'], 26)

		with self.assertRaises(Exception):
			user.age = 'foobar'

		with self.assertRaises(Exception):
			user.age = '-1'

		with self.assertRaises(Exception):
			user.age = -1

		with self.assertRaises(Exception):
			user.age = '101'

		with self.assertRaises(Exception):
			user.age = 101

		self.assertEqual(user.age, 26)

	def test_datetime (self):
		ts = int(time())
		dt = datetime.fromtimestamp(ts)

		user = User(1)
		user.created = dt
		self.assertEqual(user.created, dt)
		self.assertEqual(user['created'], ts)

		user.created = ts
		self.assertEqual(user.created, dt)
		self.assertEqual(user['created'], ts)

		with self.assertRaises(Exception):
			user.created = 'foobar'

	def test_md5pass (self):
		user = User(1)
		user.password = 'foobar'
		self.assertEqual(user.password, '3858f62230ac3c915f300c664312c63f')
		self.assertEqual(user['pass'], '3858f62230ac3c915f300c664312c63f')

		with self.assertRaises(Exception):
			user.password = 'foo'

		with self.assertRaises(Exception):
			user.password = 123

	def test_string (self):
		user = User(1)
		user.name = 'foobar'
		self.assertEqual(user.name, 'foobar')
		self.assertEqual(user['name'], 'foobar')

		user.name = 1234
		self.assertEqual(user.name, '1234')
		self.assertEqual(user['name'], '1234')

		with self.assertRaises(Exception):
			user.name = 'foo'

		with self.assertRaises(Exception):
			user.name = '1234567890_'

	def test_getall (self):
		user = User(1)
		user.name = 'foobar'
		user.email = 'foo@bar.com'
		user.created = NOW

		self.assertEqual(len(user.getall(origin=True)), 0)
		self.assertEqual(len(user.getall()), 3)

		user.save()

		self.assertEqual(len(user.getall(origin=True)), 3)
		self.assertEqual(len(user.getall()), 3)

		user.name  = None
		user.email = None
		del user['created']

		self.assertEqual(len(user.getall()), 0)
		self.assertEqual(len(user.getall(origin=True)), 3)

		user.unload()

		self.assertEqual(len(user.getall(origin=True)), 3)
		self.assertEqual(len(user.getall()), 0)

		user.save()

		self.assertEqual(len(user.getall()), 0)
		self.assertEqual(len(user.getall(origin=True)), 0)

		user.unload()

		self.assertEqual(len(user.getall()), 0)
		self.assertEqual(len(user.getall(origin=True)), 0)

	def test_export (self):
		user = User(1)
		user.name = 'foobar'
		user.email = 'foo@bar.com'
		user['created'] = NOW_TS

		self.assertEqual(user.export(), {
			'email': 'foo@bar.com',
			'name': 'foobar',
			'created': NOW,
		})

		self.assertEqual(user.export(keep_none=True), {
			'email': 'foo@bar.com',
			'password': None,
			'name': 'foobar',
			'created': NOW,
			'age': None,
			'lang': None,
		})

		user.name  = None
		user.email = None
		del user['created']

		self.assertEqual(user.export(), dict())

		self.assertEqual(user.export(keep_none=True), {
			'email': None,
			'name': None,
			'password': None,
			'created': None,
			'age': None,
			'lang': None,
		})

	def test_get (self):
		user = User(1)
		self.assertEqual(user.get('somekey'), None)

	def test_save_all (self):
		user = User(1)
		user.email = 'foo@bar.com'

		user.lang = Language(1)
		user.lang.name = 'English'

		self.assertFalse(redis0.handler.exists('u:1'))
		self.assertFalse(redis1.handler.exists('language:1'))

		Model.save_all()

		self.assertTrue(redis0.handler.exists('u:1'))
		self.assertTrue(redis1.handler.exists('language:1'))
		self.assertTrue(redis0.handler.exists('u:lang:1'))

	def test_hexid (self):
		self.assertEqual(type(hexid()), str)

	def test_intid (self):
		self.assertEqual(type(intid()), int)

	def test_range_idx (self):
		for i in range(1, 10):
			user = User(i)
			user.age = i

		User.save_all()

		# Test EQ.

		users = User.age == 5
		self.assertEqual(len(users), 1)
		self.assertEqual(users[0], User(5))
		self.assertEqual(users[0].age, 5)

		#TODO self.assertEqual(99, len(User.age.range()))
		#TODO self.assertEqual(99, len(User.age.range(1, 99)))

		# Test LT.

		users = User.age < 7
		self.assertEqual(6, len(users))

		for i in range(1, 7):
			self.assertTrue(User(i) in users)

		for i in range(7, 10):
			self.assertFalse(User(i) in users)

		# Test LE.

		users = User.age <= 7
		self.assertEqual(7, len(users))

		for i in range(1, 8):
			self.assertTrue(User(i) in users)

		for i in range(8, 10):
			self.assertFalse(User(i) in users)

		#TODO self.assertEqual(26, len(User.age.range(25, 50)))

		# Test GT.

		users = User.age > 2
		self.assertEqual(7, len(users))

		for i in range(3, 10):
			self.assertTrue(User(i) in users)

		for i in range(1, 3):
			self.assertFalse(User(i) in users)

		# Test GE.

		users = User.age >= 2
		self.assertEqual(8, len(users))

		for i in range(2, 10):
			self.assertTrue(User(i) in users)

		for i in range(1, 2):
			self.assertFalse(User(i) in users)

		# Test limits.

		users = User.age >= 2
		users.limit = 2

		self.assertEqual(2, len(users))
		self.assertTrue(User(2) in users)
		self.assertTrue(User(3) in users)

		# Test offset.

		users = User.age < 7
		users.offset = 1
		users.limit = 2

		self.assertEqual(2, len(users))
		self.assertTrue(User(2) in users)
		self.assertTrue(User(3) in users)

	def test_none_id (self):
		self.assertTrue(User(None) is not User('None'))
		self.assertTrue(User(None) is User(''))

	def test_unicode (self):
		names = ['Вася', 'Пупкин', 'John', 'Smith']

		if not PY3K:
			names[0] = names[0].decode('utf8')
			names[1] = names[1].decode('utf8')

		for name in names:
			user = User(1)
			user.name = name

			self.assertEqual(user.name, name)
			user.save()

			self.assertEqual(user.name, name)
			user.free()

			user = User(1)
			self.assertEqual(user.name, name)

			user.free()
			user = (User.name == name)[0]
			self.assertEqual(user.name, name)

			for find_name in names:
				if find_name is name:
					continue

				users = User.name == find_name
				self.assertEqual(len(users), 0)

			user.delete()
			user.free()

			for find_name in names:
				users = User.name == find_name
				self.assertEqual(len(users), 0)

	def test_bool (self):
		lang = Language.new(1)
		self.assertTrue(lang.active is False)

		lang.active = 1
		self.assertTrue(lang.active is True)
		self.assertTrue(lang['active'] is 1)

		lang.active = 0
		self.assertTrue(lang.active is False)
		self.assertTrue(lang['active'] is 0)

		lang.active = True
		self.assertTrue(lang.active is True)
		self.assertTrue(lang['active'] is 1)

		lang.save()
		lang.free()
		
		lang = Language(1)
		self.assertTrue(lang.active is True)

		lang.active = 0
		self.assertTrue(lang.active is False)
		self.assertTrue(lang['active'] is 0)

		lang.save()
		lang.free()

		lang = Language(1)
		self.assertTrue(lang.active is False)

	def test_inheritors (self):
		children = Language.inheritors()
		self.assertEqual(children, set([SubLang]))

	def test_find_children (self):
		Language.new(1).save()
		SubLang.new(1).save()

		languages = Language.active == '0'
		self.assertEqual(languages[0], Language(1))

		languages = SubLang.active == '0'
		self.assertEqual(languages[0], SubLang(1))

	def test_email_ci (self):
		user1 = User(1)
		user1.email = 'FOO@BAR.COM'
		self.assertEqual(user1.email, 'foo@bar.com')
		user1.save()

		self.assertEqual((User.email == 'FOO@BAR.COM')[0], user1)
		self.assertEqual((User.email == 'foo@bar.com')[0], user1)

	def test_getfields (self):
		lang_fields = {
			'created': Language.created,
			'active': Language.active,
			'name': Language.name,
		}

		user_fields = {
			'created': BaseModel.created,
			'email': User.email,
			'password': User.password,
			'name': User.name,
			'age': User.age,
			'lang': User.lang,
		}

		self.assertEqual(User.getfields(), user_fields)
		self.assertEqual(Language.getfields(), lang_fields)
		self.assertEqual(SubUser.getfields(), user_fields)

		lang_fields.update({
			'flag': SubLang.flag,
		})

		self.assertEqual(SubLang.getfields(), lang_fields)
		self.assertEqual(SubLang.foobar, 'foobar')

	def test_idx_expr (self):
		lang = Language.new(1)
		lang.save()

		for operand in ('0', 0):
			langs = Language.active == operand

			self.assertEqual(len(langs), 1)
			self.assertTrue(lang in langs)
			self.assertEqual(langs[0], lang)

		for operand in ('1', 1):
			langs = Language.active == operand
			self.assertEqual(len(langs), 0)
			self.assertTrue(lang not in langs)

	def test_issue1 (self):
		user1 = User(1)

		user1.email = 'foo@bar.com'
		self.assertEqual(user1.email, 'foo@bar.com')

		user1.save()
		user1.load()

		user1.email = 'bar@foo.org'
		self.assertEqual(user1.email, 'bar@foo.org')

		user1.email = 'foo@bar.com'
		self.assertEqual(user1.email, 'foo@bar.com')
		self.assertEqual(user1._diff, dict())

		user1.email = None
		self.assertEqual(user1.email, None)
		self.assertEqual(user1._diff, dict())
		self.assertEqual(len(user1._dels), 1)

		user1.email = 'foo@bar.com'
		self.assertEqual(user1.email, 'foo@bar.com')
		self.assertEqual(user1._diff, dict())
		self.assertEqual(len(user1._dels), 0)
