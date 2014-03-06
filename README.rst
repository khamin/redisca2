Master branch: |Build Status|

.. |Build Status| image:: https://travis-ci.org/khamin/redisca2.png?branch=master
   :target: https://travis-ci.org/khamin/redisca2

Installation
============

Using PyPi (recommended):

::

	sudo pip install redisca2

or

::

	wget https://pypi.python.org/packages/source/r/redisca2/redisca2-X.tar.gz
	tar xvf redisca2-X.tar.gz
	sudo python redisca2-X/setup.py install

Model
=====

.. code:: python

   from redisca2 import Model
   from redisca2 import Email
   from redisca2 import DateTime

   class User (Model):
	   email = Email(
		   name='eml',  # Define link with 'eml' hash key.
		   index=True,  # Index support.
		   unique=True, # Makes sure that field is unique across db.
	   )

	   created = DateTime(
		   name='created',      # Define link with 'created' hash key.
		   new=datetime.utcnow, # Value which is used as default in User.new()
	   )

	   age = Integer(
		   name='age',  # Define link with 'age' hash key.
		   index=True,  # Enable index.
	   )

   user = User.new() # Create model with random id and "new" fields values.
   user = User.new(model_id='your_id') # Or use custom id if needed.

   user.getid() # user id
   user.email = 'foo@bar.com'

   user.save()   # Saving routines
   user.exists() # True

   user.delete() # Deletion routines
   user.exists() # False

Fields
------

Field is the way how you should control data in your models. Just define class variables with field-specific options and take classic ORM's advantages:

-  data validation;
-  native python data types support;
-  transparent relations between models;
-  indexes support (incl. unique indexes).

Available parameters:

-  **name** - redis hash field name to store value in.
-  **index** - makes field searchable.
-  **unique** - tells that value should be unique across database. Model.save() will raise an Exception if model of same class already exists with given value.
-  **new** - field value which is used as default in Model.new(). Functions, methods and built-in's are acceptable as callback values.

Built-in fields:

-  **String** - extends *IndexField* with additional parameters *minlen* and *maxlen*.
-  **Email** - extends *IndexField* field with email validation support.
-  **Integer** - extends *RangeIndexField* with parameters *minval* and *maxval*. Accepts int and numeric strings. Returns int.
-  **Reference** - extends *IndexField* with *cls* (reference class) parameter. Accepts and returns instance of *cls*.
-  **MD5Pass** - extends *String* field. Acts like string but converts given string to md5 sum.
-  **DateTime** - extends *RangeIndexField* without additional parameters. Accepts datetime and int(timestamp) values. Returns datetime.

Getting Data
------------

Using id
~~~~~~~~

Here is an example how to get model instance using id *(empty model returned if it not exists yet)*:

.. code:: python

	user = User('user id')
	print(user.email) # 'foo@bar.com'

Each initialized model is saved in registry and returned on each attempt of re-init:

.. code:: python

	user1 = User('user_id')
	user2 = User('user_id')
	user1 is user2 # Always is True

	user.free()   # Unregister model instance.
	User.free_all()  # Cleanup User's registry.
	Model.free_all() # Unregister all known models.

Find by Index
~~~~~~~~~~~~~

.. code:: python

	users = User.email == 'foo@bar.com'

Subclasses of *RangeIndexField* has a limited support for ranged queries:

.. code:: python

	users = User.age >= 10

Dict API
~~~~~~~~

All fields are linked to model dict keys. Use can use model dict API to read and write *redis hash* data AS IS:

.. code:: python

	user = User('id')
	user['eml'] = 'foo@bar.com'
	user['age'] = 10

Connecting to Redis
-------------------

Global database connection setup looks like this:

.. code:: python

	from redisca2 import conf
	from redisca2 import RedisConnector

	conf.db = RedisConnector()

**Note:** *redisca2* uses localhost:6379(0) as default database. You can setup **inheritable** per-model database connection using *conf* class decorator:

.. code:: python

	from redisca2 import Model
	from redisca2 import conf
	from redisca2 import RedisConnector

	@conf(db=RedisConnector())
	class User (Model):
		pass

Key Format
----------

Model key format is:

::

	model_key_prefix:model_id

Default model\_key\_prefix is *lowercased class name*. Use *conf* class decorator to override it like this:

.. code:: python

	from redisca2 import Model
	from redisca2 import conf

	@conf(prefix='usr')
	class User (Model):
		pass

	print(User.getprefix()) # 'usr'

Tools
=====

ID Generator
------------

.. code:: python

	from redisca2 import hexid
	from redisca2 import intid

	print(hexid()) # 59d369790
	print(hexid()) # 59d3697bc

	print(intid()) # 24116751882
	print(intid()) # 24116788848

Flask Support
-------------

.. code:: python

	from redisca2 import FlaskRedisca

	app = Flask()

	app.config['REDISCA'] = {
		# redis.StrictRedis constructor kwargs dict.
	}

	FlaskRedisca(app)

Optional *autosave* constructor parameter tells *redisca2* that all known models should be saved at the end of request (if no exception raised). Unchanged and deleted instances are ignored. If you want to skip locally changed instances use free() method during request life.

Requirements
============

-  redis-py 2.7+
-  python 2.7/3.2+ or pypy 2.1+

Python 3.x support
------------------

Py3k support is still a sort of experiment but I'm looking carefuly into full compability with cutting-edge builds of CPython. There are no known issues with it actually.
