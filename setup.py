#!/usr/bin/env python
# -*- coding: utf-8 -

from setuptools import setup, find_packages
PACKAGES = find_packages()


setup(
	name = 'redisca2',
	version = '2.0dev',
	packages = PACKAGES,
	package_dir = {'': '.'},
	test_suite = 'redisca2.tests',
	author = 'Vitaliy Khamin',
	author_email = 'vitaliykhamin@gmail.com',
	maintainer = 'Vitaliy Khamin',
	maintainer_email = 'vitaliykhamin@gmail.com',
	description = 'Lightweight ORM for Redis',
	url = 'http://github.com/khamin/redisca2',
	zip_safe = True,

	platforms = (
		'any'
	),

	classifiers = (
		'Operating System :: OS Independent',
		'Development Status :: 4 - Beta',
		'Programming Language :: Python :: 2.7',
		'Programming Language :: Python :: 3.2',
		'Programming Language :: Python :: 3.3',
		'Topic :: Database'
	),

	install_requires = [
		'redis >= 2.7'
	]
)
