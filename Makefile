superclean: clean
	-rm *.egg*
	-rm -rf *.egg*

clean:
	-find . -type f -name "*.py[co]" -exec rm {} \;
	-find . -type d -name "__pycache__" -exec rm -r {} \;
	-rm -rf build/
	-rm -rf dist/

test: test2 test3 test-pypy
	

test2: clean
	python setup.py test

test3: clean
	python3.3 setup.py test

test-pypy: clean
	pypy setup.py test

audit:
	pylint --rcfile=pylintrc redisca/

public: test
	python setup.py sdist upload
