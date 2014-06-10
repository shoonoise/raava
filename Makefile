all:
	true

tox:
	pypy3 -m tox

pylint:
	pypy3 -m tox -e pylint

pypi:
	python setup.py register
	python setup.py sdist upload

clean-all: clean
	rm -rf .tox

clean:
	rm -rf build dist raava.egg-info
	find . -type f -name '*.pyc' -delete
	find . -type d -name __pycache__ -delete
