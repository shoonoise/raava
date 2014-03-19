all:
	true

pylint:
	pypy3 `which pylint` --rcfile=pylint.ini \
		raava \
		*.py \
		--output-format=colorized 2>&1 | less -SR

pypi:
	python setup.py register
	python setup.py sdist upload

clean:
	rm -rf build dist raava.egg-info
	find . -type f -name '*.pyc' -delete
	find . -type d -name __pycache__ -delete

