help:
	@echo 'Individual commands:'
	@echo ' lint             - Lint the code with pylint and flake8 and check imports'
	@echo '                    have been sorted correctly'
	@echo ' test             - Run tests'
	@echo ' speed            - Run a speed test against api.acmi.net.au'
	@echo ' load             - Run a load test against api.acmi.net.au'
	@echo ''
	@echo 'Grouped commands:'
	@echo ' linttest         - Run lint and test'
lint:
	# Lint the python code
	pylint *
	flake8
	isort -rc --check-only .
test:
	# Run python tests
	pytest -v -s tests/tests.py
speed:
	# Run speed test
	python3 app/speed_test.py
load:
	# Run load test
	k6 run tests/load_test.js
linttest: lint test
