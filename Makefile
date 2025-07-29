run := uv run
test:
	$(run) pytest -v

python-scan:
	$(run) python -m filescan /Users/sholden/Projects/Python/

project-scan:
	$(run) python -m filescan /Users/sholden/Projects/

etc-scan:
	$(run) python -m filescan /etc
