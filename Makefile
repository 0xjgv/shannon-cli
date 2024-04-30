install:
	poetry config virtualenvs.in-project true
	poetry env use python3.12
	poetry install -vv

check:
	poetry run ruff check --fix .

format: check
	poetry run ruff format .

clean:
	rm -fr **/__pycache__ .pytest_cache
	poetry run ruff clean

test:
	poetry run pytest -vv

update-dependencies:
	poetry config virtualenvs.in-project true
	poetry update

