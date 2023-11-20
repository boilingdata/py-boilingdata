install:
	poetry install

test:
	echo "Using BD_USERNAME: $BD_USERNAME"
	DEBUG=true poetry run pytest -rP

format:
	poetry run black py_boilingdata/*.py tests/*.py

lint: format
	poetry run pylint py_boilingdata/*.py

build:
	poetry build

run:
	poetry run python main.py

notebook:
	jupyter notebook boilingdata.ipynb