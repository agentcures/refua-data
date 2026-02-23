# Contributing

## Development setup

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"
```

## Local quality checks

```bash
ruff check src tests
mypy src
pytest -q
python -m build
python -m twine check dist/*
```

## Pull requests

- Keep changes focused and include tests for behavior changes.
- Update `README.md` when user-visible behavior changes.
- Ensure CI passes before requesting review.
