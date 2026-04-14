# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

locopy is a Python library for ETL processing with Amazon Redshift (`COPY`/`UNLOAD`) and Snowflake (`COPY INTO`). It wraps boto3 for S3 operations and is DB-API 2.0 adapter agnostic (tested with psycopg2, pg8000, snowflake-connector-python). Supports Python 3.10-3.14.

## Common Commands

### Install for development
```bash
pip install .[dev,psycopg2,pg8000,snowflake]
# Test data setup (needed for tests):
cp tests/data/.locopyrc ~/.locopyrc
cp tests/data/.locopy-sfrc ~/.locopy-sfrc
```

### Run tests
```bash
make not_integration              # Unit tests only (default CI target)
make coverage                     # All tests with coverage
pytest tests/test_utility.py      # Single test file
pytest tests/test_utility.py::test_find_column_type -v  # Single test
pytest -m 'not integration'       # Skip integration tests (same as make not_integration)
```

### Lint and format
```bash
ruff check                        # Lint
ruff check --fix                  # Lint with auto-fix
ruff format --check               # Check formatting
ruff format                       # Auto-format
```

### Build docs
```bash
make sphinx
```

### Dependency version bumps (edgetest)
Edgetest config is in `pyproject.toml` under `[edgetest.envs.core]`. It bumps upper bounds for boto3, PyYAML, pandas, numpy. The CI workflow runs weekly and creates PRs with updated `pyproject.toml` and `requirements.txt`. The lockfile is generated via `uv pip compile --output-file=requirements.txt pyproject.toml`.

## Architecture

```
locopy/
├── database.py    # Database - base class for DB connections (connect, execute, to_dataframe)
├── s3.py          # S3 - boto3 wrapper for upload/download/delete on S3 buckets
├── redshift.py    # Redshift(S3, Database) - multiple inheritance, adds COPY/UNLOAD + load_and_copy/unload_and_copy
├── snowflake.py   # Snowflake(S3, Database) - multiple inheritance, adds COPY INTO + internal stage support
├── utility.py     # Helpers: file splitting, compression, YAML config reading, column type detection
├── errors.py      # Custom exception hierarchy: LocopyError, DBError, S3Error (each with sub-exceptions)
├── logger.py      # Logging setup
└── _version.py    # Single source of version (__version__)
```

**Key inheritance pattern:** Both `Redshift` and `Snowflake` use multiple inheritance from `S3` and `Database`. The `S3` class handles AWS session/credentials and file transfer. `Database` handles DB connection lifecycle and query execution. The subclasses override `connect()` to set up both S3 and DB connections.

**Column type detection:** `utility.py` has `find_column_type` as a `@singledispatch` function with separate implementations for pandas (`find_column_type_pandas`) and polars (`find_column_type_polars`) DataFrames. When bumping pandas/polars versions, watch for dtype representation changes (e.g., pandas 3.0 changed string dtype from `object` to `StringDtype` and datetime resolution from `ns` to `us`).

**Version** is defined in `locopy/_version.py` and read dynamically by setuptools via `pyproject.toml` (`[tool.setuptools.dynamic]`).

## Code Style

- Linter/formatter: **ruff** (config in `pyproject.toml`). Pre-commit hooks enforce ruff + trailing whitespace + debug statements.
- Docstring convention: **numpy style** (`[tool.ruff.lint.pydocstyle] convention = "numpy"`)
- Relative imports are banned (`ban-relative-imports = "all"`)
- Target Python version: 3.12 (ruff target)

## Test Markers

- `@pytest.mark.integration` - Integration tests requiring real DB/S3 connections (skipped in CI unit test runs)
