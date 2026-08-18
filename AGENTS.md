# AGENTS.md — Lakehouse Engine

A configuration-driven PySpark framework for data products on AWS/Databricks, built around **ACON** (Algorithm CONfiguration) dicts. Maintained by Adidas Lakehouse Foundations Engineering.

## Architecture Overview

```
engine.py (public API facade)
  └── algorithms/         # DataLoader, DQValidator, Reconciliator, GAB, Sensor, Heartbeat
        └── algorithm.py  # Base class: parses ACON, orchestrates input→transform→dq→output→terminate
  └── io/
        ├── reader_factory.py / readers/   # Table, File, JDBC, Kafka, SAP BW/B4, SFTP, Sharepoint
        └── writer_factory.py / writers/   # Table, File, JDBC, Kafka, DeltaMerge, REST API
  └── transformers/       # 15+ stateless transformer classes, config-driven (no custom Spark needed)
  └── dq_processors/      # Great Expectations abstraction (Validator, DQFactory)
  └── terminators/        # Post-write actions: optimize, vacuum, notifications
  └── core/
        ├── exec_env.py   # Singleton SparkSession + EngineConfig (reads engine.yaml)
        ├── definitions.py # All dataclass specs: InputSpec, OutputSpec, TransformerSpec, DQSpec…
        ├── table_manager.py / file_manager.py / sensor_manager.py
  └── configs/
        └── engine.yaml      # Default engine config (DQ buckets, environment, maven repo, etc.)
        └── engine_os.yaml   # Default engine config used in the open source project (DQ buckets, environment, maven repo, etc.), when available.
                      
```

**Data flow:** `engine.py` function → `ExecEnv.get_or_create()` → `validate_and_resolve_acon()` → Algorithm (`execute()`) → `ReaderFactory` → `TransformerFactory` → DQ → `WriterFactory` → `TerminatorFactory`.

## ACON Pattern (central concept)

All operations are configured via an ACON dict or JSON/YAML file. Every engine function accepts `acon_path` (file) or `acon` (dict).

**`DataLoader`** (`load_data`) uses these top-level keys:
```python
from lakehouse_engine.engine import load_data

load_data(acon={
    "input_specs": [{"spec_id": "src", "read_type": "batch", "data_format": "delta", "location": "s3://…"}],
    "transform_specs": [{"spec_id": "t1", "input_id": "src", "transformers": [{"function": "filter_exp", "args": {"exp": "active = 1"}}]}],
    "dq_specs": [{"spec_id": "dq1", "input_id": "t1", "dq_type": "validator", "dq_functions": [{"dq_function": "expect_column_values_to_not_be_null", "args": {"column": "id"}}]}],
    "output_specs": [{"spec_id": "out", "input_id": "t1", "write_type": "overwrite", "data_format": "delta", "location": "s3://…"}],
    "terminate_specs": [{"function": "optimize", "args": {"db_table": "my_db.my_table"}}],
})
```

Other algorithms use **different** top-level keys — for example, `Reconciliator` (`execute_reconciliation`) uses `truth_input_spec`, `current_input_spec`, and `reconciliator_conf`. Always check the algorithm's `__init__` and the relevant `*Spec` dataclass in `core/definitions.py` for the correct keys.

## Documentation

The generated Lakehouse Engine documentation is sourced from `lakehouse_engine_usage/`, built with `make deploy-docs-to-github`, and then published to GitHub lakehouse-engine-docs repo to be server with GitHub Pages in https://adidas.github.io/lakehouse-engine-docs/lakehouse_engine.html. Treat that content as the primary place for end-to-end feature examples: every key feature should have a usage example there, and changes that affect user-facing behavior should be reflected in the relevant usage docs.
Test fixture ACONs (JSON/YAML configs exercised by the test suite) live in `tests/resources/feature/`.

## Transformer Convention

Transformers are referenced by **function name string** in ACON, resolved by `TransformerFactory`. Each transformer module (e.g., `transformers/filters.py`, `column_creators.py`) contains static methods decorated to be callable by name. To add a transformer: implement a static method in the appropriate module and register it in `TransformerFactory`.

## Developer Workflows

All commands run inside Docker (the image must be built first):

```bash
make build-image           # Build Docker image (ARM/AMD auto-detected)
make test-local            # Run full test suite in local Spark mode
make test-local test_only="tests/feature/test_delta_load.py"  # Run specific test
make test-security      # Run Bandit security checks
make audit-dep-safety   # Run pip-audit to check for vulnerable dependencies
make lint                  # flake8 (Google docstring style) + mypy
make style                 # isort + black auto-format
make terminal              # Drop into container shell for debugging
```

Test modes:
- `--spark_mode=local` (default): standard PySpark, supports all features incl. streaming/RDD
- `--spark_mode=connect`: Spark Connect; import to test how code will behave in spark connect architecture, which is the convention nowadays. Some unsupported streaming and transformers are marked as `@pytest.mark.local_only` to be skipped in this mode.

Tests use `tests/configs/engine.yaml` (the open source version uses `tests/configs/engine_os.yaml`) and write to `tests/lakehouse/`. `conftest.py` sets up a `test_db` database backed by local filesystem.

**Test coverage expectations:** every new feature should have a feature test under `tests/feature/` and, where applicable, a fixture ACON under `tests/resources/feature/<feature>/`. Unit tests live in `tests/unit/`.

## CI/CD, Releasing & Open-Source Sync

- **Versioning**: version lives in `cicd/.bumpversion.cfg`. Run `make prepare-release increment=<major|minor|patch>` to bump and auto-generate `CHANGELOG.md`.
- **Wheel builds**: `make build` produces `lakehouse-engine-<version>-py3-none-any.whl`. Non-release builds (when `version ≠ project_version`) produce `lakehouse-engine-experimental` wheels with a `+` suffix.
- **Lock files**: split per optional feature (`cicd/requirements_*.lock`). After changing any `requirements*.txt`, run `make build-lock-files` to regenerate them. The full lock (`requirements_full.lock`) is the constraint base for all others.
- **Security**: `make test-security` runs Bandit; `make audit-dep-safety` runs pip-audit.
- **Open-source sync**: the internal repo contains files not present on [public GitHub](https://github.com/adidas/lakehouse-engine). On `make sync-to-github`, several internal-only files are **removed** and config files are **renamed**:
  - `lakehouse_engine/configs/engine_os.yaml` → `engine.yaml` (the `engine.yaml` internal file is deleted)
  - `tests/configs/engine_os.yaml` → `tests/configs/engine.yaml`
  - `cicd/meta_os.yaml` → `cicd/meta.yaml`
  - `CONTRIBUTING_OS.md` → `CONTRIBUTING.md`
  - Also removed: `sonar-project.properties`, `CODEOWNERS`, `CHANGELOG.md`, internal `assets/`
- **Agent caution**: when contributing, do not reference or commit the internal `engine.yaml` (the private config). Only `engine_os.yaml` should carry open-source-safe defaults. Check that any new config in added internally has an `_os` counterpart if it needs to be public.

## Key Conventions

- **Specs as dataclasses**: All algorithm inputs are parsed into typed dataclasses from `core/definitions.py` (`InputSpec`, `OutputSpec`, `TransformerSpec`, etc.). When adding new ACON fields, add them here first.
- **Factory pattern throughout**: `ReaderFactory`, `WriterFactory`, `TransformerFactory`, `TerminatorFactory`, `NotifierFactory` all resolve string keys to implementations. New readers/writers/transformers require registering in the appropriate factory.
- **Streaming micro-batch fallback**: Operations incompatible with Spark Structured Streaming (sorts, window functions, DQ checks) are automatically rerouted to `foreachBatch` micro-batch mode. See `DataLoader._streaming_micro_batch_transformers_plan`.
- **Engine config vs. ACON config**: `engine.yaml` (`engine_os.yaml` for the open source version) holds environment-wide defaults (S3 paths, catalog, DQ buckets). ACON holds per-job logic. Override engine config via `ExecEnv.set_default_engine_config()`.
- **Optional feature sets**: Dependencies are split into extras (`os`, `dq`, `azure`, `sftp`, `sharepoint`) defined in `pyproject.toml`. Lock files in `cicd/` must stay in sync via `make build-lock-files`.
- **Versioning**: Version lives in `cicd/.bumpversion.cfg`. Non-release builds produce `lakehouse-engine-experimental` wheels with `+` suffix.
- **Google-style docstrings** are enforced by flake8 (`cicd/flake8.conf`).
- **Method names should be action-oriented**, e.g. get_acon_details rather than acon_details.
- **Testing expectations**: add feature tests under `tests/feature/` for new behavior, unit tests under `tests/unit/` for focused logic, and fixture ACONs under `tests/resources/feature/` when applicable.
- **Common ACON changes**: add new fields to `core/definitions.py` first, then wire them through the relevant factory/algorithm and keep DQ and streaming caveats in mind when the change affects execution flow.
