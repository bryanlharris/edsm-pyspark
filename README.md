# EDSM Utilities

This repository contains scripts and notebooks for ingesting data from the Elite Dangerous Star Map (EDSM).

## Building a wheel

To build the package locally:

```bash
pip install build
python -m build
```

## Locating bundled resources

After installing the package, resource files such as the JSON settings in
`layer_01_bronze` can be found relative to the installed package directory. Use
`PROJECT_ROOT` from `functions.project_root` to construct these paths:

```python
from functions.project_root import PROJECT_ROOT

path = PROJECT_ROOT / "layer_01_bronze" / "codex.json"
```

`PROJECT_ROOT` resolves to the directory containing the installed `edsm`
package, allowing scripts and notebooks to load bundled files regardless of the
current working directory.

## Running an ingest job

The `03_ingest.ipynb` notebook can be replaced by the `scripts.ingest` module
which executes the same pipeline from the command line. Provide the layer
color and table name as named arguments so the script can locate the appropriate settings file:

```bash
python -m scripts.ingest --color bronze --table codex
```

This command loads `layer_*_bronze/codex.json` from the installed package and
runs the read/transform/write pipeline defined there.

## Running as a wheel in Databricks

When configuring a Databricks job to execute the built wheel, provide the
following details:

* **Package to import:** `edsm`
* **Function to call:** `scripts.ingest:main`

The entry point `scripts.ingest:main` is declared in `pyproject.toml` under
`[project.scripts]` so Databricks can invoke the ingest pipeline after
installing the wheel.
