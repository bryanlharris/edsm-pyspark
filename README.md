# EDSM Utilities

This repository contains notebooks and utilities for ingesting data from the Elite Dangerous Star Map (EDSM).

## Locating bundled resources

After installing the package, resource files such as the JSON settings in
`layer_01_bronze` can be found relative to the installed package directory. Use
`PROJECT_ROOT` from `functions.project_root` to construct these paths:

```python
from functions.project_root import PROJECT_ROOT

path = PROJECT_ROOT / "layer_01_bronze" / "codex.json"
```

`PROJECT_ROOT` resolves to the directory containing the installed `edsm`
package, allowing notebooks to load bundled files regardless of the current
working directory.

