"""Expose functional submodules.

Callers should reference functions using fully qualified module paths,
for example ``functions.read.stream_read_files``. Importing these
submodules here allows :func:`importlib.import_module` to resolve modules
such as ``functions.read`` while avoiding re-exporting function names at the
package level.
"""

import os

# Allow PySpark to bind the callback server to an ephemeral port. This prevents
# ``OSError: [Errno 22] Invalid argument`` errors that can occur when
# ``foreachBatch`` starts the callback server on some platforms. Setting the
# environment variable here ensures it is defined before any of the submodules
# import PySpark.
os.environ.setdefault("PYSPARK_ALLOW_INSECURE_PORT", "1")

from . import (
    read,
    write,
    transform,
    utility,
    rescue,
    sanity,
    config,
    project_root,
    quality,
    dq_checks,
)

__all__ = [
    "read",
    "write",
    "transform",
    "utility",
    "rescue",
    "sanity",
    "config",
    "project_root",
    "quality",
    "dq_checks",
]
