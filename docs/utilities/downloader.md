# Running the downloader

The downloader script fetches daily and periodic dumps from the EDSM site and
stores them in your landing zone. By default it writes to `./landing`, but you
can supply an alternative path as the first argument.

```bash
# Uses ./landing as the landing zone root
bash utilities/downloader.sh

# Specify an alternative landing zone root
bash utilities/downloader.sh /path/to/landing/root
```

The default behaviour matches the settings expected by the rest of the project,
so running without arguments is usually sufficient. The script can also be
executed through the Python wrapper:

```bash
python utilities/downloader.py [--script PATH_TO_DOWNLOADER_SH]
```

Downloaded files are written beneath the ``landing`` directory using a dated
subdirectory such as ``landing/data/20240101``. Streaming jobs should read from
``landing/data`` and rely on the checkpoint state to locate new subdirectories
that contain unprocessed files.
