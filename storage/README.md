# Storage layout

This folder documents local storage conventions.

Delta tables should not be committed to git.
Use:

- Local dev: `.local_storage/`
- Production: ADLS Gen2 (or equivalent)

Expected layers:
- Bronze: raw snapshots
- Silver: cleaned and standardized
