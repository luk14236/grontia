# Storage

This folder documents storage conventions only.

Delta tables must NOT be committed to git.

Expected locations:
- Local development: .local_storage/delta/
- Production: ADLS Gen2

Layers:
- Bronze: raw snapshots
- Silver: cleaned and standardized data
