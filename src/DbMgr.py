import sqlite3

"""
DbManager:
- connection manager
- table creation/deletion
- query builder ?
Agnostic to use-case but handles lower-level db interactions.
"""

"""
AssetDbAPI:
- handles insertion, updates, deletions and queries specific to Asset
and AssetVersion use-case. (Not stored in this file...just making notes)
- higher-level use-case specific.
Maybe I'm basically describing the AssetScv module with the distinction
that AssetSvc also handles file I/O and data validation thru type models.
"""