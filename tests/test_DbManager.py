import pytest
from AssetServiceController.DbManager import DBManager, with_db_manager

"""
for simplicity as well as the fact that sqlite is lightweight file-based
database that can be easily created/dropped, I'm going to test
with an actual database instead of mocks. In a production environment,
I'd likely go with SQLAlchemy or SQLModel with a PostgreSQL database
in which case I'd probably use mocks for testing.
"""

def test_db_manager_connection():
    db = DBManager()
    assert db.session is not None
    # not sure if this is the best connection test...
    cursor = db.session.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [t["name"] for t in cursor.fetchall()]
    assert "sqlite_sequence" in tables

def test_create_asset_table():
    db = DBManager()
    db.drop_table("assets") 
    db.create_asset_table()
    cursor = db.session.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [t["name"] for t in cursor.fetchall()]
    assert "assets" in tables

def test_create_asset_version_table():
    db = DBManager()
    db.drop_table("asset_versions") 
    db.create_asset_version_table()
    cursor = db.session.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [t["name"] for t in cursor.fetchall()]
    assert "asset_versions" in tables
    db.drop_table("asset_versions")

def test_ensure_table():
    db = DBManager()
    db.drop_table("assets")
    with pytest.raises(ValueError):
        db.ensure_table("assets")
    db.create_asset_table()
    db.ensure_table("assets")
    db.drop_table("assets")

def test_ensure_table_fail():
    db = DBManager()
    db.drop_table("assets")
    with pytest.raises(ValueError):
        db.ensure_table("assets")