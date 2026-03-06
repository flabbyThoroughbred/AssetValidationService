import pytest
from sqlite3 import IntegrityError, ProgrammingError

from AssetServiceController.api.Model import Asset, AssetVersion
from AssetServiceController.api.DbManager import DBManager, with_db_manager

"""
for simplicity as well as the fact that sqlite is lightweight file-based
database that can be easily created/dropped, I'm going to test
with an actual database instead of mocks. In a production environment,
I'd likely go with SQLAlchemy or SQLModel with a PostgreSQL database
in which case I'd probably use mocks for testing.
"""

class TestTableCreation:
    def test_create_asset_table(self):
        db = DBManager()
        db.drop_table("assets")
        db.create_asset_table()
        cursor = db.session.execute("SELECT name FROM sqlite_master WHERE type='table' and name='assets';")
        tables = [t["name"] for t in cursor.fetchall()]
        assert "assets" in tables
        db.drop_table("assets")

    def test_create_asset_version_table(self):
        db = DBManager()
        db.drop_table("asset_versions")
        db.create_asset_version_table()
        cursor = db.session.execute("SELECT name FROM sqlite_master WHERE type='table' and name='asset_versions';")
        tables = [t["name"] for t in cursor.fetchall()]
        assert "asset_versions" in tables
        db.drop_table("asset_versions")

    def test_ensure_table_fail(self):
        db = DBManager()
        # make sure asset table doesn't exist
        db.drop_table("assets")
        with pytest.raises(ValueError):
            db.ensure_table("assets")

    def test_ensure_table(self):
        db = DBManager()
        db.create_asset_table()
        try:
            db.ensure_table("assets")
        except ValueError:
            assert False, "ensure_table raised ValueError unexpectedly"


class TestInsertions:
    @with_db_manager()
    def build_tables(self, mgr: DBManager):
        """clear tables and rebuild."""
        mgr.drop_table("asset_versions")
        mgr.drop_table("assets")
        mgr.create_asset_table()
        mgr.create_asset_version_table()

    @with_db_manager()
    def drop_tables(self, mgr: DBManager):
        mgr.drop_table("asset_versions")
        mgr.drop_table("assets")

    def test_insert_asset(self):
        """
        drop/rebuild tables and insert single asset record.
        Assert that the id of the asset is 1.
        """
        self.build_tables()
        mock_asset = Asset(name="guy", type="character")
        db = DBManager()
        asset_ids = db.insert_assets([mock_asset])
        assert len(asset_ids) == 1
        assert asset_ids[0] == 1
        self.drop_tables()

    def test_insert_asset_fail(self):
        """
        Test insertion with wrong data type.
        """
        self.build_tables()
        mock_asset = AssetVersion(
            asset=1,
            department="modeling",
            version=1,
            status="active"
        )
        db = DBManager()
        with pytest.raises((IntegrityError, ProgrammingError)):
            db.insert_assets([mock_asset])
        self.drop_tables()

    def test_insert_asset_version(self):
        """
        drop/rebuild tables and insert single asset version record.
        Assert that the id of the asset version is 1
        """
        self.build_tables()
        mock_asset = Asset(name="guy", type="character")
        db = DBManager()
        asset_ids = db.insert_assets([mock_asset])
        mock_asset_version = AssetVersion(
            asset=asset_ids[0],
            department="modeling",
            version=1,
            status="active"
        )
        ver_ids = db.insert_asset_versions([mock_asset_version])
        assert len(ver_ids) == 1
        assert ver_ids[0] == 1
        self.drop_tables()

    # TODO - need constraint test for assets (type)
    # TODO - need constraint tests for asset versions (asset, dept, version)


class TestRetrievals:
    @with_db_manager()
    def build_tables(self, mgr: DBManager):
        """clear tables and rebuild."""
        mgr.drop_table("asset_versions")
        mgr.drop_table("assets")
        mgr.create_asset_table()
        mgr.create_asset_version_table()

    @with_db_manager()
    def drop_tables(self, mgr: DBManager):
        mgr.drop_table("asset_versions")
        mgr.drop_table("assets")

    def test_list_assets(self):
        self.build_tables()
        mock_asset = Asset(name="guy", type="character")
        db = DBManager()
        db.insert_assets([mock_asset])
        assets = db.list_assets()
        assert len(assets) == 1
        assert assets[0]["name"] == "guy"
        assert assets[0]["type"] == "character"
        self.drop_tables()

    def test_list_asset_versions(self):
        self.build_tables()
        mock_asset = Asset(name="guy", type="character")
        db = DBManager()
        asset_ids = db.insert_assets([mock_asset])
        mock_asset_version = AssetVersion(
            asset=asset_ids[0],
            department="modeling",
            version=1,
            status="active"
        )
        db.insert_asset_versions([mock_asset_version])
        asset_versions = db.list_asset_versions()
        assert len(asset_versions) == 1
        assert asset_versions[0]["asset"] == asset_ids[0]
        assert asset_versions[0]["department"] == "modeling"
        assert asset_versions[0]["version"] == 1
        assert asset_versions[0]["status"] == "active"
        self.drop_tables()