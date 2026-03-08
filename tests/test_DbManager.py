from functools import wraps
import json
import pytest
from sqlite3 import IntegrityError, ProgrammingError

from AssetServiceController.api.Model import Asset, AssetVersion, AssetVersionJson

from utils import drop_tables, with_table_lifecycle, DBManager

"""
for simplicity as well as the fact that sqlite is lightweight file-based
database that can be easily created/dropped, I'm going to test
with an actual database instead of mocks. In a production environment,
I'd likely go with SQLAlchemy or SQLModel with a PostgreSQL database
in which case I'd probably use mocks for testing.
"""

class TestTableCreation:
    @with_table_lifecycle()
    def test_create_asset_table(self):
        db = DBManager()
        db.drop_table("assets")
        db.create_asset_table()
        cursor = db.session.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='table' and name='assets';
            """
        )
        tables = [t["name"] for t in cursor.fetchall()]
        assert "assets" in tables

    @with_table_lifecycle()
    def test_create_asset_version_table(self):
        db = DBManager()
        db.drop_table("asset_versions")
        db.create_asset_version_table()
        cursor = db.session.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='table' and name='asset_versions';
            """
        )
        tables = [t["name"] for t in cursor.fetchall()]
        assert "asset_versions" in tables

    def test_ensure_table_nonexistent(self):
        db = DBManager()
        # make sure asset table doesn't exist
        drop_tables()
        with pytest.raises(ValueError):
            db.ensure_table("assets")

    def test_ensure_table(self):
        drop_tables()
        db = DBManager()
        db.create_asset_table()
        try:
            db.ensure_table("assets")
        except ValueError:
            assert False, "ensure_table raised ValueError unexpectedly"


class TestInsertions:
    @with_table_lifecycle()
    def test_insert_assets(self):
        """
        drop/rebuild tables and insert single asset record.
        Assert that the id of the asset is 1.
        """
        mock_asset = Asset(name="guy", type="character")
        db = DBManager()
        asset_ids = db.insert_assets([mock_asset])
        assert len(asset_ids) == 1
        assert asset_ids[0] == 1

    @with_table_lifecycle()
    def test_insert_assets_fail(self):
        """
        Test insertion with wrong data type.
        """
        mock_asset = AssetVersion(
            asset=1,
            department="modeling",
            version=1,
            status="active"
        )
        db = DBManager()
        with pytest.raises((IntegrityError, ProgrammingError)):
            db.insert_assets([mock_asset])
    
    @with_table_lifecycle()
    def test_insert_asset_versions(self):
        """
        drop/rebuild tables and insert single asset version record.
        Assert that the id of the asset version is 1
        """
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

    @with_table_lifecycle()
    def test_insert_asset_and_version(self):
        _asset = {"name": "guy", "type": "character"}
        mock_asset = Asset(**_asset)
        mock_asset_version = AssetVersionJson(
            asset=_asset,
            department="modeling",
            version=1,
            status="active"
        )

        db = DBManager()
        ids = db.insert_asset_and_version(mock_asset, mock_asset_version)
        assert ids["asset_id"] == 1 
        assert ids["asset_version_id"] == 1

    @with_table_lifecycle()
    def test_insert_fails(self):
        
        data = {
            "fail_data": json.dumps({
                "version": 1,
                "status": "active",
                "department": "modeling"
            }),
            "loc": "asset",
            "type": "missing_field",
            "msg": "Missing required field: asset"
        }

        db = DBManager()
        id = db.insert_fails(data)
        assert id == 1
    
    @with_table_lifecycle()
    def test_insert_fails_bad_payload(self):
        """
        the 'fail_data' column expects JSON.
        """
        
        data = {
            "fail_data": {
                "version": 1,
                "status": "active",
                "department": "modeling"
            },
            "loc": "asset",
            "type": "missing_field",
            "msg": "Missing required field: asset"
        }

        db = DBManager()
        with pytest.raises(ProgrammingError):
            db.insert_fails(data)


class TestRetrievals:
    @with_table_lifecycle()
    def test_list_assets(self):
        mock_asset = Asset(name="guy", type="character")
        db = DBManager()
        db.insert_assets([mock_asset])
        assets = db.list_all_assets()
        assert len(assets) == 1
        assert assets[0]["name"] == "guy"
        assert assets[0]["type"] == "character"

    @with_table_lifecycle()
    def test_list_asset_versions(self):
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
        asset_versions = db.list_all_asset_versions()
        assert len(asset_versions) == 1
        assert asset_versions[0]["asset"] == asset_ids[0]
        assert asset_versions[0]["department"] == "modeling"
        assert asset_versions[0]["version"] == 1
        assert asset_versions[0]["status"] == "active"

    @with_table_lifecycle()
    def test_retrieve_asset_by_name_and_type(self):
        asset_name = "Spatula"
        asset_type = "prop"
        mock_assets = [
            Asset(name=asset_name, type=asset_type)
        ]
        db = DBManager()
        db.insert_assets(mock_assets)

        assets = db.retrieve_single_asset(asset_name, asset_type)
        assert assets
        assert assets["name"] == asset_name
        assert assets["type"] == asset_type

    @with_table_lifecycle()
    def test_list_asset_by_name_and_type_not_found(self):
        asset_name = "Spatula"
        asset_type = "prop"
        mock_assets = [
            Asset(name=asset_name, type=asset_type)
        ]
        db = DBManager()
        db.insert_assets(mock_assets)

        assets = db.retrieve_single_asset(asset_name, "character")
        assert assets is None
        
    @with_table_lifecycle()
    def test_list_asset_versions_by_name_and_type(self):
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

        asset_versions = db.list_asset_versions(
            mock_asset.name,
            mock_asset.type
        )

        assert len(asset_versions) == 1
        assert asset_versions[0]["asset"] == asset_ids[0]
        assert asset_versions[0]["department"] == "modeling"
        assert asset_versions[0]["version"] == 1
        assert asset_versions[0]["status"] == "active"

    @with_table_lifecycle()
    def test_list_asset_versions_by_name_and_type_not_found(self):
        db = DBManager()
        # table should be empty...
        asset_versions = db.list_asset_versions("person", "character")
        assert asset_versions == []

    @with_table_lifecycle()
    def test_retrieve_single_asset_version(self):
        mock_data =  [{
            "asset": {
            "name": "hero",
            "type": "prop"
            },
            "department": "modeling",
            "version": 1,
            "status": "active"
        },
        {
            "asset": {
            "name": "hero",
            "type": "prop"
            },
            "department": "modeling",
            "version": 2,
            "status": "active"
        },
        {
            "asset": {
            "name": "hero",
            "type": "prop"
            },
            "department": "modeling",
            "version": 3,
            "status": "active"
        }]

        db = DBManager()
        mock_asset = Asset(**mock_data[0]["asset"])
        asset_ids = db.insert_assets([mock_asset])
        asset_versions = []
        for i in mock_data:
            i["asset"] = asset_ids[0]
            asset_versions.append(AssetVersion(**i))
        db.insert_asset_versions(asset_versions)

        retrieved_item = db.retrieve_single_asset_version(
            asset_name="hero",
            asset_type="prop",
            version_num=3
        )

        assert retrieved_item["version"] == 3


    @with_table_lifecycle()
    def test_retrieve_single_asset_version_no_record(self):
        db = DBManager()
        retrieved_item = db.retrieve_single_asset_version(
            asset_name="hero",
            asset_type="prop",
            version_num=3
        )

        assert retrieved_item is None