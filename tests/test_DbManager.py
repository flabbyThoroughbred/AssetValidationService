import json
import pytest

from AssetServiceController.Errors import DatabaseError
from AssetServiceController.DbManager import DBManager
from AssetServiceController.Model import Asset, AssetVersion, AssetVersionJson


@pytest.mark.usefixtures("table_lifecycle")
class TestTableCreation:
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

    def test_ensure_table(self):
        db = DBManager()
        db.create_asset_table()
        try:
            db.ensure_table("assets")
        except ValueError:
            assert False, "ensure_table raised ValueError unexpectedly"


@pytest.mark.usefixtures("table_lifecycle")
class TestInsertions:
    def test_insert_assets(self):
        """
        drop/rebuild tables and insert single asset record.
        Assert that the id of the asset is 1.
        """
        mock_asset = Asset(name="guy", type="character")
        db = DBManager()
        asset_ids = db.insert_assets([mock_asset])
        # inserting same asset again should not raise, and should return same ID
        asset_ids2 = db.insert_assets([mock_asset])
        assert asset_ids == asset_ids2
        assert len(asset_ids) == 1
        assert asset_ids[0] == 1

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
        with pytest.raises(DatabaseError):
            db.insert_assets([mock_asset])
    
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
        # repeat insertion should return same id instead of throwing
        ver_ids2 = db.insert_asset_versions([mock_asset_version])
        assert ver_ids == ver_ids2
        assert len(ver_ids) == 1
        assert ver_ids[0] == 1

    def test_insert_asset_and_version(self, generic_asset):
        mock_asset = Asset(**generic_asset)
        mock_asset_version = AssetVersionJson(
            asset=generic_asset,
            department="modeling",
            version=1,
            status="active"
        )

        db = DBManager()
        ids = db.insert_asset_and_version(mock_asset, mock_asset_version)
        # inserting same asset/version pair again should simply return existing ids
        ids2 = db.insert_asset_and_version(mock_asset, mock_asset_version)
        assert ids == ids2
        assert ids["asset_id"] == 1 
        assert ids["asset_version_id"] == 1

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

    def test_insert_fails_duplicate(self):
        """re-inserting same failure should return the original id"""
        data = {
            "fail_data": json.dumps({
                "version": 2,
                "status": "inactive",
                "department": "modeling"
            }),
            "loc": "asset",
            "type": "missing_field",
            "msg": "Missing required field: asset"
        }
        db = DBManager()
        id1 = db.insert_fails(data)
        id2 = db.insert_fails(data)
        assert id1 == id2
    
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
        with pytest.raises(DatabaseError):
            db.insert_fails(data)


@pytest.mark.usefixtures("table_lifecycle")
class TestRetrievals:
    def test_list_assets(self):
        mock_asset = Asset(name="guy", type="character")
        db = DBManager()
        db.insert_assets([mock_asset])
        assets = db.list_all_assets()
        assert len(assets) == 1
        assert assets[0]["name"] == "guy"
        assert assets[0]["type"] == "character"

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

    def test_list_asset_versions_by_name_and_type_not_found(self):
        db = DBManager()
        # table should be empty...
        asset_versions = db.list_asset_versions("person", "character")
        assert asset_versions == []

    def test_retrieve_single_asset_version(self, good_data):
        db = DBManager()
        mock_asset = Asset(**good_data[0]["asset"])
        asset_ids = db.insert_assets([mock_asset])
        asset_versions = []
        for i in good_data:
            i["asset"] = asset_ids[0]
            asset_versions.append(AssetVersion(**i))
        db.insert_asset_versions(asset_versions)

        retrieved_item = db.retrieve_single_asset_version(
            asset_name="hero",
            asset_type="prop",
            version_num=3,
            department="modeling"
        )
        assert retrieved_item["version"] == 3

    def test_retrieve_single_asset_version_no_record(self):
        db = DBManager()
        retrieved_item = db.retrieve_single_asset_version(
            asset_name="hero",
            asset_type="prop",
            version_num=3,
            department="modeling"
        )
        assert retrieved_item is None