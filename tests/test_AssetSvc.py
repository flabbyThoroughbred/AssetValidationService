import os
import pytest

from AssetServiceController.Model import JsonFile
import AssetServiceController.AssetSvc as assetSvc

class TestLoadAssets:
    def test_valid_json_file(self, create_mock_json_file, good_data):
        json_file = create_mock_json_file(good_data)
        assert JsonFile(filePath=json_file)
    
    def test_ensure_json(self, create_mock_json_file, good_data):
        _file = create_mock_json_file(good_data)
        json_file = JsonFile(filePath=_file)
        try:
            assetSvc.ensure_json_file(json_file)
        except Exception:
            assert False, "ensure_json_file raised an exception for a valid JSON file."

    def test_missing_file(self):
        _file = "c:/this/file/does/not/exist.json"
        with pytest.raises(Exception):
            JsonFile(filePath=_file)

    def test_not_json_file(self, create_mock_json_file, good_data):
        wrong_file = create_mock_json_file(good_data, suffix=".txt")
        not_json_file = JsonFile(filePath=wrong_file)
        with pytest.raises(Exception):
            assetSvc.ensure_json_file(not_json_file)

    def test_load_assets_valid(self, create_mock_json_file, good_data):
        _file = create_mock_json_file(good_data)
        data = assetSvc.load_assets(_file)
        assert data is not None
        os.remove(_file)

    def test_load_assets_invalid_json(self, create_mock_invalid_json_file):
        """Test that loading an invalid JSON file raises an exception."""
        invalid_file_path = create_mock_invalid_json_file()
        assert assetSvc.load_assets(invalid_file_path) is None
        os.remove(invalid_file_path)


@pytest.mark.usefixtures("table_lifecycle")
class TestAssetInsertions:
    def test_insert_valid_asset(self):
        assert assetSvc.add_asset("ford pinto", "vehicle") is not None

    def test_insert_invalid_asset(self):
        assert assetSvc.add_asset("salmon", "fish") is None
    
    def test_insert_missing_attribute(self):
        with pytest.raises(TypeError):
            assetSvc.add_asset("prop")



@pytest.mark.usefixtures("table_lifecycle")
class TestAssetVersionInsertions:
    def test_valid_insert_public_asset_version(self):
        av_id = assetSvc.add_asset_version(
            asset_name="steve",
            asset_type="character",
            department="rigging",
            version_num=1,
            status="active"
        )

        assert av_id == 1

    def test_valid_insert_asset_version(self):
        new_asset_version =   {
            "asset": {
                "name": "hero",
                "type": "character"
            },
            "department": "modeling",
            "version": 2,
            "status": "active"
        }
        av_id = assetSvc._add_asset_version(new_asset_version)
        assert av_id == 1
    
    def test_missing_attribute_insert_asset_version(self):
        new_asset_version = {
            "department": "modeling",
            "version": 1,
            "status": "inactive"
        }
        assert assetSvc._add_asset_version(new_asset_version) is None

    def test_valid_insert_asset_and_version(self):
        asset = {"name": "steve", "type": "prop"}
        new_asset_version =   {
            "department": "modeling",
            "version": 2,
            "status": "active"
        }
        av_id = assetSvc.add_asset_and_version(asset, new_asset_version)
        assert av_id == 1

    def test_batch_ingest_data(self, create_mock_json_file, good_data):
        json_file = create_mock_json_file(good_data)
        was_run = None
        try:
            was_run = assetSvc.batch_ingest_data(json_file)  
        except Exception:
            assert False, "batch_ingest_data raised an exception with a valid json file."
        assert was_run is not None
        
    def test_batch_ingest_data_invalid_assets(self, create_mock_json_file, bad_data):
        """
        Json file contains invalid assets/asset versions but should not fail.
        """
        json_file = create_mock_json_file(bad_data)
        was_run = None
        try:
            was_run = assetSvc.batch_ingest_data(json_file)  
        except Exception:
            assert False, "batch_ingest_data raised an exception with a valid json file."
        assert was_run is not None

    def test_batch_ingest_data_invalid_json_file(self, create_mock_invalid_json_file):
        json_file = create_mock_invalid_json_file()
        was_run = None
        try:
            was_run = assetSvc.batch_ingest_data(json_file)  
        except Exception:
            assert False, "batch_ingest_data raised an exception with an invalid json file."
        assert was_run is None


@pytest.mark.usefixtures("table_lifecycle")
class TestServiceRetrievals:
    def test_list_assets(self):
        # add an asset
        assetSvc.add_asset("steve", "character")
        assert len(assetSvc.list_assets()) > 0

    def test_list_assets_no_records(self):
        assert len(assetSvc.list_assets()) == 0

    def test_get_asset(self):
        name = "steve"
        _type = "character"
        assetSvc.add_asset(name, _type)
        assets = assetSvc.get_assets(name, _type)
        assert len(assets) == 1
        assert assets[0]["name"] == name
        assert assets[0]["type"] == _type

    def test_get_asset_no_record(self):
        assets = assetSvc.get_assets("brick", "prop")
        assert len(assets) == 0

    def test_get_asset_invalid_type(self):
        assets = assetSvc.add_asset("brick", "prop")
        assert len(assetSvc.get_assets("brick", "porp")) == 0

    def test_get_asset_version(self, create_mock_json_file, good_data):
        assetSvc.batch_ingest_data(create_mock_json_file(good_data))
        asset_version = assetSvc.get_asset_version(
            asset_name="hero",
            asset_type="prop",
            department="modeling",
            version_num=1
        )
        assert asset_version is not None
        assert asset_version["department"] == "modeling"
        assert asset_version["version"] == 1

    def test_get_asset_version_no_record(self):
        asset_version = assetSvc.get_asset_version(
            "chuck",
            "character",
            "modeling",
            1
        )

        assert asset_version is None

    def test_list_asset_versions(self, create_mock_json_file, good_data):
        assetSvc.batch_ingest_data(create_mock_json_file(good_data))
        asset_versions = assetSvc.list_asset_versions(
            asset_name="hero",
            asset_type="prop"
        )

        assert len(asset_versions) == 3 # good_data has 3 records by these values

    def test_list_asset_versions_optional_version(self, create_mock_json_file, good_data):
        assetSvc.batch_ingest_data(create_mock_json_file(good_data))
        asset_versions = assetSvc.list_asset_versions(
            asset_name="hero",
            asset_type="prop",
            version_num=3
        )
        assert len(asset_versions) == 1 # good_data has 1 record with "hero", "prop" and version=3

    def test_list_asset_versions_optional_status(self, create_mock_json_file, good_data):
        assetSvc.batch_ingest_data(create_mock_json_file(good_data))
        asset_versions = assetSvc.list_asset_versions(
            asset_name="hero",
            asset_type="prop",
            status="active"
        )
        assert len(asset_versions) == 2 # good_data has 1 record with "hero", "prop" and status=inactive

    def test_list_asset_versions_no_records(self):
        asset_versions = assetSvc.list_asset_versions(
            asset_name="hero",
            asset_type="prop",
            version_num=3
        )
        assert len(asset_versions) == 0