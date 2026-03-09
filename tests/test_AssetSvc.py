import json
import os
import pytest
import tempfile
from pydantic import ValidationError
from unittest.mock import patch, mock_open

from AssetServiceController.Model import JsonFile
import AssetServiceController.AssetSvc as assetSvc

from utils import with_table_lifecycle

def create_mock_json_file(payload: list[dict], suffix=".json") -> str:
    """
    Create a temporary in-memory JSON file for testing.
    
    :param payload: <list[dict]> list of dictionaries to be written to JSON file.
    :param suffix: <str> the file type.
    
    :returns: <str> file path of created JSON file.
    """

    with tempfile.NamedTemporaryFile(mode="w", suffix=suffix, delete=False) as json_file:
        json.dump(payload, json_file)
        return json_file.name
    

def create_mock_invalid_json_file(suffix=".json") -> str:
    """
    Create a temporary file with invalid JSON content for testing failure cases.
    
    :param suffix: <str> the file type.
    
    :returns: <str> file path of created invalid JSON file.
    """
    with tempfile.NamedTemporaryFile(mode="w", suffix=suffix, delete=False) as json_file:
        json_file.write('{"invalid": json syntax}')  # Invalid JSON
        return json_file.name
    

GOOD_DATA = [
  {
    "asset": {
      "name": "hero",
      "type": "character"
    },
    "department": "modeling",
    "version": 1,
    "status": "active"
  },
  {
    "asset": {
      "name": "hero",
      "type": "character"
    },
    "department": "modeling",
    "version": 2,
    "status": "active"
  },
  {
    "asset": {
      "name": "hero",
      "type": "fx"
    },
    "department": "texturing",
    "version": 1,
    "status": "active"
  }
]

class TestLoadAssets:
    def test_valid_json_file(self):
        json_file = create_mock_json_file(GOOD_DATA)
        assert JsonFile(filePath=json_file)
        os.remove(json_file)
    
    def test_ensure_json(self):
        _file = create_mock_json_file(GOOD_DATA)
        json_file = JsonFile(filePath=_file)
        try:
            assetSvc.ensure_json_file(json_file)
        except Exception:
            assert False, "ensure_json_file raised an exception for a valid JSON file."
        finally:
            os.remove(_file)

    def test_missing_file(self):
        _file = "c:/this/file/does/not/exist.json"
        with pytest.raises(Exception):
            JsonFile(filePath=_file)

    def test_not_json_file(self):
        wrong_file = create_mock_json_file(GOOD_DATA, suffix=".txt")
        not_json_file = JsonFile(filePath=wrong_file)
        with pytest.raises(Exception):
            assetSvc.ensure_json_file(not_json_file)
        os.remove(wrong_file)

    def test_load_assets_valid(self):
        _file = create_mock_json_file(GOOD_DATA)
        data = assetSvc.load_assets(_file)
        assert data is not None
        os.remove(_file)

    def test_load_assets_invalid_json(self):
        """Test that loading an invalid JSON file raises an exception."""
        invalid_file_path = create_mock_invalid_json_file()
        assert assetSvc.load_assets(invalid_file_path) is None
        os.remove(invalid_file_path)
    
    def test_helper_load_assets_valid(self):
        pass

    def test_helper_load_assets_invalid(self):
        pass


class TestAssetInsertions:
    @with_table_lifecycle()
    def test_insert_valid_asset(self):
        assert assetSvc.add_asset("ford pinto", "vehicle") is not None

    @with_table_lifecycle()
    def test_insert_invalid_asset(self):
        assert assetSvc.add_asset("salmon", "fish") is None
    
    @with_table_lifecycle()
    def test_insert_missing_attribute(self):
        with pytest.raises(TypeError):
            assetSvc.add_asset("prop")

class TestAssetVersionInsertions:
    @with_table_lifecycle()
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
        av_id = assetSvc.add_asset_version(new_asset_version)
        assert av_id == 1
    
    @with_table_lifecycle()
    def test_missing_attribute_insert_asset_version(self):
        new_asset_version = {
            "department": "modeling",
            "version": 1,
            "status": "inactive"
        }
        
        assert assetSvc.add_asset_version(new_asset_version) is None

    @with_table_lifecycle()
    def test_valid_insert_asset_and_version(self):
        asset = {"name": "steve", "type": "prop"}
        new_asset_version =   {
            "department": "modeling",
            "version": 2,
            "status": "active"
        }
        av_id = assetSvc.add_asset_and_version(asset, new_asset_version)
        assert av_id == 1

#     @with_table_lifecycle()
#     def test_batch_ingest_data(self):
#         """
#         TODO: need mock json file.
#         """
#         pass


# class TestServiceRetrievals:
#     @with_table_lifecycle()
#     def test_list_assets(self):
#         pass

#     @with_table_lifecycle()
#     def test_list_assets_no_records(self):
#         pass

#     @with_table_lifecycle()
#     def test_get_asset(self):
#         pass

#     @with_table_lifecycle()
#     def test_get_asset_no_record(self):
#         pass

#     @with_table_lifecycle()
#     def test_get_asset_version(self):
#         # 1) make mock asset/asset versions (at least 3 or 4)
#         # 2) call assetSvc.get_asset_version with existing attributes to search
#         pass

#     @with_table_lifecycle()
#     def test_get_asset_version_no_record(self):
#         # 1) make mock asset/asset versions (at least 3 or 4)
#         # 2) call assetSvc.get_asset_version with existing attributes to search
#         pass

#     @with_table_lifecycle()
#     def test_list_asset_versions(self):
#         pass

#     @with_table_lifecycle()
#     def test_list_asset_versions_no_records(self):
#         pass