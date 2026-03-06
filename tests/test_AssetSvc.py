import json
import os
import pytest
import tempfile

from AssetServiceController.api.Model import JsonFile
from AssetServiceController.api.AssetSvc import ensure_json_file

def create_mock_json_file(payload: list[dict], suffix=".json") -> str:
    """
    Create a temporary JSON file for testing.
    
    :param payload: <list[dict]> list of dictionaries to be written to JSON file.

    :returns: <str> file path of created JSON file.
    """

    with tempfile.NamedTemporaryFile(mode="w", suffix=suffix, delete=False) as json_file:
        json.dump(payload, json_file)
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
            ensure_json_file(json_file)
        except Exception:
            assert False, "ensure_json_file raised an exception for a valid JSON file."
        finally:
            os.remove(_file)

    def test_invalid_file(self):
        _file = "c:/this/file/doees/not/exist.json"
        with pytest.raises(Exception):
            JsonFile(filePath=_file)

    def test_not_json_file(self):
        wrong_file = create_mock_json_file(GOOD_DATA, suffix=".txt")
        not_json_file = JsonFile(filePath=wrong_file)
        with pytest.raises(Exception):
            ensure_json_file(not_json_file)
        os.remove(wrong_file)


class TestAssetInsertions:
    def test_insert_valid_asset(self):
        pass