import os
import pytest

from AssetServiceController.Model import JsonFile

class TestLoadAssets:
    goodDataFile = os.path.join(
        os.path.dirname(__file__),
        "..",
        "sample_data/asset_data"
    )

    badDataFile = os.path.join(
        os.path.dirname(__file__),
        "..",
        "sample_data/assetData"
    )

    def test_load_assets_valid_file(self):
        assert JsonFile(filePath=self.goodDataFile + ".json")
    
    def test_load_asset_ensure_json(self):
        json_file = JsonFile(filePath=self.goodDataFile + ".json")
        assert json_file.filePath.parts[-1].lower().endswith(".json")

    def test_load_assets_invalid_file(self):
        with pytest.raises(Exception):
            JsonFile(filePath=self.badDataFile + ".json")

    def test_load_assets_not_json_file(self):
        with pytest.raises(Exception):
            not_json_file = JsonFile(filePath=self.goodDataFile + ".txt")
            assert not_json_file.filePath.parts[-1].lower().endswith(".json")