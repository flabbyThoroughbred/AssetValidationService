import json
import pytest
import tempfile

from AssetServiceController.DbManager import DBManager, with_db_manager

# =============================== DB Utils ====================================
@with_db_manager()
def build_tables(mgr: DBManager):
    """clear tables and rebuild."""
    mgr.drop_table("asset_versions")
    mgr.drop_table("assets")
    mgr.drop_table("fails")

    mgr.create_fails_table()
    mgr.create_asset_table()
    mgr.create_asset_version_table()


@with_db_manager()
def drop_tables(mgr: DBManager):
    mgr.drop_table("asset_versions")
    mgr.drop_table("assets")
    mgr.drop_table("fails")


# ================================ Fixtures ===================================
@pytest.fixture
def table_lifecycle():
    """
    Builds tables before each test and drop them after test completion.
    """
    build_tables()
    try:
        yield
    finally:
        drop_tables()


@pytest.fixture
def create_mock_json_file(tmp_path):
    """
    Create a temporary in-memory JSON file for testing.
    
    :param payload: <list[dict]> list of dictionaries to be written to JSON file.
    :param suffix: <str> the file type.
    
    :returns: <str> file path of created JSON file.
    """
    def _create(payload: list[dict], suffix=".json") -> str:
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=suffix,
            dir=tmp_path,
            delete=False,
            encoding="utf-8"
        ) as json_file:
            json.dump(payload, json_file)
            return json_file.name
    return _create


@pytest.fixture
def create_mock_invalid_json_file(tmp_path):
    def _create(suffix=".json") -> str:
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=suffix,
            dir=tmp_path,
            delete=False,
            encoding="utf-8"
        ) as json_file:
            json_file.write('{"invalid": data}')
            return json_file.name
    return _create


@pytest.fixture
def good_data():
    return [{
            "asset": {
            "name": "hero",
            "type": "prop"
            },
            "department": "modeling",
            "version": 1,
            "status": "inactive"
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


@pytest.fixture
def bad_data():
    return [{
            "asset": {
            "name": "hero",
            "type": "porp"
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
            "department": "yodeling",
            "version": 2,
            "status": "active"
        },
        {
            "asset": {
            "name": "steve",
            "type": "character"
            },
            "department": "animation",
            "version": 2,
            "status": "active"
        },
        {
            "asset": {
            "name": "hero",
            "type": "prop"
            },
            "department": "modeling",
            "version": -3,
            "status": "active"
        }]

@pytest.fixture
def generic_asset():
    return {"name": "guy", "type": "character"}