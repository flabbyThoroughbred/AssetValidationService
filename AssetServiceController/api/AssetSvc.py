import json

from .Model import JsonFile, Asset, AssetVersionJson, AssetVersion, AssetType
from .DbManager import DBManager, with_db_manager

"""
Directly accessible from other APIs. Higher-level. Just pass in your data
and away you go. In theory at least.
"""

def ensure_json_file(dataFile: JsonFile) -> None:
    """
    Ensure the the given file is a .json file.
    Raise an exception if not.

    :param dataFile: <JsonFile> file to check.

    :returns: None
    """

    if not dataFile.filePath.parts[-1].lower().endswith(".json"):        
        raise Exception(f"File {dataFile.filePath} is not a .json file.")


def load_assets(dataFile: str) -> dict:
    """
    Take an input file <json> and load as 
    dictionary.

    :param dataFile: <JsonFile> must be a .json file

    :returns: <dict> dictionary object loaded from json
    """
    json_file = JsonFile(filePath=dataFile)
    _load_assets(json_file)


def _load_assets(dataFile: JsonFile) -> dict:
    """
    Internal function to load json file as dict.

    :param dataFile: <JsonFile> must be a .json file

    :returns: <dict> dictionary object loaded from json
    """
    ensure_json_file(dataFile)
    with open(dataFile.filePath, "r") as f:
        return json.load(f)


def batch_ingest_data(dataFile: str) -> None:
    """
    Take a json file array of assets/assetVersions and add to database.
    Type models validate by checking if each individual item is either
    an Asset or AssetVersion. If AssetVersion, validates its asset field
    as an Asset type...etc.
    
    steps:
    - load json as dict
    - iterate through items
        - validate through type models
        - if asset, add to asset insertion set
        - if assetVersion, add to assetVersion insertion set
        - if either is invalid, add neither to set
        - if not asset (already has an asset id) add to different assetVersion set.
    - at this point there should be two equal arrays of pre-insertion asset and asset version data.
    - batch add assets, the asset ids should be aligned with the assetVersion count.

    """
    pass


@with_db_manager()
def add_asset(asset_name: str, asset_type: str, mgr: DBManager) -> int:
    """
    Add single asset to database.

    :param asset_name: <str> name of asset to be added.
    :param asset_type: <str> type of asset to be added.

    :returns: <int> id of inserted asset record. If asset already exists,
    return id of existing record.
    """
    ids = mgr.insert_assets([Asset(name=asset_name, type=asset_type)])
    return ids[0]

@with_db_manager()
def add_asset_version(assetVersionData: dict, mgr: DBManager) -> int:
    """
    Add single asset version to database.

    :param assetVersion: <AssetVersion> asset version to be added.
    Must conform to type AssetVersion.

    :returns: <int> id of inserted asset version record. If asset version
    already exists, return id of existing record.
    """
    pass


def get_asset(asset_name: str, asset_type: AssetType) -> dict:
    """
    Get asset record by name and type.

    :param asset_name: <str> name of asset to retrieve.
    :param asset_type: <AssetType> type of asset to retrieve. Must conform
    to type AssetType.

    :returns: <dict> asset record matching name and type.
    """
    pass


def get_asset_version(asset_id: int, asset_type: AssetType, version: str) -> dict:
    """
    Return a single asset version record by asset id, asset type and version.

    :param asset_id: <int> id of asset to retrieve version for.
    :param asset_type: <AssetType> type of asset to retrieve version for.
    Must conform to type AssetType.
    :param version: <str> version of asset version to retrieve.

    :returns: <dict> asset version record matching the given attributes.
    """
    pass


def list_assets() -> list[dict]:
    """
    :returns: list[<dict>] asset records.
    """
    pass


def list_versions_by_asset(asset_name: str, asset_type: AssetType) -> list[dict]:
    """
    Return array of asset version records matching the given asset name and type.

    :param asset_name: <str> name of asset to retrieve versions for.
    :param asset_type: <AssetType> type of asset to retrieve versions for.
    Must conform to type AssetType.

    :returns: list[<dict>] asset version records matching the given attributes.
    """
    pass
