import json
from pydantic import ValidationError

from .Logger import create_logger
logger = create_logger("AssetSvc")

from . import Model as m
from .DbManager import DBManager, with_db_manager

def validationHandler(err: ValidationError, dbMgr: DBManager, **fail_data) -> dict:
    logger.error(
        "OH no! You've encountered an error. "
        f"It has been logged for further investigation. {err}"
    )
    if type(err) == ValidationError:
        err_data = err.errors()[0]
        dbMgr.insert_fails({
            "fail_data": json.dumps(fail_data),
            "loc": ".".join(err_data["loc"]),
            "type": err_data["type"],
            "msg": err_data["msg"]
        })
    else:
        dbMgr.insert_fails({
            "fail_data": json.dumps(fail_data),
            "loc": None,
            "type": None,
            "msg": str(err)
        })


"""
Directly accessible from other APIs. Higher-level. Just pass in your data
and away you go. In theory at least.
"""

def ensure_json_file(dataFile: m.JsonFile) -> None:
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
    try:
        json_file = m.JsonFile(filePath=dataFile)
        return _load_assets(json_file)
    except ValidationError as e:
        return [{"err": f"{dataFile} does not exist."}]



def _load_assets(dataFile: m.JsonFile) -> dict:
    """
    Internal function to load json file as dict.

    :param dataFile: <JsonFile> must be a .json file

    :returns: <dict> dictionary object loaded from json
    """
    ensure_json_file(dataFile)
    try:
        with open(dataFile.filePath, "r") as f:
            return json.load(f)
    except (Exception) as e:
        return [{"err": f"An error was encountered when opening this file: {dataFile.filePath}"}]
        

@with_db_manager()
def batch_ingest_data(dataFile: str, mgr: DBManager) -> None:
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
    loaded_data = load_assets(dataFile)

    for item in loaded_data:
        if err:= item.get("err"):
            logger.error(err)
            return
        try:
            asset = m.Asset(**item["asset"])
            asset_version = m.AssetVersionJson(**item)
            ids = mgr.insert_asset_and_version(
                asset,
                asset_version,
                defer_commit=True
            )
            mgr.logger.info(ids)
        except Exception as e:
            validationHandler(e, dbMgr=mgr, item=item)
    mgr.session.commit()


@with_db_manager()
def add_asset(asset_name: str, asset_type: str, mgr: DBManager) -> int:
    """
    Add single asset to database.

    :param asset_name: <str> name of asset to be added.
    :param asset_type: <str> type of asset to be added.
    :param mgr: <DBManager> implicit inclusion by the decorator. Provide
    database operations.

    :returns: <int> id of inserted asset record. If asset already exists,
    return id of existing record.
    """
    try:
        ids = mgr.insert_assets([m.Asset(name=asset_name, type=asset_type)])
        return ids[0]
    except Exception as e:
        validationHandler(e, dbMgr=mgr, asset_name=asset_name, asset_type=asset_type)


@with_db_manager()
def _add_asset_version(asset_version: dict, mgr: DBManager) -> int:
    """
    <internal>
    Add single asset version to database. This assumes the asset
    already exists! the asset field must be the id of an existing
    asseet record.

    :param assetVersionData: <dict> asset version to be added. Assumes
    asset is already validated (is of type asset).

    :param mgr: <DBManager> implicit inclusion by the decorator. Provide
    database operations.

    :returns: <int> id of inserted asset version record. If asset version
    already exists, return id of existing record.
    """
    try:
        ids = mgr.insert_asset_versions([m.AssetVersion(**asset_version)])
        return ids[0]
    except Exception as e:
        validationHandler(e, dbMgr=mgr, asset_verion=asset_version)


@with_db_manager()
def add_asset_version(asset_version: dict, mgr: DBManager) -> int:
    """
    <User-facing>
    Accepts single asset version payload provided:
        - asset is a field that conforms to an asset payload. Otherwise
        an error is thrown.
    
    :param asset_version: <dict> the complete json representation of the
    asset version complete with full asset description.
    :param mgr: <DBManager> implicit inclusion by the decorator. Provide
    database operations.
    
    :returns: <int> the id of the asset_version created.
    """
    try:
        asset_version = m.AssetVersionJson(**asset_version)
        ids = mgr.insert_asset_and_version(
            asset_version.asset,
            asset_version
        )
        return ids["asset_version_id"]
    except Exception as e:
        validationHandler(e, dbMgr=mgr, asset_version=asset_version)


@with_db_manager()
def add_asset_and_version(asset: dict, asset_version: dict, mgr: DBManager) -> int:
    """
    <User-facing>
    Accepts both asset and version provided:
        - asset is field in asset_version and conforms to the asset payload.
    
    :param asset: <dict> an asset data entity.
    :param asset_version: <dict> the complete json representation of the
    asset version complete with full asset entity description.
    :param mgr: <DBManager> implicit inclusion by the decorator. Provide
    database operations.
    
    :returns: <int> the id of the asset_version created.

    # NOTE - when validating the asset version no assupmtion is made 
    as to whether the asset field exists or is the correct data type.
    """
    try:
        asset = m.Asset(**asset)
        asset_version = m.AssetVersionLite(**asset_version)
        ids = mgr.insert_asset_and_version(asset, asset_version)
        return ids["asset_version_id"]
    except Exception as e:
        validationHandler(e, dbMgr=mgr, asset=asset, asset_version=asset_version)


@with_db_manager()
def list_assets(mgr: DBManager) -> tuple[list[dict]|list]:
    """
    Returns list of all existing assets.

    :param mgr: <DBManager> implicit inclusion by the decorator. Provide
    database operations.

    :returns: list[<dict>] asset records.
    """
    return mgr.list_all_assets()


@with_db_manager()
def get_asset(asset_name: str, asset_type: str,
    mgr: DBManager) -> tuple[dict|None]:
    """
    Get asset record by name and type.

    :param asset_name: <str> name of asset to retrieve.
    :param asset_type: <AssetType> type of asset to retrieve. Must conform
    to type AssetType.
    :param mgr: <DBManager> implicit inclusion by the decorator. Provide
    database operations.

    :returns: <dict> asset record matching name and type.
    """
    try:
        # validate type first
        _type = m.AssetType(asset_type)
    except ValidationError as e:
        logger.error(e)

    return mgr.retrieve_single_asset(asset_name, asset_type)


@with_db_manager()
def get_asset_version(asset_name: int, asset_type: str,
    version_num: int, mgr: DBManager) -> tuple[dict|None]:
    """
    Return a single asset version record by asset id, asset type and version.

    :param asset_name: <str> name of asset to retrieve.
    :param asset_type: <AssetType> type of asset to retrieve version for.
    Must conform to type AssetType.
    :param version: <str> version of asset version to retrieve.
    :param mgr: <DBManager> implicit inclusion by the decorator. Provide
    database operations.

    :returns: <dict> asset version record matching the given attributes.
    """
    
    try:
        # validate type first
        _type = m.AssetType(asset_type)
    except ValidationError as e:
        logger.error(e)

    return mgr.retrieve_single_asset_version(
        asset_name,
        asset_type,
        version_num
    )


@with_db_manager()
def list_asset_versions(asset_name: str, asset_type: m.AssetType,
mgr: DBManager) -> tuple[list[dict]|list]:
    """
    Return array of asset version records matching the given asset name and type.

    :param asset_name: <str> name of asset to retrieve versions for.
    :param asset_type: <AssetType> type of asset to retrieve versions for.
    Must conform to type AssetType.
    :param mgr: <DBManager> implicit inclusion by the decorator. Provide
    database operations.

    :returns: list[<dict>] asset version records matching the given attributes.
    """
    try:
        # validate type first
        _type = m.AssetType(asset_type)
    except ValidationError as e:
        logger.error(e)

    return mgr.list_asset_versions(asset_name, asset_type)
