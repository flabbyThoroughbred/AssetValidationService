import json
from pydantic import ValidationError

from .Logger import create_logger
logger = create_logger("AssetSvc")

from .DbManager import DBManager, with_db_manager
from .Errors import DatabaseError
from . import Model as m

# ========================== Demonstration Utilities ==========================
@with_db_manager()
def _build_tables(mgr: DBManager):
    """
    Create simple demonstration databases.
    """
    mgr.create_tables()


@with_db_manager()
def _drop_tables(mgr: DBManager):
    """
    Drops demonstration databases.
    """
    mgr.drop_table("fails")
    mgr.drop_table("asset_versions")
    mgr.drop_table("assets")
# =============================================================================


def _validationHandler(err: ValidationError, dbMgr: DBManager, **fail_data) -> dict:
    """
    Capture pydantic validation errors, store the failed data in the fails 
    table and log the error.

    :param err: <ValidationError> the pydantic validation error
    :param dbMrg: <DBManager> database manager
    :kwargs: failed data dictionary.

    :returns: None

    """
    if type(err) == ValidationError:
        err_data = err.errors()[0]
        loc = ".".join(err_data["loc"])
        _type = err_data["type"]
        msg = err_data["msg"]

        logger.error(
            "You've encountered a data validation error! "
            f"{loc} - {msg}."
        )
        dbMgr.insert_fails({
            "fail_data": json.dumps(fail_data),
            "loc": loc,
            "type": _type,
            "msg": msg
        })
    else:
        dbMgr.insert_fails({
            "fail_data": json.dumps(fail_data),
            "loc": None,
            "type": None,
            "msg": str(err)
        })


def ensure_json_file(dataFile: m.JsonFile) -> None:
    """
    Ensure the the given file is a .json file.
    Raise an exception if not.

    :param dataFile: <JsonFile> file to check.

    :returns: None
    """

    if not dataFile.filePath.parts[-1].lower().endswith(".json"):        
        raise OSError(f"File {dataFile.filePath} is not a .json file.")


def _load_assets(dataFile: m.JsonFile) -> dict:
    """
    Internal function to load json file as dict.

    :param dataFile: <JsonFile> must be a .json file

    :returns: <dict> dictionary object loaded from json
    """

    with open(dataFile.filePath, "r") as f:
        loaded = json.load(f)
        if loaded is None:
            return {}
        else:
            return loaded

# =============================================================================
# =============================================================================
def load_assets(dataFile: str) -> dict:
    """
    Take an input file <json> and load as 
    dictionary.

    :param dataFile: <JsonFile> must be a .json file

    :returns: <dict> dictionary object loaded from json
    """
    try:
        json_file = m.JsonFile(filePath=dataFile)
        ensure_json_file(json_file)
        return _load_assets(json_file)
    except ValidationError:
        # pydantic validation error
        logger.error(f"Could not load {dataFile} -- Does not exist.")
    except OSError as e:
        # not a json filetype
        logger.error(e)
    except json.JSONDecodeError:
        # json file error
        logger.error(f"Invalid file [{dataFile}] -- Could not load.")
    return None


@with_db_manager()
def batch_ingest_data(dataFile: str, mgr: DBManager) -> None:
    """
    Given a valid json file, ingest assets and asset version:
        - validate the data against a data model.
        - insert into database.
        - log errors while continuing to process.
        
    :param mgr: <DBManager> implicit inclusion by the decorator. Provide
    database operations.    
    :param dataFile: <str> a json data file of assets and asset versions.

    :returns: <list[dict]> ids of all paired assets and asset version.
    """
    loaded_data = load_assets(dataFile)
    if loaded_data is None:
        return
    err_encountered = False
    ids = []
    for item in loaded_data:
        if err:= item.get("err"):
            logger.error(err)
            return
        try:
            asset = m.Asset(**item["asset"])
            asset_version = m.AssetVersionJson(**item)
            _ids = mgr.insert_asset_and_version(
                asset,
                asset_version,
                defer_commit=True
            )
            ids.append(_ids)
        except ValidationError as e:
            err_encountered = True
            _validationHandler(e, dbMgr=mgr, item=item)

    mgr.session.commit()
    if err_encountered:
        logger.error(
            "There were problems with this ingest. Please see prior logs."
        )
    
    return ids


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
    except ValidationError as e:
        _validationHandler(e, dbMgr=mgr, asset_name=asset_name, asset_type=asset_type)
    except DatabaseError as e:
        logger.error(e)
    return None


def add_asset_version(asset_name: str, asset_type: str, department: str,
version_num: int, status: str) -> int:
    """
    <User-facing> Add single asset version to database.
    
    :param asset_name: <str> name of asset.
    :param asset_type: <str> type of asset. Must conform to type AssetType.
    :param department: <str> name of department. Must conform to Department type.
    :param verison: <int> version number.
    :param status: <str> status identifier. Must conform to Status type.
    
    :returns: <int> the id of the asset_version created.
    """
    return _add_asset_version(
        {
            "asset": {"name": asset_name, "type": asset_type},
            "department": department,
            "version": version_num,
            "status": status
        }
    )


@with_db_manager()
def _add_asset_version(asset_version: dict, mgr: DBManager) -> int:
    """
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
    except ValidationError as e:
        _validationHandler(e, dbMgr=mgr, asset_version=asset_version)
    except DatabaseError as e:
        logger.error(e)
    return None


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
    except ValidationError as e:
        _validationHandler(e, dbMgr=mgr, asset=asset, asset_version=asset_version)
    except DatabaseError as e:
        logger.error(e)
    return None

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
def get_assets(asset_name: str=None,
asset_type: str=None, mgr: DBManager=None) -> tuple[list[dict]|list]:
    """
    Get asset record by name and type.

    :param asset_name: <str> (Optional) name of asset to retrieve.
    :param asset_type: <AssetType> (Optional) type of asset to retrieve. Must conform
    to type AssetType.
    :param mgr: <DBManager> implicit inclusion by the decorator. Provide
    database operations.

    :returns: <dict> asset record matching name and type.
    """
    try:
        # validate type first
        if asset_type:
            m._Validator(type=asset_type)
    except ValidationError as e:
        logger.error(e)

    return mgr.retrieve_assets(asset_name, asset_type)


@with_db_manager()
def get_asset_version(asset_name: str, asset_type: str, department: str,
    version_num: int, mgr: DBManager) -> tuple[dict|None]:
    """
    Return a single asset version record by asset id, asset type,
    department and version.

    :param asset_name: <str> name of asset to retrieve.
    :param asset_type: <str> type of asset to retrieve version for.
    Must conform to type AssetType.
    :param department: <str> department of asset version. Must conform
    to type Department.
    :param version: <str> version of asset version to retrieve.
    :param mgr: <DBManager> implicit inclusion by the decorator. Provide
    database operations.

    :returns: <dict> asset version record matching the given attributes.
    """
    
    try:
        # validate type and department first
        m._Validator(type=asset_type, department=department)
    except ValidationError as e:
        logger.error(e)

    return mgr.retrieve_single_asset_version(
        asset_name,
        asset_type,
        department,
        version_num
    )


@with_db_manager()
def list_asset_versions(asset_name: str, asset_type: str, department: str=None,
version_num: int=None, status: str=None, mgr: DBManager=None) -> tuple[list[dict]|list]:
    """
    Return array of asset version records matching the given asset name and type.

    :param asset_name: <str> name of asset to retrieve versions for.
    :param asset_type: <AssetType> type of asset to retrieve versions for.
    Must conform to type AssetType.
    :param department: <str> optional department parameter. Must conform to Department type.
    :param verison: <int> optional version parameter.
    :param status: <str> optional status parameter. Must conform to Status type.
    :param mgr: <DBManager> implicit inclusion by the decorator. Provide
    database operations.

    :returns: list[<dict>] asset version records matching the given attributes.
    """
    try:
        # validate type first
        m._Validator(type=asset_type)
        if department:
            m._Validator(department=department)
        if status:
            m._Validator(status=status)
    except ValidationError as e:
        logger.error(e)

    return mgr.list_asset_versions(
        asset_name=asset_name,
        asset_type=asset_type,
        department=department,
        version=version_num,
        status=status
    )
