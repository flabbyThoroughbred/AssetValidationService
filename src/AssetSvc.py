from Model import JsonFile, Asset, AssetVersionJson, AssetVersion

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

def ingest_data(dataFile: str) -> None:
    """
    Take a json file array of assets/assetVersions and add to database.
    Type models validate by checking if each individual item is either
    an Asset or AssetVersion. If AssetVersion, validates its asset field
    as an Asset type...etc.
    
    steps:
    - load json as dict
    - iterate through items
    """


    pass

def add_asset(asset: Asset) -> int:
    """
    Add asset to database, return asset id.
    If asset already exists, return existing id.
    :param asset: <dict> asset to be added
    """
    pass

def add_asset_version(assetVersion: AssetVersion) -> int:
    pass