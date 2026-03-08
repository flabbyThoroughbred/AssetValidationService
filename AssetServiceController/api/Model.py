from enum import Enum
from pydantic import BaseModel, PositiveInt, FilePath
from typing import Optional

# ===== Enumerated value types ================================================
class AssetType(str, Enum):
    character = "character"
    dressing = "dressing"
    environment = "environment"
    fx = "fx"
    prop = "prop"
    set = "set"
    vehicle = "vehicle"


class DataType(str, Enum):
    asset = "asset"
    assetVersion = "assetVersion"


class Department(str, Enum):
    modeling = "modeling"
    texturing = "texturing"
    rigging = "rigging"
    animation = "animation"
    cfx = "cfx"
    fx = "fx"


class Status(str, Enum):
    active = "active"
    inactive = "inactive"
# =============================================================================


# ===== Data models ===========================================================
class Asset(BaseModel):
    name: str = ... # required field
    type: AssetType


class AssetVersionJson(BaseModel):
    asset: Asset
    department: Department = ...
    version: PositiveInt = 1
    status: Status = Status.active


class AssetVersion(AssetVersionJson):
    asset: int # asset id, foreign key to Asset table


class AssetVersionLite(AssetVersionJson):
    """
    Helper type to avoid cases where an AssetVersion may not 
    have as asset value initially. Such cases may be when an asset
    and an asset version are provided separately.
    """
    asset: Optional[Asset|int] = None
# =============================================================================


# ==== Type Models ============================================================
class JsonFile(BaseModel):
    filePath: FilePath