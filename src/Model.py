from enum import Enum
from pydantic import BaseModel, PositiveInt, FilePath


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
# =============================================================================

# ==== Type Models ============================================================
class JsonFile(BaseModel):
    filePath: FilePath