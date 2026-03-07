import sqlite3
from functools import wraps

from .Model import Asset, AssetVersion, AssetType, Department, Status
from .Logger import create_logger

"""
Was considering using SQLAlchemy or SQLModel but going to stick with
pydantic + sqlite3 for now. This means that type enforcement is a bit
more manual but it keeps the complexity down a bit.
"""

# ====== SQL COMMAND TEMPLATES ================================================
INSERT_ASSET_CMD = """
    INSERT INTO assets (name, type)
    VALUES (:name, :type)
    ON CONFLICT (name, type) DO UPDATE SET
        name=excluded.name
    RETURNING id;
"""

INSERT_ASSET_VERSION_CMD = """
    INSERT INTO asset_versions (asset, department, version, status)
    VALUES (:asset, :department, :version, :status)
    RETURNING id;
"""

INSERT_FAILED_CMD = """
    INSERT INTO fails (fail_data, loc, type, msg)
    VALUES (:fail_data, :loc, :type, :msg)
    RETURNING id;
"""

# ===== UTILS =================================================================
def dict_factory(cursor, row):
    """
    Factory function to convert sqlite query results to dictionaries.
    """
    _dict = {}
    for idx, col in enumerate(cursor.description):
        _dict[col[0]] = row[idx]    
    return _dict


def connection():
    """
    Create a connection to a local sqlite database.
    """
    con = sqlite3.connect("db.sqlite3")
    con.row_factory = dict_factory
    con.execute("PRAGMA foreign_keys = ON;") # need for foreign keys.
    return con


def with_db_manager():
    """
    Decorator to utilize the DBManager and all of its functionality
    with the added benefit of automatic connection management.
    """
    def mgr_wrapper(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            db_manager = None
            try:
                db_manager = DBManager()
                return func(*args, **kwargs, mgr=db_manager)
            finally:
                print("Closing DB connection...")
                db_manager.session.close()
        return wrapper
    return mgr_wrapper
# =============================================================================


class DBManager:
    """
    Manager for creating/dropping Asset Service tables, inserting,
    selection and updates.
    In this guise its up to the user to manage the connection lifecycle.
    """
    def __init__(self):
        self.session = connection()
        self.logger = create_logger("DBManager")

    def ensure_table(self, table_name: str) -> None:
        """
        Check if table exists in database, if not throw an error.
        """
        table_check = self.session.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
            (table_name,)
        )
        if not table_check.fetchall():
            raise ValueError(f"Table '{table_name}' does not exist.")
        
    def drop_table(self, table_name: str) -> None:
        cmd = f"DROP TABLE IF EXISTS {table_name};" # Not safe but using for demonstration/test purposes.
        self.session.execute(cmd)
        self.session.commit()
        self.logger.info(f"Table '{table_name}' dropped successfully.")

    def create_asset_table(self) -> None:
        self.logger.info("Creating asset table...")
        type_vals = [e.value for e  in AssetType]
        _cmd = """
            CREATE TABLE IF NOT EXISTS assets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                type TEXT CHECK( type IN ({}) ) NOT NULL,
                CONSTRAINT UQ_NameType UNIQUE (name, type)
            );
        """
        cmd = _cmd.format(", ".join(f"'{val}'" for val in type_vals))
        self.session.execute(cmd)

        self.session.commit()
        self.logger.info("assets table created successfully.")

    def create_asset_version_table(self) -> None:
        dept_vals = [e.value for e in Department]
        status_vals = [e.value for e in Status]

        self.logger.info("Creating asset version table...")
        _cmd = """
            CREATE TABLE IF NOT EXISTS asset_versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset INTEGER NOT NULL,
                department TEXT CHECK( department IN ({}) ) NOT NULL,
                version INTEGER NOT NULL,
                status TEXT CHECK( status IN ({}) ) NOT NULL,
                FOREIGN KEY (asset) REFERENCES assets (id)
                CONSTRAINT UQ_ASSET_DEPT_VER UNIQUE (asset, department, version)
            );
        """
        self.session.execute(_cmd.format(
            ", ".join(f"'{val}'" for val in dept_vals),
            ", ".join(f"'{val}'" for val in status_vals)
        ))
        self.session.commit()
        self.logger.info("asset_versions table created successfully.")

    def create_fails_table(self) -> None:
        self.logger.info("Creating fails table...")
        cmds = """
            CREATE TABLE IF NOT EXISTS fails (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fail_data JSON NOT NULL,
                loc TEXT NOT NULL,
                type TEXT NOT NULL,
                msg TEXT NOT NULL
            );
        """
        self.session.execute(cmds)
        self.session.commit()
        self.logger.info("fails table created successfully.")

    def create_tables(self):
        self.create_asset_table()
        self.create_asset_version_table()
        self.create_fails_table()

    def insert_assets(self, assets: list[Asset],
    defer_commit: bool=False) -> list[int]:
        """
        Assumes data is a list of Asset dicts. Validation takes place prior
        to database insertion instead of gate-kept at the DB API level.
        
        :param assets: <array[Asset]> an array of pre-validated Asset instances.
        :param defer_commit: <bool> User is responsible for committing changes.

        :returns: <array[int]> array of inserted asset ids.
        """

        try:
            ids = []
            for asset in assets:
                cursor = self.session.execute(
                    INSERT_ASSET_CMD,
                    asset.model_dump()
                )
                ids.append(cursor.fetchone()["id"])
            if not defer_commit:
                self.session.commit()
            return ids
        except (sqlite3.IntegrityError, sqlite3.ProgrammingError) as e:
            self.logger.error(f"Error inserting assets: {e}")
            self.session.rollback()
            raise e

    def insert_asset_versions(self, asset_versions: list[AssetVersion],
    defer_commit: bool=False) -> list[int]:
        """
        Assumes data is a list of AssetVersion dicts. Validation takes place
        prior to database insertion instead of gate-kept at the DB API level.
        
        :param asset_versions: <array[AssetVersion]> an array of pre-validated
        AssetVersion instances.        
        :param defer_commit: <bool> User is responsible for committing changes.

        :returns: <array[int]> array of inserted asset version ids.
        """

        try:
            ids = []
            for asset_version in asset_versions:
                cursor = self.session.execute(
                    INSERT_ASSET_VERSION_CMD,
                    asset_version.model_dump()
                )
                ids.append(cursor.fetchone()["id"])
            if not defer_commit:
                self.session.commit()
            return ids
        except (sqlite3.IntegrityError, sqlite3.ProgrammingError) as e:
            self.logger.error(f"Error inserting asset versions: {e}")
            self.session.rollback()
            raise e

    def insert_asset_and_version(self, asset: Asset,
    asset_version: AssetVersion, defer_commit: bool=False) -> dict:
        """
        Insert an asset and companion asset version in one transaction.
        :param asset: <Asset> pre-validated Asset instance.
        :param asset_version: <AssetVersion> pre-validated AssetVersion instance.
        :param defer_commit: <bool> User is responsible for committing changes.

        :returns: <dict> dict containing asset and asset version ids.
        """

        try:
            a_crs = self.session.execute(INSERT_ASSET_CMD, asset.model_dump())
            asset_id = a_crs.fetchone()["id"]

            av_data = asset_version.model_dump()
            av_data["asset"] = asset_id
            av_crs = self.session.execute(INSERT_ASSET_VERSION_CMD, av_data)
            av_id = av_crs.fetchone()["id"]
            if not defer_commit:
                self.session.commit()
            return {"asset_id": asset_id, "asset_version_id": av_id}
        except (sqlite3.IntegrityError, sqlite3.ProgrammingError) as e:
            self.logger.error(f"Error inserting asset and version: {e}")
            self.session.rollback()
            raise e

    def insert_fails(self, failed_data: dict, defer_commit: bool=False) -> int:
        """
        Insert a data validation error record into the fails table.

        :param failed_data: <dict> failure data.
        :param defer_commit: <bool> User is responsible for committing changes.

        :returns: <int> fail record id.
        """
        try:
            cursor = self.session.execute(INSERT_FAILED_CMD, failed_data)
            fail_id = cursor.fetchone()["id"]
            if not defer_commit:
                self.session.commit()
            return fail_id
        except (sqlite3.IntegrityError, sqlite3.ProgrammingError) as e:
            self.logger.error(f"Error inserting asset and version: {e}")
            self.session.rollback()
            raise e

    def retrieve_single_asset(self, asset_name: str,
    asset_type: str) -> dict | None:
        """
        Retrieve a single asset given both and asset name and type.

        :param asset_name: <str> pre-validated name of the asset to retrieve.
        :param asset_type: <str> pre-validated type of asset to retrieve.

        :returns: <dict> the retrieved record or None if it doesn't exist.
        """

        cmd = """
        SELECT * FROM assets
        WHERE name = :asset_name AND type = :asset_type;
        """
        cursor = self.session.execute(
            cmd,
            {"asset_name": asset_name, "asset_type": asset_type}
        )
        return cursor.fetchone()
    
    def list_all_assets(self) -> list[dict] | []:
        """
        :returns: <array[dict]> array of all asset records or empty array
        if none are found.
        """

        cmd = """SELECT * FROM assets;"""
        cursor = self.session.execute(cmd)
        return cursor.fetchall()
    
    def list_asset_versions(self, asset_name: str,
    asset_type: str) -> list[dict] | []:
        """
        List all of an asset's versions by name and type.

        :param asset_name: <str> pre-validated name of the asset to retrieve.
        :param asset_type: <str> pre-validated type of asset to retrieve.

        :returns: <array[dict]> array of all asset version records related
        to a specific asset or an empty array.
        """

        # NOTE We're joining tables but we only want the asset version
        # records as our result.

        cmd = """
        SELECT av* FROM asset_versions av
        JOIN assets a ON av.asset = a.id
        WHERE a.name = :asset_name AND a.type = :asset_type;
        """

        cursor = self.session.execute(
            cmd,
            {"asset_name": asset_name, "asset_type": asset_type}
        )
        return cursor.fetchall()
    
    def list_all_asset_versions(self) -> list[dict] | []:
        """
        List all asset versions in the table.

        :returns: <array[dict]> array of all asset version records
        or empty array if none are found.
        """

        cmd = """SELECT * FROM asset_versions;"""
        cursor = self.session.execute(cmd)
        return cursor.fetchall()