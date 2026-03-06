import sqlite3
from functools import wraps

from .Model import Asset, AssetVersion, AssetType, Department, Status
from .Logger import create_logger

"""
Was considering using SQLAlchemy or SQLModel but going to stick with
pydantic + sqlite3 for now. This means that type enforcement is a bit
more manual but it keeps the complexity down a bit.
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

    def create_tables(self):
        self.create_asset_table()
        self.create_asset_version_table()

    def insert_assets(self, assets: list[Asset]) -> list[int]:
        """
        Assumes data is a list of Asset dicts. Validation takes place prior
        to database insertion instead of gate-kept at the DB API level.
        
        :param assets: <array[Asset]> an array of pre-validated Asset instances.

        :returns: <array[int]> array of inserted asset ids.
        """

        # ensures that id is returned if asset already exists.
        cmd = """
        INSERT INTO assets (name, type)
        VALUES (:name, :type)
        ON CONFLICT (name, type) DO UPDATE SET
            name=excluded.name
        RETURNING id;
        """

        try:
            ids = []
            for asset in assets:
                cursor = self.session.execute(cmd, asset.model_dump())
                ids.append(cursor.fetchone()["id"])
            self.session.commit()
            return ids
        except (sqlite3.IntegrityError, sqlite3.ProgrammingError) as e:
            self.logger.error(f"Error inserting assets: {e}")
            self.session.rollback()
            raise e

    def insert_asset_versions(self, asset_versions: list[AssetVersion]) -> list[int]:
        """
        Assumes data is a list of AssetVersion dicts. Validation takes place
        prior to database insertion instead of gate-kept at the DB API level.
        
        :param asset_versions: <array[AssetVersion]> an array of pre-validated
        AssetVersion instances.
        
        :returns: <array[int]> array of inserted asset version ids.
        """
        # NOTE - need to handle duplicate conflicts and return id of existing record.

        cmd = """
        INSERT INTO asset_versions (asset, department, version, status)
        VALUES (:asset, :department, :version, :status)
        RETURNING id;
        """

        try:
            ids = []
            for asset_version in asset_versions:
                cursor = self.session.execute(cmd, asset_version.model_dump())
                ids.append(cursor.fetchone()["id"])
            self.session.commit()
            return ids
        except (sqlite3.IntegrityError, sqlite3.ProgrammingError) as e:
            self.logger.error(f"Error inserting asset versions: {e}")
            self.session.rollback()
            raise e

    def list_assets(self) -> list[dict]:
        """
        :returns: <array[dict]> array of all asset records.
        """

        cmd = """SELECT * FROM assets;"""
        cursor = self.session.execute(cmd)
        # return [record for record in cursor.fetchall()]
        return cursor.fetchall()
    
    def list_asset_versions(self) -> list[dict]:
        """
        :returns: <array[dict]> array of all asset version records.
        """

        cmd = """SELECT * FROM asset_versions;"""
        cursor = self.session.execute(cmd)
        # return [record for record in cursor.fetchall()]
        return cursor.fetchall()