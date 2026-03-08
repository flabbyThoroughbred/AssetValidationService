from functools import wraps

from AssetServiceController.api.DbManager import DBManager, with_db_manager

@with_db_manager()
def build_tables(mgr: DBManager):
    print("build tables")
    """clear tables and rebuild."""
    mgr.drop_table("asset_versions")
    mgr.drop_table("assets")
    mgr.drop_table("fails")

    mgr.create_fails_table()
    mgr.create_asset_table()
    mgr.create_asset_version_table()

@with_db_manager()
def drop_tables(mgr: DBManager):
    print("drop tables")
    mgr.drop_table("asset_versions")
    mgr.drop_table("assets")
    mgr.drop_table("fails")


def with_table_lifecycle():
    def wrapper_fnc(fnc):
        @wraps(fnc)
        def wrapper(*args, **kwargs):
            try:
                build_tables()
                return fnc(*args, **kwargs)
            finally:
                drop_tables()
        return wrapper
    return wrapper_fnc