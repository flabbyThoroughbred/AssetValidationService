import os
import AssetServiceController.api.AssetSvc as assetSvc
from AssetServiceController.api.DbManager import DBManager
from tests import utils

if __name__ == "__main__":
    utils.build_tables()

    fake_data = os.path.join(
        os.path.dirname(__file__),
        "sample_data",
        "asset_data_with_bad.json"
        # "asset_data_1000_valid.json"
        # "asset_data_1000_with_bad.json"
    )
    assetSvc.batch_ingest_data(fake_data)

    db = DBManager()
    fails = db.list_all_failed_items()
    print(fails)
    # utils.drop_tables()