# AssetValidationService
Framework for asset/asset version validation and registration.

Approach:

When designing this API I knew that I wanted to make use of data models to
ensure data durability both in the initial ingest stage and when being inserted
into the database. 
I like using the pydantic module because its easy to use and extend types.  
It became particular helpful when the asset version type needed to take two
distinct shapes (AssetVersion and AssetVersionJson).

For the data backend I chose sqlite because its lightweight and deploys
easily for demonstration purposes. It took a bit of getting used to the
syntax since I'm more familiar with using PostgreSQL with psycopg2 but it's
mostly the same approach. For future improvements I would have used something
like SQLAlchemy or SQLModel for stronger typing enforcement but everything
was pre-verified by the pydantic model prior to data insertion. I'd 
likely include upsert/delete functionality in future additions.

Otherwise the actual user-facing API is pretty straightforward. I use
a decorator to provide database control so it should be pretty easy to 
modify the data manager or even provide a new data backend without changing
the user-facing API.

I'm using github actions to run my tests since this is a more familiar
workflow for me. If there's time I'd like to get a FastAPI setup with
some of the more general requests like single insertions and retrievals.
I'd probably avoid using the 'load' feature for now in a request scenario
since loading files on a server assumes that path would exist (maybe not).
Not to mention the unknown work time required for larger json files. 
I'd consider doing something where the request would send a task request via
a message queue (redis/rabbitmq) and handle-long running tasks in a de-coupled
setup.


Components:

Model - used to ensure type conformity.
DbManager - database interface. In this case we'll be using sqlite.
AssetSvc API - The user-facing data consumption/validation/storage interface.
Tests - comprehensive unit tests for the components. In cases where
a mock json file is needed, a temporary json file is created


How to test:
    install dependencies:
        -python -m pip install -r requirements.txt
    
    run tests:
        python -m pytest -q

Note - most recent tests will be in the github action as well.


How to use:
    Package can be installed:
        python -m pip install git+https://github.com/flabbyThoroughbred/AssetValidationService.git


    import the package:
    `from AssetServiceController import AssetSvc as aSvc`

    # note - for demonstration purposes I have included a table builder 
    function to run one at the start:
    
    `aSvc._build_tables()`

    optionally tables can be dropped with this command:

    `aSvc._drop_tables()`

    * All attributes are assumed string type unless otherwise noted.
    The following attributes are type validated and will match the following:
        - asset_type ["character", "dressing", "environment", "fx", "prop", "set", "vehicle"]
        - department ["modeling", "texturing", "rigging", "animation", "cfx", "fx"]
        - status ["active", "inactive"]

    1) Loading Assets - This only loads a json file and returns a dict object.
        `aSvc.load_assets(dataFile: <path to your file>)`

    2) Ingest Assets - Loads a json file and ingests assets/asset_versions.
        `aSvc.batch_ingest_data(dataFile: <path to your file>)`
    
    3) Add an Asset - Insert single asset into database. Returns the record id.
        `aSvc.add_asset(asset_name, asset_type)`
    
    4) Add an Asset Version - Insert single asset and asset version into
    database. Will re-use exist assets.
        `aSvc.add_asset_version(
            asset_name="guy",
            asset_type="character",
            department="modeling",
            version=1,
            status="active"
        )`
    
    5) List Assets - List all existing assets.
        `aSvc.list_assets()`

    6) Get Assets - Get assets with either an asset name, asset type.
    Or both. Or neither. Neither will simply do the same as list_assets.
        `aSvc.get_assets(asset_name<optional>, asset_type<optional>)`

    7) Get Asset Version - retrieve a single asset version with required
    criteria.
        `aSvc.get_asset_version(
            asset_name,
            asset_type,
            department,
            version_num[int]
        )

    8) List Asset Versions - retrieve all asset versions with given criteria.
        `aSvc.list_asset_versions(
            asset_name,
            asset_type,
            department<optional>,
            version_num[int]<optional>,
            status<optonal>
        )



