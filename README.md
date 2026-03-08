# AssetValidationService
Framework for asset/asset version validation and registration.

Approach:

When designing this API I knew that I wanted to make use of data models to
ensure data durability both in the initial ingest stage and when being inserted
into the database. 
I like using the pydantic module because its easy to use and extend types.  
It became particular helpful when the asset version type needed to take two
distinct shapes (AssetVersion and AssetVersionJson).

For the data backend I chose sqlite because its lightweight and relatively
simple to hit the ground running. It took a bit of getting used to the
syntax since I'm more familiar with using PostgreSQL with psycopg2 but it's
mostly the same approach. If I had more time I would have used something
like SQLAlchemy or SQLModel for stronger typing enforcement but everything
was pre-verified by the pydantic model prior to data insertion.

Otherwise the actual user-facing API is pretty straightforward. I use
a decorator to provide database control so it should be pretty easy to 
modify the data manager or even provide a new data backend without changing
the user-facing API.

Components:

Model - used to ensure type conformity.
DbManager - Interface with persistent storage. In this case we'll be using sqlite.
The interface will be abstracted out to take data and store it without worrying about
the implementation. 
AssetSvc API - The user-facing data consumption/validation/storage interface.
Tests - comprehensive unit tests for the components.


How to use:
    * Will try to make this pip-able from the github repo.

    import AssetValidationService as AVS

    Ingesting assets/asset versions from a json file:
    *** will validate file and add to asset/asset version db.
    errors will be logged and failed data will be stored in a special
    failed item database ***
        - AVS.ingest_assets(json_file) 
    
    Loading a json file of assets/asset versions as a dictionary:
        - AVS.load_assets(json_file)
    


