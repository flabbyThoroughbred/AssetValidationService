# AssetValidationService
Framework for asset validation and registration.

Components:

Model - used to ensure type conformity.
DataMgr - Interface with persistent storage. In this case we'll be using sqlite.
The interface will be abstracted out to take data and store it without worrying about
the implementation. 
API - The user-facing data consumption/validation/storage interface.
Tests - fundamental unit tests for the components.

Model is the primary validator for incoming data. It contains the
Asset and AssetVersion data structure model with field types. Any enumerated
values are also defined here. I'm using pydantic here because its able to 
act as an instant validator in many use cases.

** Development Notes **
- 03/04/2026
Added type models and planning database management and API.
Simple test suite layout.
Need to get a logger in here.




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
    


