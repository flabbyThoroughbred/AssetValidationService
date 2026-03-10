import pytest

@pytest.fixture
def good_data():
    return [{
            "asset": {
            "name": "hero",
            "type": "prop"
            },
            "department": "modeling",
            "version": 1,
            "status": "active"
        },
        {
            "asset": {
            "name": "hero",
            "type": "prop"
            },
            "department": "modeling",
            "version": 2,
            "status": "active"
        },
        {
            "asset": {
            "name": "hero",
            "type": "prop"
            },
            "department": "modeling",
            "version": 3,
            "status": "active"
        }]

@pytest.fixture
def generic_asset():
    return {"name": "guy", "type": "character"}