from setuptools import setup, find_packages

setup(
    name="AssetValidationService"
    description="Asset Validation and Ingest Service",
    version="0.0.1",
    packages=find_packages(),
    install_requires=[
        "pydantic==2.12.5"
    ]


)