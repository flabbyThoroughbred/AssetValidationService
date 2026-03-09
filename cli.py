"""
Command-line interface for the Asset Service.
"""

import click
from AssetServiceController.api import AssetSvc

@click.group()
@click.pass_context
def cli(ctx):
    """Asset Validation & Registration Service CLI"""
    pass


@cli.command()
@click.argument("file_path", type=click.Path(exists=True))
@click.pass_context
def load(ctx, file_path):
    """Load assets from a JSON file."""
    _service = ctx.obj["service"]


@cli.command()
@click.argument("asset_name")
@click.argument("asset_type")
@click.pass_context
def add(ctx, asset_name, asset_type):
    """Add an asset from a JSON file."""
    _service = ctx.obj["service"]


@cli.command()
@click.argument("asset_name")
@click.argument("asset_type")
@click.pass_context
def get(ctx, asset_name, asset_type):
    """Get an asset by name and type."""
    _service = ctx.obj["service"]


@cli.command(name="list")
@click.option(
    "--asset-name",
    "asset_name",
    required=False,
    default=None,
    help="Filter by asset name",
)
@click.option(
    "--asset-type",
    "asset_type",
    required=False,
    default=None,
    help="Filter by asset type",
)
@click.pass_context
def list_cmd(ctx, asset_name, asset_type):
    """List all assets."""
    _service = ctx.obj["service"]


@cli.group()
def versions():
    """CLI group to manage asset version subcommands"""
    pass


@versions.command("add")
@click.argument("asset_name")
@click.argument("asset_type")
@click.argument("department")
@click.argument("version_num", type=int)
@click.argument("status")
@click.pass_context
def versions_add(ctx, asset_name, asset_type, department, version_num, status):
    """Add an asset version from a JSON file."""
    _service = ctx.obj["service"]


@versions.command("get")
@click.argument("asset_name")
@click.argument("asset_type")
@click.argument("department")
@click.argument("version_num", type=int)
@click.pass_context
def versions_get(ctx, asset_name, asset_type, department, version_num):
    """Get a specific asset version."""
    _service = ctx.obj["service"]


@versions.command("list")
@click.argument("asset_name")
@click.argument("asset_type")
@click.option("--department", required=False, default=None, help="Filter by department")
@click.option("--status", required=False, default=None, help="Filter by status")
@click.option(
    "--version", required=False, default=None, type=int, help="Filter by version"
)
@click.pass_context
def versions_list(ctx, asset_name, asset_type, department, status, version):
    """List all versions of an asset."""
    _service = ctx.obj["service"]


def main():
    """Entry point for the CLI."""
    cli(obj={})


if __name__ == "__main__":
        # utils.build_tables()

    # fake_data = os.path.join(
    #     os.path.dirname(__file__),
    #     "sample_data",
    #     # "asset_data.json"
    #     # "asset_data_with_bad.json"
    #     # "asset_data_1000_valid.json"
    #     "asset_data_1000_with_bad.json"
    # )
    # assetSvc.batch_ingest_data(fake_data)

    # db = DBManager()
    # fails = db.list_all_failed_items()
    # print(fails)
    # utils.drop_tables()
    
    main()
