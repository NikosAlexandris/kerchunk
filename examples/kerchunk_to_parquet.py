from pathlib import Path

import typer
import xarray as xr
from rich import print

import fsspec
from fsspec.implementations.reference import LazyReferenceMapper
from kerchunk.hdf import SingleHdf5ToZarr


app = typer.Typer(
    no_args_is_help=True,
    add_completion=True,
    add_help_option=True,
    rich_markup_mode="rich",
    help=f"Create parquet references",
)


def create_parquet_references(
    input_file: Path,
    output_parquet_store: Path,
    record_size: int = 1000,
    dry_run: bool = False,
):
    """ """
    output_parquet_store.mkdir(parents=True, exist_ok=True)
    filesystem = fsspec.filesystem("file")
    output = LazyReferenceMapper.create(
        root=str(output_parquet_store),  # does not handle Path
        fs=filesystem,
        record_size=record_size,
    )
    if dry_run:
        print(
            f"[bold]Dry run[/bold] of [bold]operations that would be performed[/bold]:"
        )
        print(
            f"> Creating [bold]parquet[/bold] references to [code]{input_file}[/code] in [code]{output_parquet_store}[/code]"
        )
        return  # Exit for a dry run
    SingleHdf5ToZarr(input_file, out=output).translate()
    output.flush()

    return output_parquet_store


@app.command(
    "reference",
    no_args_is_help=True,
    help=f"Create Parquet references to an HDF5/NetCDF file",
)
def parquet_reference(
    input_file: Path,
    output_parquet_store: Path,
    record_size: int = 1000,
    # source_directory: Path,
    # reference_directory: Path,
    # source_pattern: str = '*.nc',
    # reference_pattern: str = '*.json',
    # workers: int = 4,
    dry_run: bool = False,
    # combined_reference: Path = "combined_kerchunk.parq",
):
    """Create Parquet references from an HDF5/NetCDF file"""
    parquet_store = create_parquet_references(
        input_file=input_file,
        output_parquet_store=output_parquet_store,
        record_size=record_size,
        dry_run=dry_run,
    )


@app.command(
    no_args_is_help=True,
    help=f"Select data from a Parquet references store",
)
def select(
    parquet_store: Path,
):
    """Select data from a Parquet store"""
    dataset = xr.open_dataset(
        str(parquet_store),  # does not handle Path
        engine="kerchunk",
        storage_options=dict(skip_instance_cache=True, remote_protocol="file"),
        # backend_kwargs={"consolidated": False},
    )
    print(dataset)


if __name__ == "__main__":
    app()
