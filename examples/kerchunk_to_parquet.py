from pathlib import Path

import typer
import xarray as xr
from rich import print

import fsspec
from fsspec.implementations.reference import LazyReferenceMapper
from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.combine import MultiZarrToZarr
import multiprocessing
import json
from loguru import logger
logger.remove()
logger.add("debug.log", format="{time} {level} {message}", level="DEBUG")
# Log environment variables
logger.info("Environment Variables:")
import os
safe_keys = ['PATH', 'USER', 'SHELL']
for key in safe_keys:
    logger.info(f"{key}: {os.environ.get(key)}")

# Log Python version
import sys
logger.info(f"Python Version: {sys.version}")

# Log installed packages and versions
import pkg_resources
logger.info("Installed Packages and Versions:")
for dist in pkg_resources.working_set:
    logger.info(f"{dist.project_name}=={dist.version}")

# Log current working directory
logger.info(f"Current Working Directory: {os.getcwd()}")




app = typer.Typer(
    no_args_is_help=True,
    add_completion=True,
    add_help_option=True,
    rich_markup_mode="rich",
    help=f"Create parquet references",
)


def create_parquet_store(
    input_file: Path,
    output_parquet_store: Path,
    record_size: int = DEFAULT_RECORD_SIZE,
):
    """ """
    logger.info('Logging execution of create_parquet_store()')
    output_parquet_store.mkdir(parents=True, exist_ok=True)

    try:
        logger.info(f'Creating a filesystem mapper for {output_parquet_store}')
        filesystem = fsspec.filesystem("file")
        output = LazyReferenceMapper.create(
            root=str(output_parquet_store),  # does not handle Path
            fs=filesystem,
            record_size=record_size,
        )
        logger.info(f'Created the filesystem mapper {output}')

        logger.info(f'Kerchunking the file {input_file}')
        single_zarr = SingleHdf5ToZarr(str(input_file), out=output)
        single_zarr.translate()
        logger.info(f'Kerchunked the file {input_file}')

    except Exception as e:
        print(f"Failed processing file [code]{input_file}[/code] : {e}")
        logger.error(f"Exception occurred: {e}")
        logger.error("Traceback (most recent call last):")
        
        tb_lines = traceback.format_exc().splitlines()
        for line in tb_lines:
            logger.error(line)

        raise

    logger.info(f'Returning a Parquet store : {output_parquet_store}')
    return output_parquet_store
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
def combine_multiple_parquet_stores(
    source_directory: Path,
    output_parquet_store: Path,
    pattern: str = '*.parquet',
    record_size: int = DEFAULT_RECORD_SIZE,
):
    # output_parquet_store = Path(output_parquet_store)
    output_parquet_store = output_parquet_store.parent / (output_parquet_store.name + '.parquet')
    output_parquet_store.mkdir(parents=True, exist_ok=True)
    filesystem = fsspec.filesystem("file")
    try:
        output = LazyReferenceMapper.create(
            root=str(output_parquet_store),
            fs=filesystem,
            record_size=record_size,
        )
        input_references = list(source_directory.glob(pattern))
        input_references = list(map(str, input_references))
        input_references.sort()
        multi_zarr = MultiZarrToZarr(
            input_references,
            remote_protocol="file",
            concat_dims=["time"],
            identical_dims= ["lat", "lon"],
            coo_map={"time": "cf:time"},
            out=output,
        )
        multi_zarr.translate()
        output.flush()

    except Exception as e:
        print(f"Failed creating the [code]{output_parquet_store}[/code] : {e}!")
        import traceback
        traceback.print_exc()
        # return


@app.command(
    "reference",
    no_args_is_help=True,
    help=f"Create Parquet references to an HDF5/NetCDF file",
)
def parquet_reference(
    input_file: Path,
    output_parquet_store: Path,
    record_size: int = 1000,
    dry_run: bool = False,
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
