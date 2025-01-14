from pathlib import Path
from functools import partial
import typer
from typing import Optional
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
import traceback


DEFAULT_RECORD_SIZE = 10000


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


def create_single_parquet_store(
    input_file_path,
    output_directory,
    record_size: int = DEFAULT_RECORD_SIZE,
    verbose: int = 0,
):
    """Helper function"""
    filename = input_file_path.stem
    single_parquet_store = output_directory / f"{filename}.parquet"
    create_parquet_store(
        input_file_path,
        output_parquet_store=single_parquet_store,
        record_size=record_size,
    )
    if verbose:
        dataset = xr.open_dataset(
            str(single_parquet_store),
            engine='kerchunk',
            storage_options=dict(remote_protocol='file')
        )
        print(dataset)


def create_multiple_parquet_stores(
    source_directory: Path,
    output_directory: Path,
    pattern: str = "*.nc",
    record_size: int = DEFAULT_RECORD_SIZE,
    workers: int = 4,
    verbose: int = 0,
):
    """ """
    input_file_paths = list(source_directory.glob(pattern))
    if verbose:
        print(f'Input file paths : {input_file_paths}')
    if not input_file_paths:
        print("No files found in [code]{source_directory}[/code] matching the pattern [code]{pattern}[/code]!"
        )
        return
    output_directory.mkdir(parents=True, exist_ok=True)
    with multiprocessing.Pool(processes=workers) as pool:
        print(f'Creating Parquet stores in [code]{output_directory}[/code]')
        partial_create_parquet_references = partial(
            create_single_parquet_store,
            output_directory=output_directory,
            record_size=record_size,
            verbose=verbose,
        )
        pool.map(partial_create_parquet_references, input_file_paths)
    if verbose:
        print(f'Done!')


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
def reference(
    input_file: Path,
    output_directory: Optional[Path] = '.',
    record_size: int = DEFAULT_RECORD_SIZE,
    dry_run: bool = False,
):
    """Create Parquet references from an HDF5/NetCDF file"""
    filename = input_file.stem
    output_parquet_store = output_directory / f'{filename}.parquet'

    if dry_run:
        print(f"[bold]Dry running operations that would be performed[/bold]:")
        print(
            f"> Creating Parquet references to [code]{input_file}[/code] in [code]{output_parquet_store}[/code]"
        )
        return  # Exit for a dry run

    create_parquet_store(
        input_file=input_file,
        output_parquet_store=output_parquet_store,
        record_size=record_size,
    )


@app.command(
    "reference-multi",
    no_args_is_help=True,
    help=f"Create Parquet references to multiple HDF5/NetCDF files",
)
def reference_multi(
    source_directory: Path,
    output_directory: Optional[Path] = '.',
    pattern: str = "*.nc",
    record_size: int = DEFAULT_RECORD_SIZE,
    workers: int = 4,
    dry_run: bool = False,
    verbose: int = 0,
):
    """Create Parquet references from an HDF5/NetCDF file"""
    input_file_paths = list(source_directory.glob(pattern))

    if not input_file_paths:
        print("No files found in the source directory matching the pattern.")
        return

    if dry_run:
        print(
            f"[bold]Dry running operations that would be performed[/bold]:"
        )
        print(
            f"> Reading files in [code]{source_directory}[/code] matching the pattern [code]{pattern}[/code]"
        )
        print(f"> Number of files matched : {len(input_file_paths)}")
        print(f"> Creating Parquet stores in [code]{output_directory}[/code]")
        return  # Exit for a dry run

    create_multiple_parquet_stores(
        source_directory=source_directory,
        output_directory=output_directory,
        pattern=pattern,
        record_size=record_size,
        workers=workers,
        verbose=verbose,
    )


@app.command(
    'combine-stores',
    no_args_is_help=True,
    help=f"Combine multiple Parquet stores",
)
def combine_parquet_stores(
        source_directory: Path,
        output_parquet_store: Path,
        pattern: str = '*.parquet',
        record_size: Optional[int] = DEFAULT_RECORD_SIZE,
):
    combine_multiple_parquet_stores(
        source_directory=source_directory,
        pattern=pattern,
        output_parquet_store=output_parquet_store,
        record_size=record_size,
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
    )
    print(dataset)


if __name__ == "__main__":
    app()
