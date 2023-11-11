from pathlib import Path
import fsspec
from fsspec.implementations.reference import LazyReferenceMapper
from kerchunk.combine import MultiZarrToZarr


def combine_multiple_parquet_stores(source_directory: Path, output_parquet_store: Path):
    source_directory = Path(source_directory)
    output_parquet_store = Path(output_parquet_store)
    output_parquet_store = output_parquet_store.parent / (output_parquet_store.name + '.parquet')
    output_parquet_store.mkdir(parents=True, exist_ok=True)
    filesystem = fsspec.filesystem("file")
    try:
        output = LazyReferenceMapper.create(
            root=str(output_parquet_store),
            fs=filesystem,
            record_size=10
        )
        reference_pattern = '*.parquet'
        input_references = list(source_directory.glob(reference_pattern))
        input_references = list(map(str, input_references))
        multi_zarr = MultiZarrToZarr(
            input_references,
            remote_protocol="memory",
            concat_dims=["time"],
            out=output,
        )
        multi_zarr.translate()
        output.flush()
    except Exception as e:
        print(f"Failed creating the [code]{output_parquet_store}[/code] : {e}!")
        # return
