import aiohttp
import apache_beam as beam
from netrc import netrc
from pangeo_forge_recipes.transforms import OpenURLWithFSSpec, OpenWithXarray, StoreToZarr
from pangeo_forge_cmr import files_from_cmr


# Specify the data collection shortname of your choice 
shortname = 'GPM_3IMERGDL'
# Specify the data collection version 
version = '06'

# Get a list of file URLs by querying CMR, and assemble them into a pangeo-forge pattern that includes
# dimensional indexing
pattern = files_from_cmr(
    shortname,
    version,

    # Tell pangeo-forge what dimension to concat files 
    concat_dim='time', 
)

# Grab netrc username and password
(username, account,password) = netrc().authenticators("urs.earthdata.nasa.gov")
client_kwargs = {
    "auth": aiohttp.BasicAuth(username, password),
    "trust_env": True,
}


transforms = (
    # Create a beam PCollection (pipeline collection) containing the paths to the granules for this dataset
    # The pattern attaches an index to each granule mapping its place in the zarr store
    beam.Create(pattern.items())

    # Open each granule into an fsspec OpenFile (EDL is needed to access)
    | OpenURLWithFSSpec(
        open_kwargs = {"client_kwargs": client_kwargs},
    )
    # Pass fsspec OpenFiles to xarray and load them into xarray datasets
    | OpenWithXarray(file_type=pattern.file_type)

    # Once you have a PCollection of xarray datasets, you can write a preprocessor here
    # You can transform those xarray datasets in ANY way that xarray allows you to transform
    # For example, selecting a variable of choice, ds.Precipitation for example. 

    # This preprocessing will be parallelized across granules, remaining completely distributed.
    # This is the power of beam! 

    # Write the zarr store to storage at a path with specificed store name
    | StoreToZarr(
        store_name = 'gpm.zarr', 
        combine_dims = pattern.combine_dim_keys,

        # In zarr store you will be aggregating 10 files per chunk
        # You want to know the size of a single granule, and we aim for an optimal 100-200MB chunks
        # Dynamic chunking is in the works: https://github.com/pangeo-forge/pangeo-forge-recipes/pull/546 
        target_chunks = {'time' : 10} 
    )
)