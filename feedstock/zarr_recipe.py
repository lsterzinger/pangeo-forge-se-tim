import aiohttp
import apache_beam as beam
from netrc import netrc
from pangeo_forge_recipes.transforms import OpenURLWithFSSpec, OpenWithXarray, StoreToZarr
from pangeo_forge_cmr import files_from_cmr


# Specify the data collection shortname of your choice 
shortname = 'GPM_3IMERGDL'
# Specify the data collection version 
version = '06'

# Get a list of file URLs by querying CMR
# Specify which dimension to chunk the data (concat_dim) and for that dimension
# specify how many times that dimension occurs in the original files (nitems_per_file)
pattern = files_from_cmr(
    shortname,
    version,
    nitems_per_file=1,
    concat_dim='time', 
)

# Grab netrc username and password
(username, account,password) = netrc().authenticators("urs.earthdata.nasa.gov")
client_kwargs = {
    "auth": aiohttp.BasicAuth(username, password),
    "trust_env": True,
}

# Run beam over the list of files/rechunk pattern defined above
# Call your EDL login information
# Open the data with xarray if target file output is zarr
# Specify a zarr store name
transforms = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec(
        open_kwargs = {"client_kwargs": client_kwargs},
    )
    | OpenWithXarray(file_type=pattern.file_type)
    | StoreToZarr(
        store_name = 'gpm.zarr', 
        combine_dims = pattern.combine_dim_keys,
    )
)