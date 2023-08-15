import aiohttp
import apache_beam as beam
from netrc import netrc
from pangeo_forge_recipes.transforms import OpenWithKerchunk, CombineReferences, WriteCombinedReference
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
# Open the data with kerchunk if target files are kerchunk side car files
# Call your EDL login information
# Specify a kerchunk store name
transforms = (
    beam.Create(pattern.items())
    | OpenWithKerchunk(
        storage_options = {"client_kwargs": client_kwargs},
    )
    | CombineReferences(
        concat_dims = pattern.concat_dims,
        identical_dims = ["lat", "lon"],
    )
    | WriteCombinedReference(
        store_name = 'gpm-kerchunk',
    )
)
