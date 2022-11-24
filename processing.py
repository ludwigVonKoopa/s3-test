import fastparquet
import h3._cy.unstable_vect as _vect
import h3.api.basic_str as h3
import numpy as np
import s3fs


def _build_h3_str(lon, lat, level):
    """build h3 str indexes

    Parameters
    ----------
    lon : np.array
        longitudes. Should be in epsg=4326 coordinates ([-180, 180])
    lat : np.array
        latitudes
    level : int
        level of h3 index. Should be 0 <= level <= 15

    Returns
    -------
    np.array
        array with h3 indexes
    """
    out = np.array([h3.geo_to_h3(lat=y, lng=x, resolution=level) for x, y in zip(lon, lat)])
    return out


def _build_h3_int(lon, lat, level):
    """build h3 int indexes

    Parameters
    ----------
    lon : np.array
        longitudes. Should be in epsg=4326 coordinates ([-180, 180])
    lat : np.array
        latitudes
    level : int
        level of h3 index. Should be 0 <= level <= 15

    Returns
    -------
    np.array
        array with h3 indexes
    """
    out = np.zeros(lon.size, dtype='uint64')
    _vect.geo_to_h3_vect(lat.astype(np.float64), lon.astype(np.float64), level, out)
    return out.astype("int64")


def build_h3_indexes(ds, levels=[0], h3_type="int", lonName="lon", latName="lat"):
    """build indexes for xr.Dataset for levels

    Parameters
    ----------
    ds : xarray.Dataset
        dataset where to build indexes. Needs to have `lonName` and `latName` coords
    levels : list or iterable, optional
        levels of h3 index to build, by default [0]
    h3_type : str, optional
        which index to build : str or int, by default "int"
    lonName : str, optional
        name of longitude coordinate, by default "lon"
    latName : str, optional
        name of latitude coordinate, by default "lat"

    Raises
    ------
    TypeError
        return xarray.Dataset with h3 indexes built. No copy is done
    """

    h3_types = ["int", "str"]
    if h3_type not in h3_types:
        raise TypeError(f"h3_type '{h3_type}' unknow. Params allowed : {h3_types}")

    # get center of boxes
    dlon = float(ds[lonName].diff("lon")[0])
    dlat = float(ds[latName][0])

    # h3 indexes needs epsg=4326 projection ([-180, 180])
    lon = ((ds[lonName] + dlon/2 + 180) % 360) - 180
    lat = ds[latName] + dlat/2

    lon_1d, lat_1d = np.meshgrid(lon.values, lat.values)
    lon_1d, lat_1d = lon_1d.flatten(), lat_1d.flatten()

    for level in levels:
        name = f"h3_{level:02d}"
        print(f"building index={level}..")
        if name in ds:
            continue

        if h3_type == "int":
            out = _build_h3_int(lon_1d, lat_1d, level)
        else:
            out = _build_h3_str(lon_1d, lat_1d, level)

        ds[name] = ([lonName, latName], out.reshape(lon.size, lat.size))

    return ds


def ds_to_parquet(ds, parquet_filename, dim_time="time", batch_size=6, chunk=10_000_000):
    """transform xarra.Dataset to parquet file. support uploading to s3.

    As the xarray.Dataset file can be big, the process will be splitted among time dimension to reduce ram consumption

    Parameters
    ----------
    ds : xarra.Dataset
        dataset to convert
    parquet_filename : str
        parquet filename. if begin with "s3://", it will store in s3
    dim_time : str, optional
        name of time dimension of dataset, by default "time"
    batch_size : int, optional
        number of time coordinate to take for each batch, by default 6
    chunk : int, optional
        parquet file chunk, by default 10_000_000
    """

    size = ds[dim_time].size
    indexs = np.arange(0, size+1+batch_size, batch_size)


    # if we used an s3 file with bucket name, we use s3fs to connect it
    if parquet_filename.startswith("s3://"):
        s3 = s3fs.S3FileSystem()
        kw = dict(open_with=s3.open(), filename=parquet_filename[5:])
    else:
        kw = dict(filename=parquet_filename)


    append_mode = False  # no append mode for the first iteration
    for i, (i0, iN) in enumerate(zip(indexs[:-1], indexs[1:])):
        print(f"step {i+1:04d}/{indexs.size-1} indices:{i0:04d}=>{iN:04d}", end="\r")

        # select time indices, transform it to pandas.DataFrame, and set time as index
        df = ds.isel(**{dim_time: slice(i0, iN)}).load().to_dataframe().reset_index().set_index(dim_time)
        fastparquet.write(data=df, compression="GZIP", write_index=True,
            append=append_mode, file_scheme="simple", row_group_offsets=chunk, **kw
        )
        append_mode = True

    print()  # because of last print end="\r", the next line printed should not be reset
