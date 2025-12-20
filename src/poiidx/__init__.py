import shapely

from poiidx.poiIdx import PoiIdx

def assert_initialized():
    try:
        PoiIdx.assert_initialized()
    except RuntimeError as e:
        raise RuntimeError("PoiIdx not initialized. Call poiidx.init() first.") from e

def init(recreate: bool = False, **kwargs):
    PoiIdx.connect(**kwargs)
    if recreate:
        PoiIdx.drop_schema()
    
    PoiIdx.init_if_new()

def close():
    assert_initialized()
    PoiIdx.close()

def recreate_schema():
    assert_initialized()
    PoiIdx.recreate_schema()

def drop_schema():
    assert_initialized()
    PoiIdx.drop_schema()

def get_nearest_pois(shape: shapely.geometry.base.BaseGeometry, buffer: float | None= None, **kwargs):
    assert_initialized()
    regions = PoiIdx.init_regions_by_shape(shape, buffer=buffer)
    return PoiIdx.get_nearest_pois(shape, regions=regions, **kwargs)

def get_administrative_hierarchy(shape: shapely.geometry.base.BaseGeometry, buffer: float | None= None):
    assert_initialized()
    PoiIdx.init_regions_by_shape(shape, buffer=buffer)
    return PoiIdx.get_administrative_hierarchy(shape)

def get_administrative_hierarchy_string(shape: shapely.geometry.base.BaseGeometry, buffer: float | None= None):
    assert_initialized()
    PoiIdx.init_regions_by_shape(shape, buffer=buffer)
    return PoiIdx.get_administrative_hierarchy_string(shape)