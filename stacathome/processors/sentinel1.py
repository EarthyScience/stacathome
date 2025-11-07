import re
from copy import deepcopy

import pystac
import rasterio
from odc.geo import geom
from pystac.extensions.projection import ProjectionExtension
from pystac.extensions.raster import RasterExtension
from rasterio.env import Env
from rio_stac.stac import get_projection_info, get_raster_info

from ..providers import SimpleProvider
from .base import register_default_processor, SimpleProcessor
from .common import get_property, no_overlap_filter_coverage
from .ecostress import handle_rasterio_env


class Sentinel1OperaL2RTCProcessor(SimpleProcessor):

    def filter_items(
        self,
        provider: SimpleProvider,
        roi: geom.Geometry,
        items: pystac.ItemCollection,
        variables: list[str] | None = None,
        temp_path: str | None = None,
    ) -> pystac.ItemCollection:

        if temp_path:
            items = provider.load_granule(items, variables, out_dir=temp_path)

        if not get_property(items[0], 'proj:code'):
            update_from_raster(items)

        items_filtered = no_overlap_filter_coverage(items, roi)

        return pystac.ItemCollection(
            items=items_filtered,
            clone_items=False,
            extra_fields=items.extra_fields,
        )

    def get_burst_id(self, item):

        # expecting item (or id string) with id like
        # 'OPERA_L2_RTC-S1-STATIC_{burst_id}_20140403_S1{sentinel_letter}_30_v1'
        # Example for valid burst id: "T043-092197-IW1"
        if isinstance(item, str):
            b_id = item.split('_')[3]
        else:
            b_id = item.id.split('_')[3]
        if not (b_id.startswith('T') and len(b_id) == 15):
            raise ValueError('faulty burst id!')
        return b_id

    def overwrite_items_to_access_static_data(self, item):

        item = deepcopy(item)

        burst_id = self.get_burst_id(item)

        sentinel_letter = re.search(r"_S1([A-Za-z])", item.id).group(1)

        static_variables = [
            "incidence_angle",
            "local_incidence_angle",
            "number_of_looks",
            "rtc_anf_gamma0_to_beta0",
            "rtc_anf_gamma0_to_sigma0",
        ]

        item.__setattr__('id', f'OPERA_L2_RTC-S1-STATIC_{burst_id}_20140403_S1{sentinel_letter}_30_v1')
        item.properties['start_datetime'] = "2014-04-03T00:00:00Z"
        item.properties['end_datetime'] = "2014-04-03T00:00:00Z"
        item.properties['datetime'] = "2014-04-03T00:00:00Z"

        item.assets = {
            variable: pystac.Asset(
                href='https://datapool.asf.alaska.edu/RTC-STATIC/OPERA-S1/'
                f'OPERA_L2_RTC-S1-STATIC_{burst_id}_20140403_S1{sentinel_letter}_30_v1.0_{variable}.tif',
                media_type=pystac.MediaType.COG,
                roles=["data"],
            )
            for variable in static_variables
        }
        return item


#     """
#     Processor for Sentinel-1 Opera L2 RTC data from EarthAccess/ASF.
#     - the data comes in 30 m burst(?) tiles oriented in utm zones
#     - decide if this should be used, the quality of the data is good, just lower resolution.
#     - aggregation per orbit_nr of same grid over time, but maybe it differs in pixels?
#     """


def update_from_raster(items: list[pystac.Item], proj=True, raster=False) -> list[pystac.Item]:
    """too slow for usage from remote data source, download granules first"""
    with Env(**handle_rasterio_env()):
        for i, _ in enumerate(items):
            proj_info = {}
            ProjectionExtension.add_to(items[i])
            RasterExtension.add_to(items[i])
            for name, asset in items[i].get_assets().items():
                # print(f"Processing asset: {name} - {asset.href}")
                with rasterio.open(asset.href) as src_dst:
                    if proj and not proj_info:
                        proj_info_set = get_projection_info(src_dst).items()
                        proj_info = {
                            f"proj:{pname}": value
                            for pname, value in proj_info_set
                            if pname in ['epsg', 'code', 'bbox', 'shape', 'transform']
                        }
                        if 'proj:epsg' in proj_info and 'proj:code' not in proj_info:
                            proj_info['proj:code'] = proj_info['proj:epsg']
                            del proj_info['proj:epsg']
                    if raster:
                        items[i].assets[name].extra_fields['raster:bands'] = get_raster_info(src_dst, max_size=1024)
            if proj and proj_info:
                proj_ext = ProjectionExtension.ext(items[i])
                proj_ext.epsg = proj_info['proj:code']
                proj_ext.bbox = proj_info['proj:bbox']
                proj_ext.shape = proj_info['proj:shape']
                proj_ext.transform = proj_info['proj:transform']
    return items


# class Sentinel1OperaL2RTCStaticProcessor(Sentinel1OperaL2RTCProcessor):

#     def overwrite_items_to_access_static_data(self, item, burst_id=None):

#         burst_id = burst_id if burst_id else self.get_burst_id(item)

#         static_variables = [
#             "incidence_angle",
#             "local_incidence_angle",
#             "number_of_looks",
#             "rtc_anf_gamma0_to_beta0",
#             "rtc_anf_gamma0_to_sigma0",
#         ]

#         item.__setattr__('id', f'OPERA_L2_RTC-S1-STATIC_{burst_id}_20140403_S1A_30_v1')
#         item.properties['start_datetime'] = "2014-04-03T00:00:00Z"
#         item.properties['end_datetime'] = "2014-04-03T00:00:00Z"
#         item.properties['datetime'] = "2014-04-03T00:00:00Z"

#         item.assets = {
#             variable: pystac.Asset(
#                 href='https://datapool.asf.alaska.edu/RTC-STATIC/OPERA-S1/'
#                 f'OPERA_L2_RTC-S1-STATIC_{burst_id}_20140403_S1A_30_v1.0_{variable}.tif',
#                 media_type=pystac.MediaType.COG,
#                 roles=["data"],
#             )
#             for variable in static_variables
#         }
#         return item

#     # def filter_items(
#     #     self,
#     #     provider: SimpleProvider,
#     #     roi: geom.Geometry,
#     #     items: pystac.ItemCollection,
#     #     variables: list[str] = None,
#     #     temp_path: Path = None,
#     # ) -> pystac.ItemCollection:

#     #     burst_ids = {}
#     #     for item in items:
#     #         if not self.get_burst_id(item) in burst_ids:
#     #             burst_ids[self.get_burst_id(item)] = self.overwrite_items_to_access_static_data(item)

#     #     items = [burst_ids[b] for b in burst_ids]  # sorted(burst_ids)]

#     #     if temp_path:
#     #         items = provider.load_granule(items, variables=variables, out_dir=temp_path)

#     #     if not get_property(items[0], 'proj:code'):
#     #         update_from_raster(items)

#     #     items_filtered = no_overlap_filter_coverage(items, roi)

#     #     return pystac.ItemCollection(
#     #         items=items_filtered,
#     #         clone_items=False,
#     #         extra_fields=items.extra_fields,
#     #     )


class Sentinel1RTCProcessor(SimpleProcessor):
    # tiles do not overlap, no regular grid to take advantage from
    pass


register_default_processor('planetary_computer', 'sentinel-1-rtc', Sentinel1RTCProcessor)
register_default_processor('earthaccess', 'sentinel-1-opera-l2rtc', Sentinel1OperaL2RTCProcessor)
# register_default_processor('earthaccess', 'sentinel-1-opera-l2rtc-static', Sentinel1OperaL2RTCStaticProcessor)
