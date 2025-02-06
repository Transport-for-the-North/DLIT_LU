"""Performs the filtering process to select large development sites from the DLOG data.

"""

# standard imports
import logging
import pathlib

# third party imports
import pandas as pd
import geopandas as gpd
import numpy as np

# local imports
from dlit_lu import stats, utilities, global_classes, parser, inputs, data_repair
from dlit_lu import land_use as lu

# constants
LOG = logging.getLogger(__name__)

class BaseZoneHandler:
    def __init__(self, geo_boundary: str, config: inputs.DLitConfig):
        self.geo_boundary = geo_boundary
        self.config = config
        self.zone_info_map = {
            "lsoa": {
                "shapefile_path": config.land_use.lsoa_shapefile_path,
                "group_by_column": "lsoa2021_id",
                "zone_gdf_id_col": "LSOA21CD",
                "translation_path": None  # No translation needed for LSOA
            },
            "normits": {
                "shapefile_path": config.dev_pattern.normits_shapefile_path,
                "group_by_column": "normits_v3_3_id",
                "zone_gdf_id_col": "normits_id",
                "translation_path": config.dev_pattern.lsoa_to_normits
            },
            "noham": {
                "shapefile_path": config.dev_pattern.noham_shapefile_path,
                "group_by_column": "noham_id",
                "zone_gdf_id_col": "id",
                "translation_path": config.dev_pattern.lsoa_to_noham
            },
            "norms": {
                "shapefile_path": config.dev_pattern.norms_shapefile_path,
                "group_by_column": "norms_id",
                "zone_gdf_id_col": "unique_id",
                "translation_path": config.dev_pattern.lsoa_to_norms
            },
            "msoa": {
                "shapefile_path": config.dev_pattern.msoa_shapefile_path,
                "group_by_column": "msoa2021_id",
                "zone_gdf_id_col": "MSOA21CD",
                "translation_path": config.dev_pattern.lsoa_to_msoa
            }
        }

        if geo_boundary not in self.zone_info_map:
            raise ValueError(f"Unsupported geo_boundary: {geo_boundary}")

        self.zone_info = self.zone_info_map[geo_boundary]
        self.zone_gdf = parser.parse_zone(self.zone_info["shapefile_path"])


class ZoneProcessor(BaseZoneHandler):
    def process_sites(self, data: pd.DataFrame):
        """Process residential and employment sites based on the zone boundary."""
        updated_data = self.zone_site_geospatial_lookup(data, self.zone_gdf)
        return updated_data
    def zone_site_geospatial_lookup(self, data: pd.DataFrame) -> gpd.GeoDataFrame:
        """Spatially joins site data (DLOG sites) to the zones (e.g., MSOA shapefile) based on location.

        Parameters
        ----------
        data : pd.DataFrame
            DataFrame containing site information with 'easting' and 'northing' columns for coordinates.

        Returns
        -------
        gpd.GeoDataFrame
            A GeoDataFrame with the spatially joined data.
        """
        # Convert the 'data' DataFrame to a GeoDataFrame with geometry based on coordinates
        dlog_geom = gpd.GeoDataFrame(
            data, geometry=gpd.points_from_xy(data["easting"], data["northing"])
        )

        # Perform a spatial join between the DLOG points and the zone geometries
        dlog_zone = gpd.sjoin(dlog_geom, self.zone_gdf, how="left")
        
        return dlog_zone


class ZoneTranslator(BaseZoneHandler):
    def merge_data(self, by_data: pd.DataFrame) -> pd.DataFrame:
        """Merge zone translation data and perform aggregation."""
        group_by_column = self.zone_info["group_by_column"]
        zone_gdf_id_col = self.zone_info["zone_gdf_id_col"]
        translation_path = self.zone_info["translation_path"]
        # Merge data with translation if needed
        by_data = self.merge_translation_data(by_data, translation_path)

        # Perform aggregation by group
        by_data = self.aggregate_by_zone(by_data, group_by_column)

        # Compute zonal area by merging geometry data
        by_data = self.compute_zonal_area(self.zone_gdf, by_data, zone_gdf_id_col, group_by_column)

        return by_data

    def merge_translation_data(self, by_data: pd.DataFrame, translation_path: str) -> pd.DataFrame:
        """Merge the zone translation data with the input dataframe."""
        if translation_path:
            zone_translation = pd.read_csv(translation_path)
            by_data = by_data.merge(zone_translation, on="lsoa2021_id", how="left")
        return by_data

    def aggregate_by_zone(self, by_data: pd.DataFrame, group_by_column: str) -> pd.DataFrame:
        """Group data by the zone and aggregate household, population, and jobs."""
        return by_data.groupby(group_by_column, as_index=False)[["household", "population", "jobs"]].sum()

    def compute_zonal_area(self, zone_gdf: gpd.GeoDataFrame, zone_df: pd.DataFrame, zone_gdf_id_col: str, zone_df_id_col: str, crs_target=27700):
        """
        Compute the area of each zone and merge it into the zone dataframe.

        Parameters:
        - zone_gdf (GeoDataFrame): The GeoDataFrame containing zone geometries.
        - zone_df (DataFrame): The DataFrame containing zone information.
        - zone_gdf_id_col (str): The common identifier column between zone_gdf and zone_df.
        - zone_df_id_col (str): The common identifier column in zone_df to join with zone_gdf.
        - crs_target (int): The EPSG code for the target CRS (default: 27700 for British National Grid).

        Returns:
        - DataFrame: The updated zone_df with an additional 'area_sqm' column.
        """
        # Check if CRS is defined
        if zone_gdf.crs is None:
            raise ValueError("Shapefile has no CRS defined. Please check the source data.")

        # Ensure it's projected in the correct CRS
        if not zone_gdf.crs.is_projected or zone_gdf.crs.to_epsg() != crs_target:
            zone_gdf = zone_gdf.to_crs(epsg=crs_target)

        # Compute area in square meters
        zone_gdf["area_sqm"] = zone_gdf.geometry.area

        # Merge area information into zone_data DataFrame
        zone_df = zone_df.merge(zone_gdf[[zone_gdf_id_col, "area_sqm"]], left_on=zone_df_id_col, right_on=zone_gdf_id_col, how="left")

        return zone_df

def get_site_reference_ids(site_df: pd.DataFrame, missing_area_col: str, missing_gfa_col: str) -> list:
    """
    Function to add a 'value_estimated' column and retrieve site_reference_ids where the value is 'estimated'.
    
    Parameters:
        site_df (pd.DataFrame): The dataframe for either residential or employment sites.
        missing_area_col (str): Column name for missing_area (e.g., 'missing_area').
        missing_gfa_col (str): Column name for missing_gfa_or_dwellings_no_site_area (e.g., 'missing_gfa_or_dwellings_no_site_area').
        
    Returns:
        list: A list of site_reference_ids where 'value_estimated' is 'estimated'.
    """
    # Add the 'value_estimated' column
    site_df['value_estimated'] = site_df.apply(
        lambda row: 'estimated' if row[missing_area_col] and row[missing_gfa_col] else 'real', 
        axis=1
    )
    
    # Filter the dataframe for rows where 'value_estimated' is 'estimated'
    estimated_sites = site_df[site_df['value_estimated'] == 'estimated']
    
    # Return the list of site_reference_ids
    return estimated_sites['site_reference_id'].tolist()


def run(input_data: global_classes.AssessData, config: inputs.DLitConfig):
    """runs process for converting DLOG to MSOA build out profiles

    disaggregaes mixed into employment and residential and land use codes
    applys dwelling types using land use split by MSOA
    rebases to MSOA build-out profiles

    Parameters
    ----------
    input_data : global_classes.AssessData
        data to further categorise site data, whether they are estimated or not
    config : inputs.DLitConfig
        config file
    """
    if config.dev_pattern is None:
        raise ValueError("cannot run development pattern without any dev_pattern parameters")

    LOG.info("Initialising Development Pattern Module")

    config.output_folder.mkdir(exist_ok=True)

    site_assessment = lu.disagg_mixed(utilities.to_dict(input_data))
    emp_sites = pd.read_csv(config.dev_pattern.emp_site_data)
    res_sites = pd.read_csv(config.dev_pattern.res_site_data)
    by_data = pd.read_csv(config.dev_pattern.lsoa_data_path)


    LOG.info("Getting subset of future development from 2024 onwards")
    # Find the last column (year) in your dataframe
    last_year_emp = emp_sites.columns[emp_sites.columns.str.isnumeric()].astype(int).max()
    last_year_res = res_sites.columns[emp_sites.columns.str.isnumeric()].astype(int).max()
    # Create a new column 'sum_from_2024_to_last' which is the sum of the columns from 2024 to the last year
    emp_sites['sum_from_2024_to_last'] = emp_sites.loc[:, '2024':str(last_year_emp)].sum(axis=1)
    res_sites['sum_from_2024_to_last'] = res_sites.loc[:, '2024':str(last_year_res)].sum(axis=1)
    columns_to_keep = ['site_reference_id', 'easting', 'northing', 'sum_from_2024_to_last']
    emp_sites = emp_sites[columns_to_keep]
    res_sites = res_sites[columns_to_keep]

    LOG.info("Creating list of sites with estimated development values")
    # list of residential sites with estimated values
    resi_estsite_reference_ids = get_site_reference_ids(
        site_assessment['residential'], 'missing_area', 'missing_gfa_or_dwellings_no_site_area'
    )

    # list of employment sites with estimated values
    emp_estsite_reference_ids = get_site_reference_ids(
        site_assessment['employment'], 'missing_area', 'missing_gfa_or_dwellings_no_site_area'
    )

    # Display the result
    print("Residential Site Reference IDs:", resi_estsite_reference_ids)
    print("Employment Site Reference IDs:", emp_estsite_reference_ids)


    LOG.info("Getting zonal attributes associated to development sites")    
    geo_boundary = config.dev_pattern.geo_boundary
    zone_processor = ZoneProcessor(geo_boundary, config)
    zone_translator = ZoneTranslator(geo_boundary, config)
    res_zone_sites = zone_processor.process_sites(res_sites)
    emp_zone_sites = zone_processor.process_sites(emp_sites)
    by_data = zone_translator.merge_data(by_data)

    # Add the 'value_estimated' column for residential zone sites
    res_zone_sites['value_estimated'] = res_zone_sites['site_reference_id'].apply(
        lambda x: 'estimated' if x in resi_estsite_reference_ids else 'real'
    )
    emp_zone_sites['value_estimated'] = emp_zone_sites['site_reference_id'].apply(
        lambda x: 'estimated' if x in emp_estsite_reference_ids else 'real'
    )



# # Function to process zone sites based on geo_boundary
# def process_geo_sites(geo_boundary: str, config: inputs.DLitConfig, res_sites: pd.DataFrame, emp_sites:pd.DataFrame):
#     if geo_boundary == 'lsoa':
#         lsoa = parser.parse_zone(config.land_use.lsoa_shapefile_path)
#         res_zone_sites = zone_site_geospatial_lookup(res_sites, lsoa)
#         emp_zone_sites = zone_site_geospatial_lookup(emp_sites, lsoa)

#     elif geo_boundary == 'normits':
#         normits = parser.parse_zone(config.dev_pattern.normits_shapefile_path)
#         res_zone_sites = zone_site_geospatial_lookup(res_sites, normits)
#         emp_zone_sites = zone_site_geospatial_lookup(emp_sites, normits)

#     elif geo_boundary == 'noham':
#         noham = parser.parse_zone(config.dev_pattern.noham_shapefile_path)
#         res_zone_sites = zone_site_geospatial_lookup(res_sites, noham)
#         emp_zone_sites = zone_site_geospatial_lookup(emp_sites, noham)

#     elif geo_boundary == 'norms':
#         norms = parser.parse_zone(config.dev_pattern.norms_shapefile_path)
#         res_zone_sites = zone_site_geospatial_lookup(res_sites, norms)
#         emp_zone_sites = zone_site_geospatial_lookup(emp_sites, norms)

#     elif geo_boundary == 'msoa':
#         msoa = parser.parse_zone(config.dev_pattern.msoa_shapefile_path)
#         res_zone_sites = zone_site_geospatial_lookup(res_sites, msoa)
#         emp_zone_sites = zone_site_geospatial_lookup(emp_sites, msoa)

#     return res_zone_sites, emp_zone_sites

# # Function to merge zone translation data based on geo_boundary
# def merge_zone_translation_data(geo_boundary: str, config:inputs.DLitConfig, by_data: pd.DataFrame):
#     if geo_boundary == 'lsoa':
#         # No need for zone translation for LSOA
#         zone_shapefile_path = config.land_use.lsoa_shapefile_path
#         by_data = compute_zonal_area(by_data, zone_shapefile_path, zone_id_col="lsoa2021_id")
#         return by_data

#     # Load the appropriate zone translation file and merge with by_data
#     zone_translation_path = None
#     group_by_column = None

#     if geo_boundary == 'normits':
#         zone_translation_path = config.dev_pattern.lsoa_to_normits
#         zone_shapefile_path = config.dev_pattern.normits_shapefile_path
#         group_by_column = "normits_v3_3_id"
#     elif geo_boundary == 'noham':
#         zone_translation_path = config.dev_pattern.lsoa_to_noham
#         zone_shapefile_path = config.dev_pattern.noham_shapefile_path
#         group_by_column = "noham_id"
#     elif geo_boundary == 'norms':
#         zone_translation_path = config.dev_pattern.lsoa_to_norms
#         zone_shapefile_path = config.dev_pattern.norms_shapefile_path
#         group_by_column = "norms_id"
#     elif geo_boundary == 'msoa':
#         zone_translation_path = config.dev_pattern.lsoa_to_msoa
#         zone_shapefile_path = config.dev_pattern.msoa_shapefile_path
#         group_by_column = "msoa2021_id"
#     # Load the zone translation file
#     zone_translation = pd.read_csv(zone_translation_path)

#     # Merge by_data with the zone translation data
#     by_data = by_data.merge(zone_translation, on="lsoa2021_id", how="left")

#     # Group by the appropriate column and sum the values
#     by_data = by_data.groupby(group_by_column)[["household", "population", "jobs"]].sum()

#     return by_data

# def process_geo_boundary(config, parser., res_sites, emp_sites):
#     geo_boundary = config.dev_pattern.geo_boundary.lower()
#     boundary_map = {
#         "lsoa": config.land_use.lsoa_shapefile_path,
#         "normits_v3_3": config.dev_pattern.normits_shapefile_path,
#         "noham": config.dev_pattern.noham_shapefile_path,
#         "norms": config.dev_pattern.norms_shapefile_path,
#         "msoa": config.dev_pattern.msoa_shapefile_path,
#     }
    
#     if geo_boundary in boundary_map:
#         boundary_data = parser.parse_zone(boundary_map[geo_boundary])
#         res_zone_sites = zone_site_geospatial_lookup(res_sites, boundary_data)
#         emp_zone_sites = zone_site_geospatial_lookup(emp_sites, boundary_data)
#         return res_zone_sites, emp_zone_sites
#     else:
#         raise ValueError(f"Invalid geo_boundary: {geo_boundary}")

# def compute_zonal_area(zone_gdf, zone_df, zone_id_col="ZoneID", crs_target=27700):
#     """
#     Compute the area of each zone and merge it into the zone dataframe.

#     Parameters:
#     - zone_gdf (GeoDataFrame): The GeoDataFrame containing zone geometries.
#     - zone_df (DataFrame): The DataFrame containing zone information.
#     - zone_id_col (str): The common identifier column between zone_gdf and zone_df.
#     - crs_target (int): The EPSG code for the target CRS (default: 27700 for British National Grid).

#     Returns:
#     - DataFrame: The updated zone_df with an additional 'area_sqm' column.
#     """
#     # Check if CRS is defined
#     if zone_gdf.crs is None:
#         raise ValueError("Shapefile has no CRS defined. Please check the source data.")

#     # Ensure it's projected in the correct CRS
#     if not zone_gdf.crs.is_projected or zone_gdf.crs.to_epsg() != crs_target:
#         zone_gdf = zone_gdf.to_crs(epsg=crs_target)

#     # Compute area in square meters
#     zone_gdf["area_sqm"] = zone_gdf.geometry.area

#     # Merge area information into zone_data DataFrame
#     zone_df = zone_df.merge(zone_gdf[[zone_id_col, "area_sqm"]], on=zone_id_col, how="left")

#     return zone_df

# def msoa_site_geospatial_lookup(
#     data: pd.DataFrame,
#     msoa: gpd.GeoDataFrame,
# ) -> gpd.GeoDataFrame:
#     """spatially joins MSOA shapefile to DLOG sites


#     Parameters
#     ----------
#     data : pd.DataFrame
#         data to join to msoa
#     msoa : gpd.GeoDataFrame
#         msoa data

#     Returns
#     -------
#     gpd.GeoDataFrame
#         spatially joined data
#     """

#     dlog_geom = gpd.GeoDataFrame(
#         data, geometry=gpd.points_from_xy(data["easting"], data["northing"])
#     )
#     dlog_msoa = gpd.sjoin(dlog_geom, msoa, how="left")
#     return dlog_msoa

# def zone_site_geospatial_lookup(
#     data: pd.DataFrame,
#     zone: gpd.GeoDataFrame,
# ) -> gpd.GeoDataFrame:
#     """spatially joins MSOA shapefile to DLOG sites


#     Parameters
#     ----------
#     data : pd.DataFrame
#         data to join to msoa
#     msoa : gpd.GeoDataFrame
#         msoa data

#     Returns
#     -------
#     gpd.GeoDataFrame
#         spatially joined data
#     """

#     dlog_geom = gpd.GeoDataFrame(
#         data, geometry=gpd.points_from_xy(data["easting"], data["northing"])
#     )
#     dlog_zone = gpd.sjoin(dlog_geom, zone, how="left")
#     return dlog_zone



