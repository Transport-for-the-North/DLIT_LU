"""Performs the filtering process to select large development sites from the DLOG data.

"""

# standard imports
import logging
import pathlib

# third party imports
import pandas as pd
import geopandas as gpd
import numpy as np
from sklearn.preprocessing import MinMaxScaler

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


class SiteZoneProcessor(BaseZoneHandler):
    def merge_zonal_attributes(self, data: pd.DataFrame, by_data: pd.DataFrame) -> pd.DataFrame:
        """Merge the zonal attributes with the data based on the zone ID."""
        group_by_column = self.zone_info["group_by_column"]
        zone_gdf_id_col = self.zone_info["zone_gdf_id_col"]
        # Merge the zonal attributes with the data based on the zone ID
        data = data.merge(by_data, left_on=zone_gdf_id_col, right_on=group_by_column, how="left")
        return data
    def process_sites(self, data: pd.DataFrame):
        """Process residential and employment sites based on the zone boundary."""
        updated_data = self.zone_site_geospatial_lookup(data)
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
    
        # Select only the columns from the original data plus the zone_gdf_id_col
        updated_data = dlog_zone[[col for col in data.columns] + [self.zone_info["zone_gdf_id_col"]]]
        
        return updated_data


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


def calculate_density_and_index(by_data: pd.DataFrame, columns_to_process: list) -> pd.DataFrame:
    """
    Calculate density and index values for specified columns.

    Parameters
    ----------
    by_data : pd.DataFrame
        DataFrame containing columns to process and 'area_sqm'.
    columns_to_process : list
        List of column names to calculate density and index for.

    Returns
    -------
    pd.DataFrame
        Updated DataFrame with density and index columns added.
    """
    scaler = MinMaxScaler()
    # Loop through each column to calculate density and index

    for col in columns_to_process:
        density_col = f"{col[:2]}_den"
        by_data[density_col] = by_data[col] / by_data['area_sqm'] * 1000000
        index_col = f"{density_col}_index"
        by_data[index_col] = scaler.fit_transform(by_data[[density_col]])
    return by_data

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
    geo_boundary = config.dev_pattern.geo_boundary


    LOG.info("Creating list of sites with estimated development values")
    # list of residential sites with estimated values
    resi_estsite_reference_ids = get_site_reference_ids(
        site_assessment['residential'], 'missing_area', 'missing_gfa_or_dwellings_no_site_area'
    )
    # list of employment sites with estimated values
    emp_estsite_reference_ids = get_site_reference_ids(
        site_assessment['employment'], 'missing_area', 'missing_gfa_or_dwellings_no_site_area'
    )

    LOG.info("Processing base year land use data")    
    zone_translator = ZoneTranslator(geo_boundary, config)
    by_data = zone_translator.merge_data(by_data)
    columns_to_calden = ['household', 'population', 'jobs']
    by_data = calculate_density_and_index(by_data, columns_to_calden)


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



    # # Display the result
    # print("Residential Site Reference IDs:", resi_estsite_reference_ids)
    # print("Employment Site Reference IDs:", emp_estsite_reference_ids)

    LOG.info("Mapping development sites to pre_defined zone")    

    sitezone_processor = SiteZoneProcessor(geo_boundary, config)

    res_zone_sites = sitezone_processor.process_sites(res_sites)
    emp_zone_sites = sitezone_processor.process_sites(emp_sites)

    LOG.info("Getting attributes associated to development sites") 
    # Add the 'value_estimated' column for residential zone sites
    res_zone_sites['value_estimated'] = res_zone_sites['site_reference_id'].apply(
        lambda x: 'estimated' if x in resi_estsite_reference_ids else 'real'
    )
    emp_zone_sites['value_estimated'] = emp_zone_sites['site_reference_id'].apply(
        lambda x: 'estimated' if x in emp_estsite_reference_ids else 'real'
    )


    # Merge sites with associated zonal attributes
    res_zone_sites = sitezone_processor.merge_zonal_attributes(res_zone_sites, by_data)
    emp_zone_sites = sitezone_processor.merge_zonal_attributes(emp_zone_sites, by_data)


    # Calculate ratio of new development to existing development
    res_zone_sites['n_e_ratio'] = res_zone_sites['sum_from_2024_to_last'] / res_zone_sites['household']
    emp_zone_sites['n_e_ratio'] = emp_zone_sites['sum_from_2024_to_last'] / emp_zone_sites['jobs']
    print("Residential Sites Data:", res_zone_sites)


    LOG.info("Visualizing the distribution of the attributes of the development sites")
    columns_to_explore = [
        'sum_from_2024_to_last',
        'ho_den',
        'po_den', 
        'jo_den', 
        'n_e_ratio', 
        # 'dist_ho_c',
        # 'dist_po_c',
        # 'dist_jo_c',
        ]
    plot_path = config.output_folder / "plot_distribution_attributes"
    plot_path.mkdir(exist_ok=True)
    stats.plot_distribution(res_zone_sites, columns_to_explore, plot_path, category='Residential')
    stats.plot_distribution(emp_zone_sites, columns_to_explore, plot_path, category='Employment')

    LOG.info("Calculating basic statistics of the development sites")
    res_stats = stats.basic_statistics(res_zone_sites, columns_to_explore)
    emp_stats = stats.basic_statistics(emp_zone_sites, columns_to_explore)
    print("Residential Sites Statistics:", res_stats)
    print("Employment Sites Statistics:", emp_stats)
    res_low_density_zones = stats.percentiles_or_quantiles(res_zone_sites, columns_to_explore)
    emp_low_density_zones = stats.percentiles_or_quantiles(emp_zone_sites, columns_to_explore)
    print("Residential Low Density Zones:", res_low_density_zones)
    print("Employment Low Density Zones:", emp_low_density_zones)
    res_low_density_zones_z = stats.z_score_method(res_zone_sites, columns_to_explore)
    emp_low_density_zones_z = stats.z_score_method(emp_zone_sites, columns_to_explore)
    print("Residential Low Density Zones (Z-Score):", res_low_density_zones_z)
    print("Employment Low Density Zones (Z-Score):", emp_low_density_zones_z)


    LOG.info("Ending Development Pattern Module")
    return res_zone_sites, emp_zone_sites

    

