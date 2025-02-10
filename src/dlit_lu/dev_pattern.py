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
from scipy.spatial import cKDTree

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

        # Calculate density and index for specified columns
        columns_to_process = ['household', 'population', 'jobs']  # Example columns
        by_data = self.calculate_density_and_index(by_data, columns_to_process)

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
        # zone_df.drop(columns=[zone_gdf_id_col], inplace=True)

        return zone_df

    def calculate_density_and_index(self, by_data: pd.DataFrame, columns_to_process: list) -> pd.DataFrame:
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
            # Check if 'area_sqm' > 0, otherwise set density to 0
            by_data[density_col] = np.where(by_data['area_sqm'] > 0, by_data[col] / by_data['area_sqm'] * 1000000, 0)
            index_col = f"{density_col}_index"
            by_data[index_col] = scaler.fit_transform(by_data[[density_col]])

        return by_data

class SiteZoneProcessor(BaseZoneHandler):
    def merge_zonal_attributes(self, site_data: pd.DataFrame, by_data: pd.DataFrame) -> pd.DataFrame:
        """Merge the zonal attributes with the data based on the zone ID."""
        group_by_column = self.zone_info["group_by_column"]
        zone_gdf_id_col = self.zone_info["zone_gdf_id_col"]
        # Merge the zonal attributes with the data based on the zone ID
        site_data = site_data.merge(by_data, on=zone_gdf_id_col, how="left")
        return site_data
    # def process_sites(self, data: pd.DataFrame):
    #     """Process residential and employment sites based on the zone boundary."""
    #     updated_data = self.zone_site_geospatial_lookup(data)
    #     return updated_data
    def zone_site_geospatial_lookup(self, site_data: pd.DataFrame) -> gpd.GeoDataFrame:
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
            site_data, geometry=gpd.points_from_xy(site_data["easting"], site_data["northing"])
        )

        # Perform a spatial join between the DLOG points and the zone geometries
        dlog_zone = gpd.sjoin(dlog_geom, self.zone_gdf, how="left")
    
        # Select only the columns from the original data plus the zone_gdf_id_col
        updated_data = dlog_zone[[col for col in site_data.columns] + [self.zone_info["zone_gdf_id_col"]]]
        
        return updated_data
    
    def calculate_distance_to_zone_centroids(self, site_data: pd.DataFrame, centroid_type: str) -> pd.DataFrame:
        centroid_file_map = {
            # "lsoa": {
            #     "hh": self.config.dev_pattern.lsoa_hh_centroids,
            #     "emp": self.config.dev_pattern.lsoa_emp_centroids,
            #     "pop": self.config.dev_pattern.lsoa_pop_centroids,
            # },
            "normits": {
                "hh": self.config.dev_pattern.normits_hh_centroids,
                "emp": self.config.dev_pattern.normits_emp_centroids,
                "pop": self.config.dev_pattern.normits_pop_centroids,
            },
            # "noham": {
            #     "hh": self.config.dev_pattern.noham_hh_centroids,
            #     "emp": self.config.dev_pattern.noham_emp_centroids,
            #     "pop": self.config.dev_pattern.noham_pop_centroids,
            # },
            # "norms": {
            #     "hh": self.config.dev_pattern.norms_hh_centroids,
            #     "emp": self.config.dev_pattern.norms_emp_centroids,
            #     "pop": self.config.dev_pattern.norms_pop_centroids,
            # },
            # "msoa": {
            #     "hh": self.config.dev_pattern.msoa_hh_centroids,
            #     "emp": self.config.dev_pattern.msoa_emp_centroids,
            #     "pop": self.config.dev_pattern.msoa_pop_centroids,
            # }
        }
        
        if self.geo_boundary not in centroid_file_map:
            raise ValueError(f"Unsupported geo_boundary: {self.geo_boundary}")

        centroid_file = centroid_file_map[self.geo_boundary].get(centroid_type)
        if not centroid_file:
            raise ValueError("Invalid centroid type. Choose from 'hh', 'emp', or 'pop'.")

        centroids = pd.read_csv(centroid_file)
        site_coords = np.vstack((site_data["easting"], site_data["northing"])).T
        centroid_coords = np.vstack((centroids["x"], centroids["y"])).T

        tree = cKDTree(centroid_coords)
        distances, _ = tree.query(site_coords)
        site_data[f"dist_to_{centroid_type}_c"] = distances

        return site_data


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


def process_site_data(site_data: pd.DataFrame,  by_data: pd.DataFrame, site_type: str, site_reference_ids: list, sitezone_processor: SiteZoneProcessor,):
    LOG.info(f"Processing {site_type} site data")

    # Find the last column (year) in your dataframe
    last_year = site_data.columns[site_data.columns.str.isnumeric()].astype(int).max()

    # Create a new column 'sum_from_2024_to_last' which is the sum of the columns from 2024 to the last year
    site_data['sum_from_2024_to_last'] = site_data.loc[:, '2024':str(last_year)].sum(axis=1)
    columns_to_keep = ['site_reference_id', 'easting', 'northing', 'sum_from_2024_to_last']
    site_data = site_data[columns_to_keep]

    # Map development sites to pre-defined zone
    site_zone_sites = sitezone_processor.zone_site_geospatial_lookup(site_data)

    LOG.info(f"Calculating distance to zone centroids for {site_type} sites")
    centroid_types = ['hh', 'emp', 'pop']

    for centroid_type in centroid_types:
        site_zone_sites = sitezone_processor.calculate_distance_to_zone_centroids(site_zone_sites, centroid_type)

    LOG.info(f"Getting attributes associated with {site_type} development sites")
    # Add the 'value_estimated' column for zone sites
    site_zone_sites['value_estimated'] = site_zone_sites['site_reference_id'].apply(
        lambda x: 'estimated' if x in site_reference_ids else 'real'
    )

    # Merge sites with associated zonal attributes
    site_zone_sites = sitezone_processor.merge_zonal_attributes(site_zone_sites, by_data)

    # Calculate ratio of new development to existing development
    ratio_column = 'n_e_ratio'
    if site_type == 'Residential':
        # Calculate the ratio for Residential sites using household data
        site_zone_sites[ratio_column] = site_zone_sites['sum_from_2024_to_last'] / site_zone_sites['household']
    elif site_type == 'Employment':
        # Calculate the ratio for Employment sites using jobs data
        site_zone_sites[ratio_column] = site_zone_sites['sum_from_2024_to_last'] / site_zone_sites['jobs']
    else:
        # For any other site type, default ratio calculation (can be customized as needed)
        LOG.warning(f"Unknown site type: {site_type}. Defaulting to a ratio using jobs.")
        site_zone_sites[ratio_column] = site_zone_sites['sum_from_2024_to_last'] / site_zone_sites['jobs']

    return site_zone_sites

def process_stats(site_data: pd.DataFrame, site_type: str, columns_to_explore: list, plot_path: pathlib.Path):
    """
    This function processes the statistics for the given site data, generates plots, 
    and calculates percentiles, quantiles, and z-scores.
    
    :param site_data: The dataframe of processed site data (either residential or employment).
    :param site_type: A string indicating the type of site ('Residential' or 'Employment').
    :param columns_to_explore: A list of column names for which statistics will be calculated.
    :param plot_path: The path where plots will be saved.
    :return: None
    """
    # Visualize the distribution of attributes
    LOG.info(f"Visualizing the distribution of {site_type} site attributes")
    stats.plot_distribution(site_data, columns_to_explore, plot_path, category=site_type)

    # Calculate basic statistics
    LOG.info(f"Calculating basic statistics for {site_type} site data")
    print(f"Total number of rows for {site_type} data:", len(site_data))
    print(f"Total number of columns for {site_type} data:",len(site_data.columns))
    site_stats= stats.basic_statistics(site_data, columns_to_explore)

    site_z_scores = stats.calculate_z_scores(site_data, columns_to_explore)
    print(f"Total number of rows for {site_type} z_scores data:", len(site_data))
    print(f"Total number of columns for {site_type} z_scores data:",len(site_data.columns))

    site_z_scores_stats= stats.basic_statistics(site_z_scores, columns_to_explore)
    # Concatenate site_stats and site_z_scores_stats along columns (axis=1)
    combined_stats = pd.concat([site_stats, site_z_scores_stats], axis=1)
    print(f"{site_type} Sites Statistics:", site_stats)
    print(f"{site_type} Sites Z_score Statistics:", site_z_scores_stats)
    # Calculate percentiles or quantiles for low-density zones
    low_density_zones = stats.percentiles_or_quantiles(site_data, columns_to_explore)
    print(f"{site_type} Low Density Zones:", low_density_zones)

    # Calculate z-scores for low-density zones
    # low_density_zones_z = stats.z_score_method(site_data, columns_to_explore)
    # print(f"{site_type} Low Density Zones (Z-Score):", low_density_zones_z)

    return combined_stats, site_z_scores 



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
    # columns_to_calden = ['household', 'population', 'jobs']
    # by_data = calculate_density_and_index(by_data, columns_to_calden)


    LOG.info("Processing site data for year 2024 upwards")
    res_zone_sites = process_site_data(res_sites, by_data, 'Residential', resi_estsite_reference_ids, SiteZoneProcessor(geo_boundary, config))
    emp_zone_sites = process_site_data(emp_sites, by_data, 'Employment', emp_estsite_reference_ids, SiteZoneProcessor(geo_boundary, config))
    print("Residential Sites Data:", res_zone_sites)
    print("Employment Sites Data:", emp_zone_sites)
    # columns_to_keep = [zoneconfig.zone_info["zone_gdf_id_col"], 
    #                    'easting', 
    #                    'northing', 
    #                    'normits_id', 
    #                    'household', 
    #                    'population', 
    #                    'jobs', 
    #                    'area_sqm', 
    #                    'value_estimated',
    #                    'sum_from_2024_to_last', 
    #                    'dist_to_hh_c', 
    #                    'dist_to_emp_c', 
    #                    'dist_to_pop_c', 
    #                    'n_e_ratio', 
    #                    'ho_den', 
    #                    'po_den', 
    #                    'jo_den']
    # res_zone_sites = res_zone_sites[columns_to_keep]
    # emp_zone_sites = emp_zone_sites[columns_to_keep]
    res_file_name = "residential_site_zone.csv"
    emp_file_name = "employment_site_zone.csv"
    utilities.write_to_csv(config.output_folder / res_file_name, res_zone_sites)
    utilities.write_to_csv(config.output_folder / emp_file_name, emp_zone_sites)

    # Columns to explore for both residential and employment sites
    columns_to_explore = [
        'sum_from_2024_to_last',
        'ho_den',
        'po_den', 
        'jo_den', 
        'n_e_ratio', 
        'dist_to_hh_c',
        'dist_to_emp_c',
        'dist_to_pop_c',
    ]
    plot_path = config.output_folder / "plot_distribution_attributes"
    plot_path.mkdir(exist_ok=True)

    # Process statistics for residential and employment sites
    res_combined_stats, res_z_scores = process_stats(
        res_zone_sites, 
        'Residential', 
        columns_to_explore, 
        plot_path)
    emp_combined_stats, emp_z_scores = process_stats(
        emp_zone_sites, 
        'Employment', 
        columns_to_explore, 
        plot_path)
    res_combined_stats_file = "residential_sites_combined_stats.csv"
    res_z_scores_file = "residential_sites_z_scores.csv"
    emp_combined_stats_file = "employment_sites_combined_stats.csv"
    emp_z_scores_file = "employment_sites_z_scores.csv"
    utilities.write_to_csv(config.output_folder / res_combined_stats_file, res_combined_stats)
    utilities.write_to_csv(config.output_folder / res_z_scores_file, res_z_scores)
    utilities.write_to_csv(config.output_folder / emp_combined_stats_file, emp_combined_stats)
    utilities.write_to_csv(config.output_folder / emp_z_scores_file, emp_z_scores)
  

    LOG.info("Ending Development Pattern Module")
    return res_zone_sites, emp_zone_sites

# if __name__ == "__main__":

    

