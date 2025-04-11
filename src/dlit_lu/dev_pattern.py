"""Performs the filtering process to select large development sites from the DLOG data.

"""

# standard imports
import logging
import pathlib
from pathlib import Path
from typing import Optional, Dict, Any, List, Union

# third party imports
import pandas as pd
import geopandas as gpd
import numpy as np
# from sklearn.preprocessing import MinMaxScaler
from scipy.spatial import cKDTree
# import os

# local imports
from dlit_lu import stats, utilities, global_classes, parser, inputs
from dlit_lu import land_use as lu
from caf.base.data_structures import DVector
import caf.base as cb
# from caf.toolkit import iterative_proportional_fitting as ipfn 

# constants
LOG = logging.getLogger(__name__)


class BaseZoneHandler:
    def __init__(self, config: inputs.DLitConfig):
        self.geo_boundary: inputs.GeoBoundary = config.dev_pattern.geo_boundary
        self.config: inputs.DLitConfig = config
        self.zone_info_map: Dict[inputs.GeoBoundary, Dict[str, Any]] = {
            inputs.GeoBoundary.LSOA: {
                "shapefile_path": config.land_use.lsoa_shapefile_path,
                "group_by_column": "lsoa2021_id",
                "zone_gdf_id_col": "LSOA21CD",
                "prop_column": None,  # No proportion column needed for LSOA
                "translation_path": None,  # No translation needed for LSOA
                "centroid_files": {
                    "hh": config.dev_pattern.lsoa_hh_centroids,
                    "emp": config.dev_pattern.lsoa_emp_centroids,
                    "pop": config.dev_pattern.lsoa_pop_centroids,
                },
                "zone_to_lad_path": config.dev_pattern.summary_data.lsoa_to_lad_file,
                "lad_id_col": "lad2013_id",
                "zone_to_lad_prop": "lsoa2021_to_lad2013",
            },
            inputs.GeoBoundary.NORMITS: {
                "shapefile_path": config.dev_pattern.normits_shapefile_path,
                "group_by_column": "normits_v3.3_id",
                "zone_gdf_id_col": "normits_id",
                "prop_column": "lsoa2021_to_normits_v3.3",
                "translation_path": config.dev_pattern.lsoa_to_normits,
                "centroid_files": {
                    "hh": config.dev_pattern.normits_hh_centroids,
                    "emp": config.dev_pattern.normits_emp_centroids,
                    "pop": config.dev_pattern.normits_pop_centroids,
                },
                "zone_to_lad_path": config.dev_pattern.summary_data.normits_to_lad_file,
                "lad_id_col": "lad2013_id",
                "zone_to_lad_prop": "normits_v3.3_to_lad2013",
                "ntem_hh_car_ratio": config.dev_pattern.normits_hh_car,
                "ntem_pop_car_ratio": config.dev_pattern.normits_pop_car,
                "ntem_pop_age_ratio": config.dev_pattern.normits_pop_age,
            },
            inputs.GeoBoundary.NOHAM: {
                "shapefile_path": config.dev_pattern.noham_shapefile_path,
                "group_by_column": "noham_v3.7_id",
                "zone_gdf_id_col": "ZONE ID_v3",
                "prop_column": "lsoa2021_to_noham_v3.7",
                "translation_path": config.dev_pattern.lsoa_to_noham,
                "centroid_files": {
                    "hh": config.dev_pattern.noham_hh_centroids,
                    "emp": config.dev_pattern.noham_emp_centroids,
                    "pop": config.dev_pattern.noham_pop_centroids,
                },
                "zone_to_lad_path": config.dev_pattern.summary_data.noham_to_lad_file,
                "lad_id_col": "lad2013_id",
                "zone_to_lad_prop": "noham_v3.7_to_lad2013",
            },
            inputs.GeoBoundary.NORMS: {
                "shapefile_path": config.dev_pattern.norms_shapefile_path,
                "group_by_column": "norms_v3.3_id",
                "zone_gdf_id_col": "unique_id",
                "prop_column": "lsoa2021_to_norms_v3.3",
                "translation_path": config.dev_pattern.lsoa_to_norms,
                "centroid_files": {
                    "hh": config.dev_pattern.norms_hh_centroids,
                    "emp": config.dev_pattern.norms_emp_centroids,
                    "pop": config.dev_pattern.norms_pop_centroids,
                },
                "zone_to_lad_path": config.dev_pattern.summary_data.norms_to_lad_file,
                "lad_id_col": "lad2013_id",
                "zone_to_lad_prop": "norms_v3.3_to_lad2013",
            },
            inputs.GeoBoundary.MSOA: {
                "shapefile_path": config.dev_pattern.msoa_shapefile_path,
                "group_by_column": "msoa2021_id",
                "zone_gdf_id_col": "MSOA21CD",
                "prop_column": "lsoa2021_to_msoa2021",
                "translation_path": config.dev_pattern.lsoa_to_msoa,
                "centroid_files": {
                    "hh": config.dev_pattern.msoa_hh_centroids,
                    "emp": config.dev_pattern.msoa_emp_centroids,
                    "pop": config.dev_pattern.msoa_pop_centroids,
                },
                "zone_to_lad_path": config.dev_pattern.summary_data.msoa_to_lad_file,
                "lad_id_col": "lad2013_id",
                "zone_to_lad_prop": "msoa2021_to_lad2013",
            },
        }

        if self.geo_boundary not in self.zone_info_map:
            raise ValueError(f"Unsupported geo_boundary: {self.geo_boundary}")

        self.zone_info: Dict[str, Any] = self.zone_info_map[self.geo_boundary]
        self.zone_gdf = parser.parse_zone(self.zone_info["shapefile_path"])


class ZoneTranslator(BaseZoneHandler):
    def __init__(self, config):
        super().__init__(config)
    def merge_data(
        self, 
        by_data: pd.DataFrame,
        columns_to_process: Optional[list[str]]=None,
        dimension_columns: Optional[list] = None,
        by_column: Optional[str]=None,
        fy_columns: Optional[list[str]]=None,
        new_hh_data: Optional[pd.DataFrame]=None,
        new_pop_data: Optional[pd.DataFrame]=None,
        new_job_data: Optional[pd.DataFrame]=None,
        merge_translation_data: bool = True,
        aggregate_by_zone: bool = True,
        compute_area: bool = True, 
        calculate_density: bool = True,
        model_zone_data: bool = True
    ) -> pd.DataFrame:
        """
        Merge zone translation data and perform aggregation.

        Parameters
        ----------
        by_data : pd.DataFrame
            Input data containing zone-level information.
        by_column : Optional[str], default=None
            Column name used for aggregation (e.g., financial year).
        fy_columns : Optional[list[str]], default=None
            List of financial year columns for cumulative calculations.
        new_hh_data : Optional[pd.DataFrame], default=None
            DataFrame with updated household data.
        new_pop_data : Optional[pd.DataFrame], default=None
            DataFrame with updated population data.
        new_job_data : Optional[pd.DataFrame], default=None
            DataFrame with updated job data.
        merge_translation_data : bool, default=True
            Whether to merge zone translation data.
        aggregate_by_zone : bool, default=True
            Whether to aggregate data by zones.
        compute_area : bool, default=True
            Whether to compute zonal area.
        calculate_density : bool, default=True
            Whether to calculate density for key columns.
        model_zone_data : bool, default=True
            Whether to merge and model zone data.

        Returns
        -------
        pd.DataFrame | Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]
            If `model_zone_data` is True, returns six DataFrames for household, population, and job data.
            Otherwise, returns a single processed DataFrame.
        """
        zone_id = self.zone_info["group_by_column"]
        group_by_columns = [zone_id] if dimension_columns is None else [zone_id] + dimension_columns
        zone_gdf_id_col = self.zone_info["zone_gdf_id_col"]
        translation_path = self.zone_info["translation_path"]
        prop_column = self.zone_info["prop_column"]
        # Merge data with translation if needed
        if merge_translation_data:
            by_data = self._merge_translation_data(
                by_data, translation_path, columns_to_process, prop_column, group_by_columns
            )

        # Perform aggregation by group
        if aggregate_by_zone:
            by_data = self._aggregate_by_zone(by_data, group_by_columns, columns_to_process)

        # Compute zonal area if enabled
        if compute_area:
            by_data = self._compute_zonal_area_dia(
                self.zone_gdf, by_data, zone_gdf_id_col, zone_id
            )

        # Calculate density if enabled
        if calculate_density:
            by_data = self._calculate_density(by_data, columns_to_process)
        
        # Merge zonal data if enabled
        if model_zone_data:
            zonal_household_growth, zonal_population_growth, zonal_job_growth= self._model_zone_data(
                by_data,
                new_hh_data,
                new_pop_data,
                new_job_data,
                zone_id,
            )
            zonal_household = self._cumulative_yearly_totals(
                zonal_household_growth,
                by_column,
                fy_columns,
            )
            zonal_population = self._cumulative_yearly_totals(
                zonal_population_growth,
                by_column,
                fy_columns,
            )
            zonal_job = self._cumulative_yearly_totals(
                zonal_job_growth,
                by_column,
                fy_columns,
            )
            return zonal_household_growth, zonal_population_growth, zonal_job_growth, zonal_household, zonal_population, zonal_job 

        return by_data
    
    def lad_summary(
        self,
        data: pd.DataFrame,
        base_year_column: str,
        future_year_columns: list,
    ) -> pd.DataFrame:
        """
        Aggregates zone-level data to LAD (Local Authority District) level.

        Parameters
        ----------
        data : pd.DataFrame
            Input data containing zone-level information.
        base_year_column : str
            Column representing the base year for calculations.
        future_year_columns : list[str]
            List of column names representing future years.

        Returns
        -------
        Tuple[pd.DataFrame, pd.DataFrame]
            - lad_data_annualgrowth: DataFrame with LAD-level annual growth data.
            - lad_data_annualtot: DataFrame with LAD-level cumulative totals.

        """
        # Retrieve required metadata from zone_info
        lookup_path = self.zone_info["zone_to_lad_path"]
        zone_id = self.zone_info["group_by_column"]
        lad_id = self.zone_info["lad_id_col"]
        zone_to_lad_prop_col = self.zone_info["zone_to_lad_prop"]
        # Aggregate zone-level data to LAD level
        lad_data_annualgrowth = self._zone_to_lad(
            data,
            lookup_path,
            zone_id,
            base_year_column,
            future_year_columns,
            lad_id,
            zone_to_lad_prop_col
        )
        # Compute cumulative yearly totals for LAD-level data
        lad_data_annualgrowth = lad_data_annualgrowth.set_index(lad_id)
        lad_data_annualtot = self._cumulative_yearly_totals(
            lad_data_annualgrowth,
            base_year_column,
            future_year_columns,
        )
        return lad_data_annualgrowth, lad_data_annualtot


    def _merge_translation_data(
        self,
        by_data: pd.DataFrame,
        translation_path: str,
        columns_to_process: list,
        prop_column: str,
        group_by_columns: list[str],
    ) -> pd.DataFrame:
        """Merge the zone translation data with the input dataframe."""
        if translation_path:
            # Load translation data
            zone_translation = pd.read_csv(translation_path)
            # Merge and process data
            by_data = by_data.merge(zone_translation, on="lsoa2021_id", how="left").set_index(
                ["lsoa2021_id"] + group_by_columns
            )
            # Apply proportional adjustment and reset index
            by_data = by_data.loc[:, columns_to_process].multiply(
                by_data[prop_column], axis=0
            )

        return by_data.reset_index()


    def _aggregate_by_zone(
        self, by_data: pd.DataFrame, group_by_columns: list[str], columns_to_process: list
    ) -> pd.DataFrame:
        """
        Group data by the zone and aggregate specified columns.

        Parameters
        ----------
        by_data : pd.DataFrame
            Dataframe containing zone-level information.
        group_by_column : str
            Column used for grouping.
        columns_to_process : List[str]
            List of columns to aggregate.

        Returns
        -------
        pd.DataFrame
            Aggregated DataFrame grouped by the specified column.
        """
        aggregated_df = by_data.groupby(group_by_columns, as_index=False)[
            columns_to_process
        ].sum()
        return aggregated_df

    def _compute_zonal_area_dia(
        self,
        zone_gdf: gpd.GeoDataFrame,
        zone_df: pd.DataFrame,
        zone_gdf_id_col: str,
        zone_df_id_col: str,
        crs_target=27700,
    ):
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
            raise ValueError(
                "Shapefile has no CRS defined. Please check the source data."
            )

        # Ensure it's projected in the correct CRS
        if not zone_gdf.crs.is_projected or zone_gdf.crs.to_epsg() != crs_target:
            zone_gdf = zone_gdf.to_crs(epsg=crs_target)

        # Compute area in square meters
        zone_gdf["area_sqm"] = zone_gdf.geometry.area
        zone_gdf["diameter"] = 2 * np.sqrt(zone_gdf["area_sqm"] / np.pi)

        # Merge area information into zone_data DataFrame
        zone_df = zone_df.merge(
            zone_gdf[[zone_gdf_id_col, "area_sqm", "diameter"]],
            left_on=zone_df_id_col,
            right_on=zone_gdf_id_col,
            how="left",
        )
        # zone_df.drop(columns=[zone_gdf_id_col], inplace=True)
        return zone_df

    def _calculate_density(
        self, by_data: pd.DataFrame, columns_to_process: list
    ) -> pd.DataFrame:
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
        # Loop through each column to calculate density and index

        for col in columns_to_process:
            density_col = f"{col[:2]}_den"
            # Check if 'area_sqm' > 0, otherwise set density to 0
            by_data[density_col] = np.where(
                by_data["area_sqm"] > 0, by_data[col] / by_data["area_sqm"] * 1000000, 0
            )
            # index_col = f"{density_col}_index"
            # by_data[index_col] = scaler.fit_transform(by_data[[density_col]])

        return by_data
    

    def _model_zone_data(
        self,
        by_data: pd.DataFrame,
        new_hh_data: pd.DataFrame, 
        new_pop_data: pd.DataFrame, 
        new_job_data: pd.DataFrame, 
        zone_col_in_by: str,
    ) -> pd.DataFrame:
        """
        Merge new dwelling, population, and job data into the existing zonal data.

        Parameters
        ----------
        new_hh_data : pd.DataFrame
            DataFrame containing new dwelling data with a zone column.
        new_pop_data : pd.DataFrame
            DataFrame containing new population data with a zone column.
        new_job_data : pd.DataFrame
            DataFrame containing new job data with a zone column.
        by_data : pd.DataFrame
            Existing zonal data to be updated.
        zone_col_in_new : str
            The column name representing zones in new dataframes.
        zone_col_in_by : str
            The column name representing zones in by_data.
        Returns
        -------
        pd.DataFrame
            Updated zonal data with merged dwelling, population, and job information.
        """
        # Merging by_data with new_dwel_data to create zonal_dwelling

        zonal_household = by_data.merge(new_hh_data,
                                    on=zone_col_in_by, 
                                    # left_on=zone_col_in_by, 
                                    # right_on=zone_col_in_new, 
                                    how="left")
        zonal_household = zonal_household[[zone_col_in_by, "household"] + new_hh_data.columns.to_list()]
        zonal_household = zonal_household.rename(columns={"household": "2023"})
        zonal_household = zonal_household.loc[:, ~zonal_household.columns.duplicated()]
        # Merging by_data with new_pop_data to create zonal_population
        zonal_population = by_data.merge(new_pop_data, 
                                    on=zone_col_in_by, 
                                    # left_on=zone_col_in_by, 
                                    # right_on=zone_col_in_new, 
                                        how="left")
        zonal_population = zonal_population[[zone_col_in_by, "population"] + new_pop_data.columns.to_list()]
        zonal_population = zonal_population.rename(columns={"population": "2023"})
        zonal_population = zonal_population.loc[:, ~zonal_population.columns.duplicated()]
        # Merging by_data with new_job_data to create zonal_jobs
        zonal_job = by_data.merge(new_job_data, 
                                on=zone_col_in_by, 
                                # left_on=zone_col_in_by, 
                                # right_on=zone_col_in_new,  
                                how="left")
        zonal_job = zonal_job[[zone_col_in_by, "jobs"] + new_job_data.columns.to_list()]
        zonal_job = zonal_job.rename(columns={"jobs": "2023"})
        zonal_job = zonal_job.loc[:, ~zonal_job.columns.duplicated()]
        return zonal_household.fillna(0), zonal_population.fillna(0), zonal_job.fillna(0)

    def _model_zone_data_with_dim(
        self,
        by_data: pd.DataFrame,
        new_data: pd.DataFrame,
        column_to_process: str,
        dimension_columns: list[str],
    ) -> pd.DataFrame:
        """
        Merge new dwelling, population, and job data into the existing zonal data.

        Parameters
        ----------
        by_data : pd.DataFrame
            Existing zonal data to be updated.
        new_ata : pd.DataFrame
            DataFrame containing new future year data with a zone column.
        column_to_process : str
            The column name to be processed, such as household, population, or jobs.
        dimension_columns : list[str]
            The list of columns containing dimensions.
        Returns
        -------
        pd.DataFrame
            Updated zonal data with merged dwelling, population, and job information.
        """
        # Merging by_data with new__data to create zonal_jobs segmented by sic_2d
        zone_id = self.zone_info["group_by_column"]
        zone_col_in_by = [zone_id] + dimension_columns
        zonal_data = by_data.merge(new_data,
                                    on=zone_col_in_by, 
                                    # left_on=zone_col_in_by, 
                                    # right_on=zone_col_in_new, 
                                    how="left")
        zonal_data = zonal_data[zone_col_in_by + [column_to_process] + new_data.columns.to_list()]
        zonal_data = zonal_data.rename(columns={column_to_process: "2023"})
        zonal_data = zonal_data.loc[:, ~zonal_data.columns.duplicated()]

        return zonal_data.fillna(0)

    def _zone_to_lad(
        self,
        data: pd.DataFrame,
        lookup_path: pathlib.Path,
        zone_id: str,
        base_year_column: str,
        future_year_columns: list,
        lad_id: str,
        zone_to_lad_prop_col: str,
    ) -> pd.DataFrame:
        """
        Aggregate zonal data to LAD level using a lookup file with proportional mapping.

        Parameters
        ----------
        data : pd.DataFrame
            DataFrame containing zone-level data.
        lookup_path : pathlib.Path
            Path to the lookup CSV file containing zone-to-LAD mapping and proportion columns.
        zone_id : str
            Column name in data representing the zone identifier.
        base_year_column : str
            Name of the base year column.
        future_year_columns : List[str]
            List of future year columns for aggregation.
        lad_id : str
            Column name in lookup representing the LAD identifier.
        zone_to_lad_prop_col : str
            Column in lookup representing the proportion of zone value allocated to each LAD.

        Returns
        -------
        pd.DataFrame
            Aggregated LAD-level DataFrame with the summed values.

        Raises
        ------
        ValueError
            If the total values before and after aggregation differ for any column.
        """
        # Load the lookup DataFrame
        lookup_df = pd.read_csv(lookup_path)

        # Check initial totals for all val_cols
        val_cols = [base_year_column] + future_year_columns
        totals_before = {col: data[col].sum() for col in val_cols}

        # Merge data with lookup to assign LADs and proportions
        merged_df = data.merge(
            lookup_df[[zone_id, lad_id, zone_to_lad_prop_col]],
            on=zone_id,
            how='left'
        )

        # Apply proportions to each value column
        for col in val_cols:
            merged_df[col] = merged_df[col] * merged_df[zone_to_lad_prop_col]

        # Group by LAD and sum the values
        lad_agg = merged_df.groupby(lad_id, as_index=False)[val_cols].sum()

        # Check totals after aggregation
        totals_after = {col: lad_agg[col].sum() for col in val_cols}

        for col in val_cols:
            if not np.isclose(totals_before[col], totals_after[col]):
                raise ValueError(
                    f"Total of '{col}' changed after aggregation: before={totals_before[col]}, after={totals_after[col]}"
                )

        return lad_agg
    

    def _cumulative_yearly_totals(
        self,
        data: pd.DataFrame,
        base_year_column : str = "2023",
        future_year_columns: list[str] = ["2024", "2025", "2026", "2027", "2028", "2029", "2030"],
    ) -> pd.DataFrame:
        """
        Calculate cumulative yearly totals from 2023 onwards, where each year's total
        is the sum of the previous year's total and the new values for that year.

        Parameters
        ----------
        data : pd.DataFrame
            DataFrame with a base year column and future year columns.
        base_year_column : str, optional
            The base year column from which cumulative sums start (default is "2023").
        future_year_columns : List[str]
            List of future year columns.

        Returns
        -------
        pd.DataFrame
            Updated DataFrame with cumulative totals for each year.
        """
        updated_data = data.copy()

        # Ensure all specified columns exist in the data
        missing_cols = [col for col in future_year_columns if col not in data.columns]
        if missing_cols:
            raise ValueError(f"Missing columns in data: {missing_cols}")

        # Compute cumulative totals year by year
        for idx, year in enumerate(future_year_columns):
            prev_year = base_year_column if idx == 0 else future_year_columns[idx - 1]
            updated_data[year] = updated_data[prev_year] + data[year]

        return updated_data

    def _cumulative_yearly_growth(
        self,
        data: pd.DataFrame,
        base_year_column : str = "2023",
        future_year_columns: list[str] = ["2024", "2025", "2026", "2027", "2028", "2029", "2030"],
    ) -> pd.DataFrame:
        """
        Calculate cumulative yearly totals from 2023 onwards, where each year's total
        is the sum of the previous year's total and the new values for that year.

        Parameters
        ----------
        data : pd.DataFrame
            DataFrame with a base year column and future year columns.
        base_year_column : str, optional
            The base year column from which cumulative sums start (default is "2023").
        future_year_columns : List[str]
            List of future year columns.

        Returns
        -------
        pd.DataFrame
            Updated DataFrame with cumulative totals for each year.
        """
        updated_data = data.copy()

        # Ensure all specified columns exist in the data
        missing_cols = [col for col in future_year_columns if col not in data.columns]
        if missing_cols:
            raise ValueError(f"Missing columns in data: {missing_cols}")

        # Compute cumulative totals year by year
        for _, year in enumerate(future_year_columns):
            prev_year = base_year_column 
            updated_data[year] = updated_data[year] - data[prev_year]

        return updated_data


class Ratios(BaseZoneHandler):
    """A class for calculating GB-wide distributions for households, travellers, and employment types."""
    def __init__(self, config):
        super().__init__(config)

    def gb_hh_type_ratios(self, data: pd.DataFrame, hh_type_columns: List[str]) -> pd.DataFrame:
        """
        Calculates household type distribution ratios and merges them back to the data.

        Parameters
        ----------
        data : pd.DataFrame
            Base year household data.
        hh_type_columns : List[str]
            List of columns containing household segmentations.

        Returns
        -------
        pd.DataFrame
            Dataframe indexed by zone and household type columns, containing household ratios.
        """

        ratios = (
            data.groupby(hh_type_columns)["household"]
            .sum()
            .pipe(lambda x: x / x.sum())
            .reset_index(name="ratios")
        )
        return ratios
    
    def zone_hh_type_ratios(
        self, 
        data: pd.DataFrame, 
        hh_type_columns: List[str], 
    ) -> pd.DataFrame:
        """
        Calculates the proportion of each household type per zone.

        Parameters
        ----------
        data : pd.DataFrame
            Base year household data. Must include a 'household' count column.
        hh_type_columns : List[str]
            Columns that define household segmentations (e.g. size, age, income).
        reset_index : bool, optional
            Whether to reset the index to return a flat DataFrame. Default is False.

        Returns
        -------
        pd.DataFrame
            DataFrame indexed by zone and household type columns with household counts and ratios.
        """
        zone_id = self.zone_info["group_by_column"]
        group_by = [zone_id] + hh_type_columns

        # Step 1: Group and sum households by zone and household types
        zonal_ratios = data.groupby(group_by)["household"].sum().to_frame()

        # Step 2: Create MultiIndex (already created by groupby)
        #         Compute total households per zone and the ratio per household type
        zonal_ratios["ratios"] = zonal_ratios["household"] / zonal_ratios.groupby(level=0)["household"].transform("sum")

        return zonal_ratios.reset_index()


    def gb_traveller_type_ratios(self, data: pd.DataFrame, pop_type_columns: List[str]) -> pd.DataFrame:
        """
        Calculates the traveller type distribution for each zone.

        Parameters
        ----------
        data : pd.DataFrame
            Base year population data.
        pop_type_columns : List[str]
            List of columns containing population segmentations.
        Returns
        -------
        pd.DataFrame
            DataFrame indexed by zone and traveller type, with population ratios.
        """

        ratios = (
            data.groupby(pop_type_columns)["population"]
            .sum()
            .pipe(lambda x: x / x.sum())
            .reset_index(name="ratios")
        )
        return ratios

    def zone_traveller_type_ratios(
        self, 
        data: pd.DataFrame, 
        pop_type_columns: List[str], 
    ) -> pd.DataFrame:
        """
        Calculates the proportion of each household type per zone.

        Parameters
        ----------
        data : pd.DataFrame
            Base year household data. Must include a 'household' count column.
        pop_type_columns : List[str]
            Columns that define population segmentations (e.g. size, age, income).
        reset_index : bool, optional
            Whether to reset the index to return a flat DataFrame. Default is False.

        Returns
        -------
        pd.DataFrame
            DataFrame indexed by zone and household type columns with household counts and ratios.
        """
        zone_id = self.zone_info["group_by_column"]
        group_by = [zone_id] + pop_type_columns

        # Step 1: Group and sum households by zone and household types
        zonal_ratios = data.groupby(group_by)["population"].sum().to_frame()

        # Step 2: Create MultiIndex (already created by groupby)
        #         Compute total households per zone and the ratio per household type
        zonal_ratios["ratios"] = zonal_ratios["population"] / zonal_ratios.groupby(level=0)["population"].transform("sum")

        return zonal_ratios.reset_index()

    def gb_job_type_ratios(self, data: pd.DataFrame, job_type_columns: List[str]) -> pd.DataFrame:
        """
        Calculates the traveller type distribution for each zone.

        Parameters
        ----------
        data : pd.DataFrame
            Base year population data.
        job_type_columns : List[str]
            List of columns containing job segmentations.
        Returns
        -------
        pd.DataFrame
            DataFrame indexed by zone and traveller type, with population ratios.
        """
        # Conditional filtering only if job_type_columns is exactly ['soc']
        if job_type_columns == ["soc"]:
            filtered_data = data[data["soc"].isin([1, 2, 3])]
        else:
            filtered_data = data
        ratios = (
            filtered_data.groupby(job_type_columns)["jobs"]
            .sum()
            .pipe(lambda x: x / x.sum())
            .reset_index(name="ratios")
        )
        return ratios

    def gb_soc_over_sic_ratios(self, data: pd.DataFrame, job_type_columns: List[str]) -> pd.DataFrame:
        """
        Calculates the ratio of SOC within each SIC across all zones.

        Parameters
        ----------
        data : pd.DataFrame
            Base year employment data.

        Returns
        -------
        pd.DataFrame
            DataFrame with zone, SIC, SOC and their associated ratio.
        """

        ratios = data.groupby(job_type_columns)["jobs"].sum()

        ratios["ratios"] = ratios["jobs"] / ratios.groupby(level=0).transform("sum")

        return ratios.reset_index()
    
    def zone_soc_over_sic_ratios(
            self, 
            df: pd.DataFrame,
            job_type_columns: List[str]
    ) -> pd.DataFrame:
        """
        Calculates the SOC ratio within each (zone_id, sic_2d) based on jobs, using MultiIndex.

        Parameters
        ----------
        df : pd.DataFrame
            Input DataFrame with columns: 'zone_id', 'sic_2d', 'soc', 'jobs'

        Returns
        -------
        pd.DataFrame
            DataFrame with MultiIndex (zone_id, sic_2d, soc) and 'jobs' and 'soc_ratio' columns.
        """
        zone_id = self.zone_info["group_by_column"]
        group_by = [zone_id] + job_type_columns
        # Set MultiIndex
        zone_ratios = df.groupby(group_by)["jobs"].sum().to_frame()
        # Calculate SOC ratios
        zone_ratios['ratios'] = zone_ratios['jobs'] / zone_ratios.groupby(level=[0, 1])['jobs'].transform('sum')


        return zone_ratios.reset_index()
    
    def zone_vals_by_type_fy(
        self, 
        data: pd.DataFrame, 
        dimension_columns: List[str], 
        build_out_columns: List[str],
    ) -> pd.DataFrame:
        """
        Calculates the proportion of each household type per zone and year, using MultiIndex.

        Parameters
        ----------
        data : pd.DataFrame
            Household data by zone and household type, with household counts in year columns.
        hh_type_columns : List[str]
            Columns that define household segmentations (e.g. size, age, income).
        build_out_columns : List[str]
            List of year-based columns (e.g., ['2024', '2025', ...]) with household counts.

        Returns
        -------
        pd.DataFrame
            MultiIndexed DataFrame with ratios for each [zone_id, *hh_type_columns], and
            one column per year showing the household type proportions.
        """
        zone_id = self.zone_info["group_by_column"]
        index_cols = [zone_id] + dimension_columns

        #Aggregate to get total household counts per group
        grouped = data.groupby(index_cols)[build_out_columns].sum()

        grouped = grouped.reset_index()


        return grouped

    def zone_ratios_by_type_fy(
        self, 
        data: pd.DataFrame, 
        dimension_columns: List[str], 
        build_out_columns: List[str],
        level: Union[int, List[int]] = 0
    ) -> pd.DataFrame:
        """
        Calculates the proportion of each household type per zone and year, using MultiIndex.

        Parameters
        ----------
        data : pd.DataFrame
            Household data by zone and household type, with household counts in year columns.
        Dimension_columns : List[str]
            Columns that define household segmentations (e.g. size, age, income).
        build_out_columns : List[str]
            List of year-based columns (e.g., ['2024', '2025', ...]) with household counts.
        level : int or List[int], default=0
            The index level(s) to group by when calculating zone-level totals for ratio computation.

        Returns
        -------
        pd.DataFrame
            MultiIndexed DataFrame with ratios for each [zone_id, *hh_type_columns], and
            one column per year showing the household type proportions.
        """
        zone_id = self.zone_info["group_by_column"]
        index_cols = [zone_id] + dimension_columns

        # Aggregate to get total household counts per group
        grouped = data.groupby(index_cols)[build_out_columns].sum()

        grouped = grouped.reset_index()

        # Re-apply MultiIndex for consistent groupby levels
        grouped = grouped.set_index(index_cols)

        # Compute ratios
        for year in build_out_columns:
            grouped[(year, 'ratio')] = grouped[year] / grouped.groupby(level=level)[year].transform("sum")

        # Drop raw count columns, keep only ratio columns
        ratio_cols = [(year, 'ratio') for year in build_out_columns]
        zonal_ratios = grouped[ratio_cols]

        # Rename columns to just year
        zonal_ratios.columns = [str(year) for year, _ in zonal_ratios.columns]
        # Fill NaN values with zero
        zonal_ratios = zonal_ratios.fillna(0)
        return zonal_ratios.reset_index()

    @staticmethod
    def expand_gb_ratios_by_zone(zone_list_dataframe: pd.DataFrame, gb_ratios: pd.DataFrame) -> pd.DataFrame:
        """
        Perform a cross join between zone_list_dataframe and gb_ratios,
        repeating all ratio combinations for each zone.

        Parameters:
        - zone_list_dataframe: pd.DataFrame with a 'normits_id' column.
        - gb_ratios: pd.DataFrame with household type categories and 'ratios' column.

        Returns:
        - pd.DataFrame with cross-joined data (each ratio row repeated for each zone).
        """
        # Step 1: Add dummy key column to both dataframes
        zone_list_copy = zone_list_dataframe.copy()
        gb_ratios_copy = gb_ratios.copy()
        zone_list_copy["key"] = 1
        gb_ratios_copy["key"] = 1

        # Step 2: Perform cross join
        by_zone_type_ratio = pd.merge(zone_list_copy, gb_ratios_copy, on="key").drop(columns="key")

        # Step 3: Reset index
        by_zone_type_ratio.reset_index(drop=True, inplace=True)

        return by_zone_type_ratio

    def apply_ratio(
        self,
        data: pd.DataFrame,
        unit_cols: list[str],
        dimension_cols: list[str],
        ratios: pd.DataFrame,
    ) -> pd.DataFrame:
        """applies TfN population land use factors to data

        Parameters
        ----------
        data : pd.DataFrame
            Data containing the original unit values to be adjusted.
        unit_cols : list[str]
            List of column names in `data` that contain the unit values to be updated.
        dimension_cols : list[str]
            List of column names used to define the ratio dimensions (e.g. demographic categories).
        ratios : pd.DataFrame
            DataFrame containing ratio values. It must include the same zone and dimension columns 
            used to join with `data`, plus a column named 'ratios'.

        Returns
        -------
        pd.DataFrame
            A DataFrame with the same index as the original `data` but with unit columns adjusted 
            by the ratio values.
        """
        zone_id = self.zone_info["group_by_column"]
        key_cols = [zone_id] + dimension_cols 
        data_ratios = (
            data.reset_index(drop=False)
            .merge(
                ratios.reset_index(drop=False),
                on=zone_id,
                how="left",
            )
        )
        data_ratios = data_ratios[key_cols + unit_cols + ["ratios"]].set_index(key_cols)
        data_ratios = data_ratios.loc[:, unit_cols].multiply(
            data_ratios["ratios"], axis=0
        )
        return data_ratios.reset_index()

    def apply_soc_over_sic_ratio(
        self,
        data: pd.DataFrame,
        unit_cols: list[str],
        dimension_cols: list[str],
        ratios: pd.DataFrame,
    ) -> pd.DataFrame:
        """applies TfN population land use factors to data

        Parameters
        ----------
        data : pd.DataFrame
            Data containing the original unit values to be adjusted.
        unit_cols : list[str]
            List of column names in `data` that contain the unit values to be updated.
        dimension_cols : list[str]
            List of column names used to define the ratio dimensions (e.g. demographic categories).
        ratios : pd.DataFrame
            DataFrame containing ratio values. It must include the same zone and dimension columns 
            used to join with `data`, plus a column named 'ratios'.

        Returns
        -------
        pd.DataFrame
            A DataFrame with the same index as the original `data` but with unit columns adjusted 
            by the ratio values.
        """
        zone_id = self.zone_info["group_by_column"]

        key_cols = [zone_id] + dimension_cols
        data_ratios = (
            data.reset_index(drop=False)
            .merge(
                ratios.reset_index(drop=False),
                on=[zone_id, "sic_2d"],
            )
        )
        data_ratios = data_ratios[key_cols + unit_cols + ["ratios"]].set_index(key_cols)
        data_ratios = data_ratios.loc[:, unit_cols].multiply(
            data_ratios["ratios"], axis=0
        )
        return data_ratios.reset_index()
    
    def ntem_pop_car_ratios(
        self,
        base_year_column: str,
        build_out_columns: List[str]
    ) -> pd.DataFrame:
        """
        Extrapolate missing future year values and calculate ratios
        relative to a specified base year.

        Parameters:
        -----------
        base_year : str
            The year used as the reference for ratio calculations (e.g., "2023").
        build_out_column : List[str]
            Full list of desired build-out years as strings (e.g., ["2024", ..., "2066"]).

        Returns:
        --------
        pd.DataFrame
            DataFrame with extrapolated values and ratio columns.
        """
        data = pd.read_csv(self.zone_info["ntem_pop_car_ratio"])
        # Extract available year columns in the DataFrame
        year_cols_in_data = [col for col in data.columns if col.isdigit()]
        ntem_future_year_list = [y for y in year_cols_in_data if int(y) >= int(base_year_column)]
        extrapolated_years = [y for y in build_out_columns if y not in ntem_future_year_list]

        extended_data = data.copy()
        x_known = np.array([int(y) for y in ntem_future_year_list])

        for row_idx, row in extended_data.iterrows():
            y_known = row[ntem_future_year_list].values.astype(float)

            # Fit linear trend
            coeffs = np.polyfit(x_known, y_known, deg=1)
            y_pred = np.polyval(coeffs, [int(y) for y in extrapolated_years])

            # Fill extrapolated values
            for i, year in enumerate(extrapolated_years):
                extended_data.at[row_idx, year] = y_pred[i]

        # Calculate ratios for all years in build_out_column vs base_year
        for year in build_out_columns:
            extended_data[year] = (
                extended_data[year].astype(float) / extended_data[base_year_column].astype(float)
            )

        return extended_data.drop(columns=[base_year_column])

    def ntem_hh_car_ratios(
        self,
        base_year_column: str,
        build_out_columns: List[str]
    ) -> pd.DataFrame:
        """
        Extrapolate missing future year values and calculate ratios
        relative to a specified base year.

        Parameters:
        -----------
        base_year : str
            The year used as the reference for ratio calculations (e.g., "2023").
        build_out_column : List[str]
            Full list of desired build-out years as strings (e.g., ["2024", ..., "2066"]).

        Returns:
        --------
        pd.DataFrame
            DataFrame with extrapolated values and ratio columns.
        """
        data = pd.read_csv(self.zone_info["ntem_hh_car_ratio"])
        # Extract available year columns in the DataFrame
        year_cols_in_data = [col for col in data.columns if col.isdigit()]
        ntem_future_year_list = [y for y in year_cols_in_data if int(y) >= int(base_year_column)]
        extrapolated_years = [y for y in build_out_columns if y not in ntem_future_year_list]

        extended_data = data.copy()
        x_known = np.array([int(y) for y in ntem_future_year_list])

        for row_idx, row in extended_data.iterrows():
            y_known = row[ntem_future_year_list].values.astype(float)

            # Fit linear trend
            coeffs = np.polyfit(x_known, y_known, deg=1)
            y_pred = np.polyval(coeffs, [int(y) for y in extrapolated_years])

            # Fill extrapolated values
            for i, year in enumerate(extrapolated_years):
                extended_data.at[row_idx, year] = y_pred[i]

        # Calculate ratios for all years in build_out_column vs base_year
        for year in build_out_columns:
            extended_data[year] = (
                extended_data[year].astype(float) / extended_data[base_year_column].astype(float)
            )

        return extended_data.drop(columns=[base_year_column])
    
    def apply_ntem_trend(
        self,
        data: pd.DataFrame,
        ntem_trends: pd.DataFrame,
        base_year_column: str,
        build_out_columns: list[str],
        dimension_cols: list[str],
    ) -> pd.DataFrame:
        """
        Applies NTEM land use trend ratios to base-year data and calculates normalized proportions
        across categories (e.g. car ownership) for each zone and future year.

        This method joins the input data with NTEM trend ratios, multiplies the base-year values
        by trend multipliers for each build-out year, and then normalizes the results within each zone
        so that the values across specified categories sum to 1 per zone per year.

        Parameters
        ----------
        data : pd.DataFrame
            Base-year data containing values to be scaled. Must include zone and dimension columns.
        
        ntem_trends : pd.DataFrame
            NTEM trend multipliers by zone and category for future years. Must include the same 
            zone and dimension columns as `data`, plus columns for each year in `build_out_columns`.

        base_year_column : str
            Name of the column in `data` that contains base-year values (e.g. "2023").
        
        build_out_columns : list[str]
            List of year columns representing future projection years (e.g. ["2024", ..., "2066"]).

        dimension_cols : list[str]
            Column names defining disaggregation categories (e.g. ["car_ownership"]).

        Returns
        -------
        pd.DataFrame
            A DataFrame with trend-adjusted and normalized values for each zone and category 
            across all build-out years.
        """
        zone_id = self.zone_info["group_by_column"]
        key_cols = [zone_id] + dimension_cols
        # Join original data with NTEM trend ratios
        zone_ratios = data.merge(
            ntem_trends,
            on=key_cols,
            how="left",
        )
        zone_ratios = zone_ratios[key_cols + [base_year_column] + build_out_columns]
        # Multiply base values by trend factors for future years
        for year in build_out_columns:
            zone_ratios[year] = (
                zone_ratios[year].astype(float) * zone_ratios[base_year_column].astype(float)
            )
        # Normalize each year's values within each zone so they sum to 1 across categories
        for year in build_out_columns:
            total_per_zone = zone_ratios.groupby(zone_id)[year].transform("sum")
            zone_ratios[year] = zone_ratios[year] / total_per_zone

        return zone_ratios
    
    def expand_by_dimension(
        self,
        zone_data: pd.DataFrame,
        zone_car_ratio: pd.DataFrame,
        build_out_columns: list[str],
        dimension_columns: list[str], 
    ) -> pd.DataFrame:
        """
        Calculate the number of households by given dimension categories per zone and year.

        Parameters
        ----------
        zone_data : pd.DataFrame
            DataFrame with total projected households per zone and year.
            Expected columns: [zone_id] + build_out_columns.
        
        zone_car_ratio : pd.DataFrame
            DataFrame with ratios per zone and dimension (e.g., car availability) for each year.
            Expected columns: [zone_id, *dimension_columns] + build_out_columns.

        build_out_columns : list[str]
            List of column names representing future years (e.g. ['2024', '2025', ..., '2066']).
        
        dimension_columns : Optional[list[str]], default=None
            List of columns representing the dimensions (e.g., car availability, income group).
            If None, defaults to using a single dimension column like 'car_availability'.

        Returns
        -------
        pd.DataFrame
            A wide-format DataFrame with [zone_id, dimension_categories] and one column per year,
            representing the number of households by each dimension category.
        """

        zone_id = self.zone_info["group_by_column"]
        # Check if zone_data contains 'sic_2d' (i.e., jobs data) and adjust id_vars
        if 'sic_2d' in zone_data.columns:
            id_list = [zone_id, 'sic_2d']
        else:
            id_list = [zone_id] # Add 'sic_2d' for jobs data

        # Melt both dataframes to long format
        data_long = zone_data.melt(id_vars=id_list, value_vars=build_out_columns, 
                                    var_name="year", value_name="vals")
        
        ratio_long = zone_car_ratio.melt(id_vars=id_list + dimension_columns, 
                                            value_vars=build_out_columns, 
                                            var_name="year", value_name="ratios")

        # Merge on zone_id and year
        merged = data_long.merge(
            ratio_long,
            on=id_list + ["year"],
            how="left"
        )

        # Calculate households by car availability
        merged["val_by_dimension"] = (
            merged["vals"].astype(float) * merged["ratios"].astype(float)
        )

        # Pivot to wide format (optional)
        wide = merged.pivot_table(
            index=id_list + dimension_columns,
            columns="year",
            values="val_by_dimension"
        ).reset_index()

        wide.columns.name = None  # Clean up column index name
        return wide

    def scaling_factors(
        self,
        target_df: pd.DataFrame,
        estimated_df: pd.DataFrame,
        build_out_columns: list[str],
        dimension_columns: list[str]
    ) -> pd.DataFrame:
        """
        Calculate scaling factors by dividing target values by estimated values 
        for each zone and dimension combination. Missing outcomes are filled with 1.

        Parameters
        ----------
        target_df : pd.DataFrame
            DataFrame containing the target values (e.g., from expand_by_dimension).
        estimated_df : pd.DataFrame
            DataFrame containing the estimated values (e.g., from zone_vals_by_type_fy).
        build_out_columns : list[str]
            List of column names representing years (e.g., ['2024', '2025', ..., '2066']).
        dimension_columns : list[str]
            List of dimension columns to include in the merge (e.g., ['car_availability']).

        Returns
        -------
        pd.DataFrame
            DataFrame with zone_id, dimension columns, and scaling factors per year.
        """
        zone_id = self.zone_info["group_by_column"]

        # Merge the two dataframes using outer join
        merged = target_df.merge(
            estimated_df,
            on=[zone_id] + dimension_columns,
            how="outer",
            suffixes=("_target", "_estimated")
        )

        # Compute ratio for each year and fill missing values in the final outcome
        for year in build_out_columns:
            target_col = f"{year}_target"
            estimated_col = f"{year}_estimated"

            # Compute the ratio (NaNs allowed for now)
            merged[year] = merged[target_col] / merged[estimated_col]

        # Fill final outcome NaNs with 1
        merged[build_out_columns] = merged[build_out_columns].fillna(1)

        # Return only the required columns
        return merged[[zone_id] + dimension_columns + build_out_columns] 

    def apply_scaling_factor(
        self,
        segmented_df: pd.DataFrame,
        scaler_df: pd.DataFrame,
        build_out_columns: list[str],
        dimension_columns: list[str]
    ) -> pd.DataFrame:
        """
        Apply scaling factors to a segmented DataFrame based on dimension columns (e.g., 'car_availability').

        Parameters
        ----------
        segmented_df : pd.DataFrame
            DataFrame with segmented values (e.g., from apply_ratio), containing zone_id,
            dimension columns (like 'car_availability'), and yearly values.
        scaler_df : pd.DataFrame
            DataFrame with corresponding scaling factors per zone and dimension, and per year.
        build_out_columns : list[str]
            List of yearly columns to scale (e.g., ['2024', '2025', ..., '2066']).
        dimension_columns : list[str]
            List of dimension columns to merge on (e.g., ['car_availability']).

        Returns
        -------
        pd.DataFrame
            A new DataFrame with scaled values for each zone, dimension, and year.
        """
        zone_id = self.zone_info["group_by_column"]
        
        # Merge the scaler values back into the segmented_df
        merged = segmented_df.merge(
            scaler_df,
            on=[zone_id] + dimension_columns,
            how="left",
            suffixes=("", "_scaler")
        )

        # Apply scaling for each year
        for year in build_out_columns:
            merged[year] = merged[year] * merged[f"{year}_scaler"]

        # Drop the scaler columns (optional)
        scaler_cols = [f"{year}_scaler" for year in build_out_columns]
        merged = merged.drop(columns=scaler_cols)

        return merged   

class SiteZoneProcessor(BaseZoneHandler):
    def __init__(self, config: inputs.DLitConfig):
        super().__init__(config)
    def zone_site_geospatial_lookup(
        self, 
        site_data: pd.DataFrame,
        site_geometry_col: str
        ) -> gpd.GeoDataFrame:
        """
        Perform a spatial join between site locations and predefined zones.
        
        Parameters:
        site_data (pd.DataFrame): DataFrame containing site information.
        site_id_col (str): Column name representing unique site identifiers.
        site_geometry_col (str): Column name containing site geometries.
        
        Returns:
        gpd.GeoDataFrame: Site data with zone assignments.
        """
        # Convert the 'data' DataFrame to a GeoDataFrame with geometry based on coordinates
        site_data = site_data.copy()
        site_data[site_geometry_col] = gpd.points_from_xy(site_data["easting"], site_data["northing"])
        site_gdf = gpd.GeoDataFrame(site_data, geometry=site_geometry_col, crs=self.zone_gdf.crs)

        # Perform spatial join with the zone geometries
        joined_data = gpd.sjoin(site_gdf, self.zone_gdf, how="left")

        # Retain original site columns plus zone ID
        return joined_data[site_data.columns.tolist() + [self.zone_info["zone_gdf_id_col"]]]

    def agg_zonal_data(
        self, 
        site_data: pd.DataFrame, 
        build_out_columns: list, 
        site_size_column: str,
        dimension_columns: Optional[list] = None
    ) -> pd.DataFrame:
        """
        Aggregate zonal data by summing build-out columns after dropping site-related columns.
        Additionally, compute separate zonal values for large and small sites.

        Parameters
        ----------
        site_data : pd.DataFrame
            DataFrame containing site-level data, including a 'site_size' column.
        build_out_columns : list
            List of columns to be summed during aggregation.
        site_related_columns : list
            List of columns to be dropped from site_data before aggregation.
        dimension_columns : list, optional
            Additional columns to group by, by default ["tt"].

        Returns
        -------
        pd.DataFrame
            Aggregated DataFrame grouped by zone ID and dimension columns with summed values
            for all sites, large sites, and small sites.
        """

        # Define grouping keys
        zone_id_col = self.zone_info["zone_gdf_id_col"]
        rename_zone_id_col = self.zone_info["group_by_column"]

        if dimension_columns is None:
            dimension_columns = []  # If None, set to an empty list
        groupby_columns = [zone_id_col] + dimension_columns


        # Aggregate for all sites
        agg_all = site_data.groupby(groupby_columns, as_index=False)[build_out_columns].sum()

        # Aggregate for large sites
        agg_large = (
            site_data[site_data[site_size_column] == "large"]
            .groupby(groupby_columns, as_index=False)[build_out_columns]
            .sum()
        )
        # Add suffix only to the build_out_columns
        agg_large = agg_large.rename(columns={col: f"{col}_large" for col in build_out_columns})
        # Aggregate for small sites
        agg_small = (
            site_data[site_data[site_size_column] == "small"]
            .groupby(groupby_columns, as_index=False)[build_out_columns]
            .sum()
        )
        # Add suffix only to the build_out_columns
        agg_small = agg_small.rename(columns={col: f"{col}_small" for col in build_out_columns})
        # Merge all results
        agg_data = agg_all.merge(agg_large, on=groupby_columns, how="left").merge(agg_small, on=groupby_columns, how="left")
        # Rename zone_id_col to rename_zone_id_col
        agg_data = agg_data.rename(columns={zone_id_col: rename_zone_id_col})
        # Infill NaNs with 0
        agg_data = agg_data.fillna(0)

        return agg_data
    
    @staticmethod
    def accumulated_growth(
            site_data: pd.DataFrame,
            build_out_columns: list,
            column_prefixes: list
        )-> pd.DataFrame:
        """
        Computes accumulated sum for each group of columns in column_prefixes.
        
        Parameters:
            df (pd.DataFrame): The input DataFrame.
            build_out_columns: Future year columns.
            column_prefixes (list): List of column name patterns (e.g., "", "_large", "_small").
        
        Returns:
            pd.DataFrame: DataFrame with accumulated values.
        """
        df_accumulated = site_data.copy()

        # Loop through each prefix to compute cumulative sum for the columns with the given prefix
        for prefix in column_prefixes:
            # Create column names based on year and prefix
            cols = [f"{year}{prefix}" for year in build_out_columns]
            
            # Apply cumulative sum across the rows (axis=1)
            df_accumulated[cols] = site_data[cols].cumsum(axis=1)
        
        return df_accumulated
        

    def merge_zonal_attributes(
        self, site_data: pd.DataFrame, by_data: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Merges zonal attributes with site data.

        Parameters
        ----------
        site_data : pd.DataFrame
            Data containing site and assigned zone information.
        by_data : pd.DataFrame
            Zonal data with attributes to be merged.

        Returns
        -------
        pd.DataFrame
            Merged DataFrame with additional zonal attributes.
        """
        return site_data.merge(by_data, on=self.zone_info["zone_gdf_id_col"], how="left")

    def calculate_distance_to_zone_centroids(
        self, site_data: pd.DataFrame, centroid_type: str
    ) -> pd.DataFrame:
        """
        Compute the distance of each site to its corresponding zone centroid.
        
        Parameters:
        site_data (pd.DataFrame): DataFrame with site details.
        centroid_type (str): Type of centroid (hh, emp or pop).
        
        Returns:
        pd.Series: Distance values from sites to zone centroids.
        """
        if self.geo_boundary not in self.zone_info_map:
            raise ValueError(f"Unsupported geo_boundary: {self.geo_boundary}")

        centroid_file = self.zone_info_map[self.geo_boundary]["centroid_files"].get(
            centroid_type
        )
        if not centroid_file:
            raise ValueError(
                "Invalid centroid type. Choose from 'hh', 'emp', or 'pop'."
            )

        centroids = pd.read_csv(centroid_file)
        site_coords = np.vstack((site_data["easting"], site_data["northing"])).T
        centroid_coords = np.vstack((centroids["x"], centroids["y"])).T

        tree = cKDTree(centroid_coords)
        distances, _ = tree.query(site_coords)
        site_data[f"dist_to_{centroid_type}_c"] = distances

        return site_data

    def compute_centroid_shift(
        self, site_data: pd.DataFrame, centroid_type: str
    ) -> pd.DataFrame:
        """
        Compute the shift in household centroid due to new site developments.

        Parameters:
        site_data (pd.DataFrame): DataFrame with site details.
        centroid_type (str): Type of centroid (hh, emp or pop).
        Return: 
        DataFrame with updated centroid coordinates and shift distances.
        """
        if self.geo_boundary not in self.zone_info_map:
            raise ValueError(f"Unsupported geo_boundary: {self.geo_boundary}")

        centroid_file = self.zone_info_map[self.geo_boundary]["centroid_files"].get(
            centroid_type
        )
        if not centroid_file:
            raise ValueError(
                "Invalid centroid type. Choose from 'hh', 'emp', or 'pop'."
            )

        centroids = pd.read_csv(centroid_file)
        site_data = site_data.copy()
        zone_id = self.zone_info["group_by_column"]
        # Merge with existing centroid data
        site_data = site_data.merge(centroids, on=zone_id, how="left")
        site_data.rename(
            columns={"x": f"{centroid_type}_x", "y": f"{centroid_type}_y"}, inplace=True
        )
        if centroid_type == "hh":

            site_data[f"n_{centroid_type}_x"] = (
                site_data[f"{centroid_type}_x"] * site_data["household"]
                + site_data["easting"] * site_data["sum_proposed"]
            ) / (site_data["household"] + site_data["sum_proposed"])

            site_data[f"n_{centroid_type}_y"] = (
                site_data[f"{centroid_type}_y"] * site_data["household"]
                + site_data["northing"] * site_data["sum_proposed"]
            ) / (site_data["household"] + site_data["sum_proposed"])

            # Compute centroid shift distance
            site_data["centroid_shift"] = np.sqrt(
                (site_data[f"n_{centroid_type}_x"] - site_data[f"{centroid_type}_x"])
                ** 2
                + (site_data[f"n_{centroid_type}_y"] - site_data[f"{centroid_type}_y"])
                ** 2
            )
        elif centroid_type == "emp":
            site_data[f"n_{centroid_type}_x"] = (
                site_data[f"{centroid_type}_x"] * site_data["jobs"]
                + site_data["easting"] * site_data["sum_proposed"]
            ) / (site_data["jobs"] + site_data["sum_proposed"])

            site_data[f"n_{centroid_type}_y"] = (
                site_data[f"{centroid_type}_y"] * site_data["jobs"]
                + site_data["northing"] * site_data["sum_proposed"]
            ) / (site_data["jobs"] + site_data["sum_proposed"])

            # Compute centroid shift distance
            site_data["centroid_shift"] = np.sqrt(
                (site_data[f"n_{centroid_type}_x"] - site_data[f"{centroid_type}_x"])
                ** 2
                + (site_data[f"n_{centroid_type}_y"] - site_data[f"{centroid_type}_y"])
                ** 2
            )
        else:
            raise ValueError("Invalid centroid type. Choose from 'hh' and 'emp'.")

        return site_data

    def streamline_dataset(
        self,
        site_data: pd.DataFrame,
        key_columns: list[str]
    ) -> pd.DataFrame:
        """
        Streamline the site data with necessary columns and renaming.

        :param site_data: DataFrame containing site details, centroids, and calculated attributes.
        :return: Processed DataFrame with streamlined columns.
        """
        zone_column = self.zone_info["group_by_column"]

        # Define required columns and rename mapping
        rename_mapping = {
            "household": "exsiting_Household",
            "population": "existing_Population",
            "jobs": "existing_Jobs",
        }

        # Select necessary columns
        streamlined_data = site_data[
            key_columns + [
                zone_column,
                "prob_val",
                "value_estimated",
                "sum_proposed",
                "household",
                "population",
                "jobs",
                "ho_den",
                "po_den",
                "jo_den",
                "ctrd_shift_ratio",
                "n_e_ratio",
            ]
        ].rename(columns=rename_mapping)

        return streamlined_data

class LargeSites:
    """Class to handle site data processing, including calculating statistics, z-scores,
    and filtering large sites based on index thresholds.
    """

    def __init__(self, site_data: pd.DataFrame, columns_to_explore: list):
        """
        Initializes the LargeSites class with site data and configuration.

        Parameters
        ----------
        site_data : pd.DataFrame
            The dataframe of processed site data (either residential or employment).
        columns_to_explore : list
            List of column names for which statistics and z-scores will be calculated.
        """
        self.site_data = site_data
        self.columns_to_explore = columns_to_explore
    
    def large_sites_selection(
        self,
        category: str = "residential",
        zscore_suffix: str = "zscore",
        weight_dict: dict = None,
        index_col: str = "weighted_index",
        index_threshold: float = "0.5",
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, list]:
        """
        Select large sites based on the weighted index and threshold values.

        Parameters
        ----------
        category : str, optional
            Category name, defaults to 'residential'.
        zscore_suffix : str, optional
            The suffix for z-score columns, options include 'zscore', 'robust_zscore', 'modified_zscore'.
        weight_dict : dict, optional
            Custom weights for the columns, defaults to predefined weight_dict if None.
        index_col : str, optional
            The column name for the weighted index, defaults to 'weighted_index'.
        index_threshold : float, optional
            The threshold value for selecting large sites, defaults to 0.5.
        Returns
        -------
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, list]
            The site counts, merged sites, large sites, and list of large sites.
        """
        # Calculate z-scores and weighted index
        data_z_scores = self._calculate_z_scores()
        data_weightindex = self._cal_site_weight(
            data_z_scores,
            self.columns_to_explore,  # Use instance attribute
            category,
            zscore_suffix,
            weight_dict,
            index_col,
        )
        site_counts = self._count_sites(data_weightindex, index_col)
        # Filter large sites based on the threshold
        merged_sites, large_sites, large_sites_list = self._large_sites(
            data_weightindex, index_col, index_threshold,
        )
        return site_counts, merged_sites, large_sites, large_sites_list
   
        
    def _calculate_statistics(self):
        """Calculates basic statistics for the given columns.

        Computes mean, median, standard deviation, etc., for the provided columns.

        Returns
        -------
        site_stats : pd.DataFrame
            DataFrame containing the calculated basic statistics.
        """
        site_stats = stats.basic_statistics(self.site_data, self.columns_to_explore)
        return site_stats
    
    def _calculate_z_scores(self):
        """Computes z-scores for the given columns.

        Calculates z-scores to assess the deviations from the mean for the specified columns.

        Returns
        -------
        site_z_scores : pd.DataFrame
            DataFrame containing the calculated z-scores.
        """
        site_z_scores = stats.compute_z_scores(self.site_data, self.columns_to_explore)
        return site_z_scores
    

    def _cal_site_weight(
        self,
        data: pd.DataFrame,
        columns_to_explore: list[str],
        category: str = "residential",
        zscore_suffix: str = "zscore",
        weight_dict: dict = None,
        index_col: str = "weighted_index",
    ):
        """runs process for calculating the weighted index based on site data

        Transforms site data by applying z-scores, creating new columns for transformed values.
        Calculates a weighted index based on the provided weights for different columns.
        Adjusts the index based on site real values and probability values.

        Parameters
        ----------
        data: pd.DataFrame
            The site data containing columns with zscore.
        columns_to_explore : list[str]
            List of columns to be further converted to standardized values.
        category : str, optional
            Category name, defaults to 'residential'. Determines how transformations are applied.
        zscore_suffix : str, optional
            The suffix for z-score columns, options include 'zscore', 'robust_zscore', 'modified_zscore'.
        weight_dict : dict, optional
            Custom weights for the columns, defaults to predefined weight_dict if None.
        index_col : str, optional
            The column name for the weighted index, defaults to 'weighted_index'.

        Returns
        -------
        pd.DataFrame
            The original site_data dataframe with additional columns and the calculated weighted index.
        """
        # Create the 'site_with_realval' column
        data["site_with_realval"] = data["value_estimated"].apply(
            lambda x: 1 if x == "real" else 0
        )
        # Define the columns with the chosen suffix (e.g., '_zscore', '_robust_zscore', '_modified_zscore')
        columns_z_score = [f"{col}_{zscore_suffix}" for col in columns_to_explore]
        
        # Apply transformations based on category
        if category.lower() == "residential":
            for col in columns_z_score:
                new_col_name = col.replace(f"_{zscore_suffix}", "_index")
                if col in [
                    f"ho_den_{zscore_suffix}",
                    f"po_den_{zscore_suffix}",
                ]:
                    data[new_col_name] = data[col].apply(lambda x: -x)
                else:
                    data[new_col_name] = data[col]
        elif category.lower() == "employment":
            for col in columns_z_score:
                new_col_name = col.replace(f"_{zscore_suffix}", "_index")
                if col in [f"jo_den_{zscore_suffix}"]:
                    data[new_col_name] = data[col].apply(lambda x: -x)
                else:
                    data[new_col_name] = data[col]
        else:
            raise ValueError(f"Unknown category: {category}. Please specify 'residential' or 'employment'.")

        # Define weight dictionary for new columns if not provided
        if weight_dict is None:
            weight_dict = {
                "sum_proposed_index": 0.3,
                "ho_den_index": 0.15,
                "po_den_index": 0,
                "jo_den_index": 0.05,
                "n_e_ratio_index": 0.1,
                "ctrd_shift_ratio_index": 0.4,
            }

        # Create 'weighted_index' column by calculating the weighted average
        weighted_sum = 0
        total_weight = 0
        for col, weight in weight_dict.items():
            if col in data.columns:
                weighted_sum += data[col] * weight
                total_weight += weight

        # Final weighted index, ensuring the total weight sums to 1
        data[index_col] = weighted_sum / total_weight
        data[index_col] = data[index_col] * data["site_with_realval"] * data["prob_val"]
        columns_to_explore_index = [col + "_index" for col in columns_to_explore]
        columns = ["site_reference_id"] + [index_col] + columns_to_explore_index

        return data[columns]

    @staticmethod
    def _count_sites(
        df: pd.DataFrame,
        df_name: str = "DataFrame",
        col: str = "weighted_index",
        var_name: str = "zscore",
    ) -> pd.DataFrame:
        """runs process for generating a summary table of site counts based on thresholds

        Counts total records and those with values greater than specific thresholds for the given column.
        Generates a summary of the distribution of records across defined threshold values.

        Parameters
        ----------
        df : pd.DataFrame
            The dataframe to be analyzed.
        df_name : str, optional
            The name of the dataframe, defaults to 'DataFrame'.
        col : str, optional
            The column name on which thresholds are applied, defaults to 'weighted_index'.
        var_name : str, optional
            The variable name for the column in the summary table, defaults to 'zscore'.

        Returns
        -------
        pd.DataFrame
            A summary DataFrame showing the total count and counts for each threshold.
        """
        thresholds = [
            -5, -4, -3, -2, -1, 0, 0.2, 0.4, 0.6, 0.8, 1, 1.1, 1.2, 1.3, 1.4, 1.5,
            1.6, 1.7, 1.8, 1.9, 2, 3, 4, 5, 10
        ]
        summary = {"Total Records": len(df)}

        for t in thresholds:
            summary[f"> {t}"] = (df[col] > t).sum()

        summary_df = pd.DataFrame(summary.items(), columns=[df_name, var_name])

        return summary_df
    

    def _large_sites(
        self,
        z_scores_withindex: pd.DataFrame,
        index_col: str,
        index_threshold: int,
    ):
        """runs process for identifying and filtering large sites based on index threshold

        Merges site data with z-scores, filters the sites based on a defined threshold for the index column,
        and returns the filtered large sites as well as the full merged dataset.

        Parameters
        ----------
        self.site_data : pd.DataFrame
            The zone site data.
        z_scores_withindex : pd.DataFrame
            The z-score data with site_reference_id.
        index_col : str
            The column containing the index values.
        index_threshold : int
            The threshold for selecting large sites based on the index column.

        Returns
        -------
        pd.DataFrame
            The merged dataframe containing both zone sites and z-scores.
        pd.DataFrame
            The filtered dataframe containing large sites based on the threshold.
        list
            A list of site_reference_ids of the large sites.
        """
        merged_sites = self.site_data.merge(z_scores_withindex, on="site_reference_id")
        large_sites = merged_sites[merged_sites[index_col] >= index_threshold]
        large_site_ids = large_sites["site_reference_id"].tolist()

        return merged_sites, large_sites, large_site_ids

class PrepTripends:
    
    CATEGORIES = ['', '_large']

    def __init__(self, config: inputs.DLitConfig) -> None:
        """Initialize the class with dynamic YEARS based on the base year."""
        self.base_year_int = int(config.dev_pattern.base_year) 
        self.end_year_int = int(config.dev_pattern.end_year) # Ensure config has base_year_int
        self.years = range(self.base_year_int + 1, self.end_year_int + 1)  

        self.data_config = {
            "soc_sic_emp": {
                "index_cols": ['sic_2_digit', 'soc'],
                "rename_cols": {'sic_2d': 'sic_2_digit'},
                "segments": ['sic_2_digit', 'soc']
            },
            "tt_pop": {
                "index_cols": ['tt'],
                "segments": ['gender_3', 'aws', 'soc', 'ns_sec', 'hh_type'],
            },
            "hh": {
                "index_cols": ['accom_h', 'ns_sec', 'adults', 'car_availability', 'children'],
                "segments": ['accom_h', 'ns_sec', 'adults', 'car_availability', 'children'],
            }
        }
        self.cols_to_merge = ['gender_3', 'aws', 'ns_sec', 'soc', 'hh_type']
        self.rename_mapping = {
            'gender': 'gender_3',
            'aws': 'aws',
            'ns': 'ns_sec',
            'soc': 'soc',
            'hh_type': 'hh_type'
        }
        # Initialize the BaseZoneHandler for geo_boundary and other zone info
        self.base_zone_handler = BaseZoneHandler(config)
        self.zone_info = self.base_zone_handler.zone_info
        self.zone_id = self.zone_info["group_by_column"]

    def merge_and_rename(self, 
                         data: pd.DataFrame, 
                         lookup: pd.DataFrame) -> pd.DataFrame:
        
        """Merge and rename columns for tt_pop data."""

        return data.merge(lookup, on='tt', how='left').rename(columns=self.rename_mapping)

    def process_data(
            self, 
            data: pd.DataFrame, 
            data_type: str, 
            lookup: pd.DataFrame = None
    ) -> Dict[str, pd.DataFrame]:
        
        """Process data for a given type and pivot it per year and category."""

        if data_type not in self.data_config:
            raise ValueError(f"Unknown data type: {data_type}")

        config = self.data_config[data_type]
        if "rename_cols" in config:
            data.rename(columns=config["rename_cols"], inplace=True)

        index_cols = config["index_cols"]

        if data_type == "tt_pop" and lookup is not None:
            data = self.merge_and_rename(data, lookup)
            index_cols = self.cols_to_merge  

        if self.zone_id in data.columns:
            data[self.zone_id] = data[self.zone_id].astype('int64')

        processed_data = {
            f"{year}{category}": data.pivot_table(index=index_cols, columns=self.zone_id, values=f"{year}{category}", fill_value=0)
            for year in self.years for category in self.CATEGORIES if f"{year}{category}" in data.columns
        }

        return processed_data

    def prepare_segmentation_input(self, 
                                   data_type: str) -> cb.SegmentationInput:
        
        """Prepare segmentation input based on data type."""

        if data_type not in self.data_config:

            raise ValueError(f"Unknown data type for segmentation: {data_type}")
        
        return cb.SegmentationInput(enum_segments=self.data_config[data_type]["segments"], naming_order=self.data_config[data_type]["segments"])

    def perform_segmentation_and_zoning(
            self, 
            data: pd.DataFrame, 
            data_type: str,
            output_path: Path
    ):
        
        """Perform segmentation and zoning, then save the result."""

        seg = cb.Segmentation(self.prepare_segmentation_input(data_type))
        zoning = cb.ZoningSystem.get_zoning('normits')

        dvec = DVector(import_data=data, zoning_system=zoning, segmentation=seg)
        if data_type == "tt_pop" :
            dvec = dvec.add_segments(['adult_nssec'])

        if data_type == "hh" :
            dvec = dvec.add_segments(['adult_nssec', 'total'])

        dvec.save(output_path)



def get_site_reference_ids(
    site_df: pd.DataFrame, missing_area_col: str, missing_gfa_col: str
) -> list:
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
    site_df["value_estimated"] = site_df.apply(
        lambda row: (
            "estimated" if row[missing_area_col] and row[missing_gfa_col] else "real"
        ),
        axis=1,
    )

    # Filter the dataframe for rows where 'value_estimated' is 'estimated'
    estimated_sites = site_df[site_df["value_estimated"] == "estimated"]

    # Return the list of site_reference_ids
    return estimated_sites["site_reference_id"].tolist()




def process_site_data(
    site_data: pd.DataFrame,
    by_data: pd.DataFrame,
    site_type: str,
    site_reference_ids: list,
    key_columns: list[str],
    build_out_columns: list[str],
    probability_dict: dict,
    sitezone_processor: SiteZoneProcessor,
    use_prob_for_size: bool,
):
    """
    Processes site data for a given site type (Residential or Employment), calculates various metrics,
    and merges zonal attributes. This function handles site data by creating new columns, mapping probability values,
    calculating ratios, and computing centroid shifts for the development sites.

    Parameters:
    -----------
    site_data : pd.DataFrame
        A DataFrame containing site data, including columns for proposed development and other site-related attributes.
    
    by_data : pd.DataFrame
        A DataFrame containing base year zonal attributes to be merged with the site data.

    site_type : str
        The type of site (e.g., "Residential" or "Employment") to process.

    site_reference_ids : list
        A list of site reference IDs to identify whether a site is real or estimated.

    key_columns : list[str]
        A list of key columns to retain in the final processed DataFrame.

    build_out_columns : list[str]
        A list of columns representing the proposed development for each year.

    probability_dict : dict
        A dictionary mapping web tag certainty values to probability values, used to adjust the site size.

    sitezone_processor : SiteZoneProcessor
        An instance of the SiteZoneProcessor class, used to perform geospatial lookups, merge zonal attributes,
        and compute centroid shifts.

    use_prob_for_size : bool
        A boolean flag indicating whether to adjust the site size using probability values.

    Returns:
    --------
    pd.DataFrame
        A DataFrame containing the processed site data with calculated ratios, centroid shifts, and merged zonal attributes.
        The DataFrame includes the following columns:
        - 'sum_proposed' : Sum of the proposed development for the site across the specified columns.
        - 'prob_val' : The probability value mapped from the web tag certainty.
        - 'value_estimated' : Indicates whether the site is estimated or real.
        - 'ho_den' : household density.
        - 'po_den' :  populationd density.
        - 'jo_den' : job density.
        - 'n_e_ratio' : The ratio of new development to existing development (based on household or job data).
        - 'centroid_shift' : The shift in centroid location caused by the development.
        - 'ctrd_shift_ratio' : The ratio of centroid shift to site diameter.

    Raises:
    -------
    ValueError
        If an unknown site type is provided, the function defaults to using jobs for ratio calculations.
    """
    LOG.info(f"Processing {site_type} site data")

    # Create a new column 'sum_proposed' which is the sum of the columns from 2024 to the last year
    site_data["sum_proposed"] = site_data.loc[:, build_out_columns].sum(
        axis=1
    )
    columns_to_keep = key_columns + [
        "sum_proposed",
    ]
    site_data = site_data[columns_to_keep]

    site_data["prob_val"] = site_data["web_tag_certainty"].map(probability_dict)
    if use_prob_for_size:
        site_data["sum_proposed"] = site_data["prob_val"] * site_data["sum_proposed"]
    # Map development sites to pre-defined zone
    site_zone_sites = sitezone_processor.zone_site_geospatial_lookup(site_data, site_geometry_col="geometry")

    LOG.info(f"Getting attributes associated with {site_type} development sites")
    # Add the 'value_estimated' column for zone sites
    site_zone_sites["value_estimated"] = site_zone_sites["site_reference_id"].apply(
        lambda x: "estimated" if x in site_reference_ids else "real"
    )

    # Merge sites with associated zonal attributes
    site_zone_sites = sitezone_processor.merge_zonal_attributes(
        site_zone_sites, by_data
    )

    # Calculate ratio of new development to existing development
    ratio_column = "n_e_ratio"
    if site_type == "Residential":
        # Calculate the ratio for Residential sites using household data
        site_zone_sites[ratio_column] = (
            site_zone_sites["sum_proposed"] / site_zone_sites["household"]
        )
    elif site_type == "Employment":
        # Calculate the ratio for Employment sites using jobs data
        site_zone_sites[ratio_column] = (
            site_zone_sites["sum_proposed"] / site_zone_sites["jobs"]
        )
    else:
        # For any other site type, default ratio calculation (can be customized as needed)
        LOG.warning(
            f"Unknown site type: {site_type}. Defaulting to a ratio using jobs."
        )
        site_zone_sites[ratio_column] = (
            site_zone_sites["sum_proposed"] / site_zone_sites["jobs"]
        )
    # Replace inf, -inf, and NaN with zero in n_e_ratio
    site_zone_sites[ratio_column] = site_zone_sites[ratio_column].replace([np.inf, -np.inf], 0).fillna(0)


    LOG.info(f"Calculating centroid shift caused by {site_type} sites")
    centroid_types = ["hh", "emp"]

    for centroid_type in centroid_types:
        site_zone_sites = sitezone_processor.compute_centroid_shift(
            site_zone_sites, centroid_type
        )

    ctrd_shift_ratio_col = "ctrd_shift_ratio"
    site_zone_sites[ctrd_shift_ratio_col] = site_zone_sites["centroid_shift"]/site_zone_sites["diameter"]

    site_zone_sites = sitezone_processor.streamline_dataset(site_zone_sites, key_columns)


    return site_zone_sites

class TotalsComparison:
    def __init__(self, build_out_columns: List[str]):
        """
        Initialize with year-based columns.

        Parameters
        ----------
        build_out_columns : List[str]
            Year columns (e.g., ['2024', '2025', '2026', ...]).
        """
        self.build_out_columns = build_out_columns
        self.results = []

    def compare_pair(
        self,
        label_prefix: str,
        df_predisagg: pd.DataFrame,
        df_postdisagg: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Compare total values across years for a pair of datasets.

        Parameters
        ----------
        label_prefix : str
            Label prefix for the comparison group (e.g. 'Household').
        df_predisagg : pd.DataFrame
            Pre-disaggregated dataframe.
        df_postdisagg : pd.DataFrame
            Post-disaggregated dataframe.

        Returns
        -------
        pd.DataFrame
            DataFrame with total values for each year.
        """
        df = pd.DataFrame({
            f"{label_prefix}_Zonal": df_predisagg[self.build_out_columns].sum(),
            f"{label_prefix}_Segmented": df_postdisagg[self.build_out_columns].sum()
        })

        df[f"{label_prefix}_AbsDiff"] = df[f"{label_prefix}_Segmented"] - df[f"{label_prefix}_Zonal"]
        df[f"{label_prefix}_PctDiff"] = (
            100 * df[f"{label_prefix}_AbsDiff"] / df[f"{label_prefix}_Zonal"]
        ).replace([float("inf"), -float("inf")], pd.NA)

        return df

    def add_comparison(
        self,
        label_prefix: str,
        df_predisagg: pd.DataFrame,
        df_postdisagg: pd.DataFrame,
    ):
        """
        Run a comparison and store the result internally.

        Parameters
        ----------
        label_prefix : str
            Label for the comparison.
        df_predisagg : pd.DataFrame
            Pre-disaggregated data.
        df_postdisagg : pd.DataFrame
            Post-disaggregated data.
        """
        result = self.compare_pair(label_prefix, df_predisagg, df_postdisagg)
        self.results.append(result)

    def get_summary(self) -> pd.DataFrame:
        """
        Combine all stored comparisons into a single DataFrame.

        Returns
        -------
        pd.DataFrame
            Summary DataFrame across all added comparisons.
        """
        summary = pd.concat(self.results, axis=1)
        summary.loc["Total"] = summary.sum(numeric_only=True)
        return summary



def run(input_data: global_classes.AssessData, config: inputs.DLitConfig):
    """runs process for converting DLOG to zone build out profiles

    disaggregaes mixed into employment and residential and land use codes
    applys dwelling types using land use split by MSOA
    rebases to zone build-out profiles

    Parameters
    ----------
    input_data : global_classes.AssessData
        data to further categorise site data, whether they are estimated or not
    config : inputs.DLitConfig
        config file
    """
    if config.dev_pattern is None:
        raise ValueError(
            "cannot run development pattern without any dev_pattern parameters"
        )

    LOG.info("Initialising Development Pattern Module")

    config.output_folder.mkdir(exist_ok=True)
    LOG.info("Loading Key Inputs")
    # Create probability values for each certainty
    probability_dict_df = pd.read_csv(config.land_use.web_tag_certainty_path)
    # Convert it to a dictionary
    probability_dict = dict(zip(probability_dict_df["web_tag_certainty"], probability_dict_df["probability"]))

    # Create res_weight_dict directly by loading the relevant columns and converting them into a dictionary
    res_weight_dict_df = pd.read_csv(
        config.dev_pattern.index_weights_path, usecols=['variables', 'residential']
    )
    
    res_weight_dict = dict(zip(res_weight_dict_df['variables'], res_weight_dict_df['residential']))
    # Create emp_weight_dict directly by loading the relevant columns and converting them into a dictionary
    emp_weight_dict_df = pd.read_csv(
        config.dev_pattern.index_weights_path, usecols=['variables', 'employment']
    )
    emp_weight_dict = dict(zip(emp_weight_dict_df['variables'], emp_weight_dict_df['employment']))

    site_assessment = lu.disagg_mixed(utilities.to_dict(input_data))
    emp_sites = pd.read_csv(config.dev_pattern.emp_site_data)
    res_sites = pd.read_csv(config.dev_pattern.res_site_data)
    pop_sites = pd.read_csv(config.dev_pattern.pop_site_data)
    job_sic_sites = pd.read_csv(config.dev_pattern.emp_sic_site_data)
    tfn_tt = pd.read_csv(config.dev_pattern.tfn_tt)
    # hh_type_sites = pd.read_csv(config.dev_pattern.hh_type_site_data)
    # pop_tt_sites = pd.read_csv(config.dev_pattern.pop_tt_site_data)
    # job_sic_soc_sites = pd.read_csv(config.dev_pattern.emp_sic_soc_site_data)
    # Get base year data
    LOG.info("Loading base year data")
    # Get base year zonal total pop, hh and jobs without dimensions
    by_data_tot = pd.read_csv(config.dev_pattern.lsoa_data_path)
    # Get base year zonal total pop, hh and jobs with full dimensions
    lsoa_hh_column_names = [
        "accom_h",
        "ns_sec",
        "adults",
        "car_availability",
        "children",
        "lsoa2021_id",
        "household"
    ]
    hh_type_columns = [
        "accom_h",
        "ns_sec",
        "adults",
        "car_availability", #no car, 1 car, 2+ cars
        "children"
    ]
    lsoa_pop_by_tt_column_names = [
        "lsoa2021_id",
        "tt",
        "population",
    ]
    pop_type_columns = [
        "tt",
        "gender",
        "aws",
        "soc",
        "ns",
        "hh_type",
        "car_ownership", # without and with car
    ]

    lsoa_jobs_column_names = [
        "sic_2d",
        "soc",
        "lsoa2021_id",
        "jobs",
    ]
    job_type_columns = [
        "sic_2d",
        "soc",
    ]

    by_lsoa_hh_type = pd.read_csv(
        config.land_use.lsoa_hh_types_path, 
        names=lsoa_hh_column_names, 
        header=0, 
        index_col=None
    )

    by_lsoa_pop_tt = pd.read_csv(
        config.land_use.lsoa_traveller_type_path,
        names=lsoa_pop_by_tt_column_names,
        header=0,
        index_col=None
    )
    by_lsoa_pop_tt = by_lsoa_pop_tt.merge(
        tfn_tt, on="tt", how="left"
    )[["lsoa2021_id", "population"] + pop_type_columns[:-1] ]

    # Define the mapping
    without_car = [1, 3, 6]
    by_lsoa_pop_tt["car_ownership"] = by_lsoa_pop_tt["hh_type"].apply(
        lambda x: 1 if x in without_car else 2
    )
    by_lsoa_pop_tt = by_lsoa_pop_tt[["lsoa2021_id"] + pop_type_columns +  ["population"]]

    by_lsoa_jobs_sic_soc = pd.read_csv(
        config.land_use.lsoa_jobs_path, 
        names=lsoa_jobs_column_names, 
        header=0, 
        index_col=None
    )

    # Define the mapping for household car    
    # Key parameter and paths
    model_zone = config.dev_pattern.geo_boundary.value
    base_year = config.dev_pattern.base_year
    base_year_int = int(base_year)
    key_output_path = config.output_folder / f"05_{model_zone}"
    key_output_path.mkdir(exist_ok=True)
    key_agg_path = config.output_folder / f"05_aggregation"
    key_agg_path.mkdir(exist_ok=True)
   

    key_columns = ["site_reference_id", "easting", "northing", "web_tag_certainty"]
    build_out_columns = [str(year) for year in range(base_year_int + 1, 2067)]

    LOG.info("Creating list of sites with estimated development values")
    # list of residential sites with estimated values
    resi_estsite_reference_ids = get_site_reference_ids(
        site_assessment["residential"],
        "missing_area",
        "missing_gfa_or_dwellings_no_site_area",
    )
    # list of employment sites with estimated values
    emp_estsite_reference_ids = get_site_reference_ids(
        site_assessment["employment"],
        "missing_area",
        "missing_gfa_or_dwellings_no_site_area",
    )

    LOG.info("Processing base year land use data")
    # get totals before zone translation
    by_tot_hhs_prev = by_data_tot["household"].sum()
    by_tot_pops_prev = by_data_tot["population"].sum()
    by_tot_jobs_prev = by_data_tot["jobs"].sum()
    LOG.info(
        f"Sum of Input Totals-- Total Household: {by_tot_hhs_prev}, Total Population: {by_tot_pops_prev}, Total Jobs: {by_tot_jobs_prev}"
    )
    zone_translator = ZoneTranslator(config)
    if model_zone == inputs.GeoBoundary.LSOA:
        by_data_processed = zone_translator.merge_data(
            by_data=by_data_tot,
            columns_to_process = ["household", "population", "jobs"], 
            merge_translation_data = False,
            aggregate_by_zone = False,
            compute_area = True, 
            calculate_density = True,
            model_zone_data= False
        )
    else:
        by_data_processed = zone_translator.merge_data(
            by_data=by_data_tot,
            columns_to_process = ["household", "population", "jobs"], 
            merge_translation_data = True,
            aggregate_by_zone = True,
            compute_area = True, 
            calculate_density = True,
            model_zone_data= False
        )

    LOG.info("Checking base year totals before and after converting LSOA to the pre-defined model zone")
    # get totals after zone translation and other calculations
    by_tot_hhs_post = by_data_processed["household"].sum()
    by_tot_pops_post = by_data_processed["population"].sum()
    by_tot_jobs_post = by_data_processed["jobs"].sum()
    LOG.info(
        f"Sum of Totals after translating LSOA to pre-defined zone-- Total Household: {by_tot_hhs_post}, Total Population: {by_tot_pops_post}, Total Jobs: {by_tot_jobs_post}"
    )
    totals_dict = {
        "Category": ["Household", "Population", "Jobs"],
        "Before Translation": [by_tot_hhs_prev, by_tot_pops_prev, by_tot_jobs_prev],
        "After Translation": [by_tot_hhs_post, by_tot_pops_post, by_tot_jobs_post]
    }
    totals_df = pd.DataFrame(totals_dict)
    # Format the numbers with commas as thousand separators
    totals_df["Before Translation"] = totals_df["Before Translation"].apply(lambda x: "{:,}".format(x))
    totals_df["After Translation"] = totals_df["After Translation"].apply(lambda x: "{:,}".format(x))
    # Specify the output file path
    totals_df_file = f"totals_before_after_{model_zone}_translation.csv"

    # Save the DataFrame to a CSV file
    utilities.write_to_csv(key_output_path / totals_df_file, totals_df)
    LOG.info("Processing base year zonal data and stats")
    by_columns_stats = [
        "household",
        "population",
        "jobs",
        "ho_den",
        "po_den",
        "jo_den",
    ]
    by_data_stats = stats.basic_statistics(by_data_processed, by_columns_stats)
    by_data_file = f"by_{model_zone}_data.csv"
    by_data_stats_file = f"by_{model_zone}_data_stats.csv"
    utilities.write_to_csv(key_output_path / by_data_file, by_data_processed)
    utilities.write_to_csv(key_output_path / by_data_stats_file, by_data_stats)

    LOG.info("Processing site data for year 2024 upwards")
    res_zone_sites = process_site_data(
        res_sites,
        by_data_processed,
        "Residential",
        resi_estsite_reference_ids,
        key_columns,
        build_out_columns,
        probability_dict,
        SiteZoneProcessor(config),
        use_prob_for_size=False,
    ).fillna(0)
    print("res_zone_sites column headers", res_zone_sites.columns)
    
    emp_zone_sites = process_site_data(
        emp_sites,
        by_data_processed,
        "Employment",
        emp_estsite_reference_ids,
        key_columns,
        build_out_columns,
        probability_dict,
        SiteZoneProcessor(config),
        use_prob_for_size=False,
    ).fillna(0)
    print("emp_zone_sites column headers", emp_zone_sites.columns)

    LOG.info("Starting process to categorise large sites")
    # Columns to explore for both residential and employment sites
    columns_to_explore = [
        "sum_proposed",
        "ho_den",
        "po_den",
        "jo_den",
        "n_e_ratio",
        "ctrd_shift_ratio",
    ]
    zscore_suffix = 'zscore' #'zscore', 'robust_zscore', 'modified_zscore'

    index_col = "weighted_index"
    large_sites_res = LargeSites(site_data=res_zone_sites, columns_to_explore=columns_to_explore)
    large_sites_emp = LargeSites(site_data=emp_zone_sites, columns_to_explore=columns_to_explore)

    LOG.info("Procesing stats and calculating zscores of each attribute")
    res_site_count, res_site_zone, res_large_site_zone, res_large_site_list = large_sites_res.large_sites_selection(
        category="residential",
        zscore_suffix=zscore_suffix,
        weight_dict=res_weight_dict,
        index_col=index_col,
        index_threshold=1.5,
    )
    emp_site_count, emp_site_zone, emp_large_site_zone, emp_large_site_list = large_sites_emp.large_sites_selection(
        category="employment",
        zscore_suffix=zscore_suffix,
        weight_dict=emp_weight_dict,
        index_col=index_col,
        index_threshold=0.4,
    )

    LOG.info("Exporting key outputs by sites")

    res_site_count_summary_file = f"residential_sites_count_{model_zone}_summary.csv"
    emp_site_count_summary_file = f"employment_sites_count_{model_zone}_summary.csv"
    utilities.write_to_csv(
        config.output_folder / res_site_count_summary_file, res_site_count
    )
    utilities.write_to_csv(
        config.output_folder / emp_site_count_summary_file, emp_site_count
    )

    res_file_name = f"residential_site_{model_zone}.csv"
    emp_file_name = f"employment_site_{model_zone}.csv"
    utilities.write_to_csv(key_output_path / res_file_name, res_site_zone)
    utilities.write_to_csv(key_output_path / emp_file_name, emp_site_zone)

    large_res_sites_file_name = f"large_residential_site_{model_zone}.csv"
    large_emp_sites_file_name = f"large_employment_site_{model_zone}.csv"
    utilities.write_to_csv(
        key_output_path / large_res_sites_file_name, res_large_site_zone
    )
    utilities.write_to_csv(
        key_output_path / large_emp_sites_file_name, emp_large_site_zone
    )


    LOG.info("Calculating statistics for residential and employment sites")
    res_stats = large_sites_res._calculate_statistics()
    emp_stats= large_sites_emp._calculate_statistics()

    res_stats_file = f"residential_sites_stats_{model_zone}.csv"
    emp_stats_file = f"employment_sites_stats_{model_zone}.csv"

    utilities.write_to_csv(key_output_path / res_stats_file, res_stats)
    utilities.write_to_csv(key_output_path / emp_stats_file, emp_stats)

    visualization = config.dev_pattern.viz_distribution

    if visualization:
        LOG.info("Attribute value distribution plot")
        plot_path = config.output_folder / "05_plot_distribution_attributes"
        plot_path.mkdir(exist_ok=True)

        LOG.info(f"Visualizing the distribution of residential site attributes")
        stats.plot_distribution(
            res_zone_sites, columns_to_explore, plot_path, category="res_val"
        )
        # Box plot of attributes
        stats.plot_boxplots(
            res_zone_sites, columns_to_explore, plot_path, category="res_val"
        )

        LOG.info(f"Visualizing the distribution of employment site attributes")
        stats.plot_distribution(
            emp_zone_sites, columns_to_explore, plot_path, category="emp_val"
        )
        # Box plot of attributes
        stats.plot_boxplots(
            emp_zone_sites, columns_to_explore, plot_path, category="emp_val"
        )

   
    LOG.info("Processing base year data to get profiles for household, population and jobs")
    # Process base year data for household, population and jobs with required dimensions
    # Translate LSOA to pre-defined model zone
    by_zone_hh_type_data = zone_translator.merge_data(
        by_data=by_lsoa_hh_type,
        columns_to_process=["household"],
        dimension_columns=hh_type_columns,
        merge_translation_data=True,
        aggregate_by_zone=True,
        compute_area=False,
        calculate_density=False,
        model_zone_data=False,
    )

    by_zone_pop_tt_data = zone_translator.merge_data(
        by_data=by_lsoa_pop_tt,
        columns_to_process=["population"],
        dimension_columns=pop_type_columns,
        merge_translation_data=True,
        aggregate_by_zone=True,
        compute_area=False,
        calculate_density=False,
        model_zone_data=False,
    )

    by_zone_job_sic_soc_data = zone_translator.merge_data(
        by_data=by_lsoa_jobs_sic_soc,
        columns_to_process=["jobs"],
        dimension_columns=job_type_columns,
        merge_translation_data=True,
        aggregate_by_zone=True,
        compute_area=False,
        calculate_density=False,
        model_zone_data=False,
    )

    by_zone_job_sic_data = zone_translator.merge_data(
        by_data=by_lsoa_jobs_sic_soc,
        columns_to_process=["jobs"],
        dimension_columns=["sic_2d"],
        merge_translation_data=True,
        aggregate_by_zone=True,
        compute_area=False,
        calculate_density=False,
        model_zone_data=False,
    )
    print("by_zone_job_sic_data", by_zone_job_sic_data)
    # sic_2d_subset = [97, 98, 99]
    # by_zone_job_sic_subset = by_zone_job_sic_soc_data[by_zone_job_sic_soc_data["sic_2d"].isin(sic_2d_subset)]
    # print("by_zone_job_sic_subset", by_zone_job_sic_subset)

    # Calculate ratios across dimensions for base year household, population and jobs
    cal_ratios = Ratios(config)
    zone_info = cal_ratios.zone_info_map.get(cal_ratios.geo_boundary, {})
    if not zone_info:
        LOG.error(f"No zone information found for zone {cal_ratios.geo_boundary}")
        return
    

    zone_id = zone_info.get("group_by_column")

    # Get full unique zone id list
    # zone_shape_file = zone_info.get("shapefile_path")
    # zone_id_col_shapefile = zone_info.get("zone_gdf_id_col")
    # zone_list = gpd.read_file(zone_shape_file)[zone_id_col_shapefile].unique()
    # Create a DataFrame with all zone ids
    # all_zones_df = pd.DataFrame({zone_id: zone_list})
    # print("all_zones_df", all_zones_df)
    # Calculate ratios for household, population and jobs
    # by_gb_hh_type_ratio = cal_ratios.gb_hh_type_ratios(
    #     by_zone_hh_type_data,
    #     hh_type_columns,
    # )
    # by_gb_hh_car_ratio = cal_ratios.gb_hh_type_ratios(
    #     by_zone_hh_type_data,
    #     ["car_availability"],
    # )

    by_zone_hh_type_ratio = cal_ratios.zone_hh_type_ratios(
        by_zone_hh_type_data,
        hh_type_columns,
    )
    print("by_zone_hh_type_ratio", by_zone_hh_type_ratio)
    by_zone_hh_car_ratio = cal_ratios.zone_hh_type_ratios(
        by_zone_hh_type_data,
        ["car_availability"],
    ).drop(columns=["household"]).rename(columns={"ratios": base_year})

    by_zone_pop_tt_ratio = cal_ratios.zone_traveller_type_ratios(
        by_zone_pop_tt_data,
        pop_type_columns,
    )
    print("by_zone_pop_tt_ratio", by_zone_pop_tt_ratio)
    by_zone_pop_car_ratio = cal_ratios.zone_traveller_type_ratios(
        by_zone_pop_tt_data,
        ["car_ownership"],
    ).drop(columns=["population"]).rename(columns={"ratios": base_year})
    # Calculate default soc ratios for jobs
    by_gb_job_soc_ratio = cal_ratios.gb_job_type_ratios(
        by_zone_job_sic_soc_data,
        ["soc"],
    )
    by_gb_job_soc_ratio = by_gb_job_soc_ratio.rename(columns={"ratios": "default_ratio"})


    by_zone_job_soc_over_sic_ratio = cal_ratios.zone_soc_over_sic_ratios(
        by_zone_job_sic_soc_data,
        job_type_columns,
    )


    by_zone_job_soc_over_sic_ratio = by_zone_job_soc_over_sic_ratio.merge(
        by_gb_job_soc_ratio,
        on="soc",
        how="left",        
    )
    # infill nan with default ratio
    by_zone_job_soc_over_sic_ratio["ratios"] = by_zone_job_soc_over_sic_ratio["ratios"].fillna(
        by_zone_job_soc_over_sic_ratio["default_ratio"]
    )
    by_zone_job_soc_over_sic_ratio = by_zone_job_soc_over_sic_ratio.drop(
        columns=["default_ratio"]
    )
    print("by_zone_job_sic_soc_ratio", by_zone_job_soc_over_sic_ratio)


    # Define filenames and corresponding DataFrames
    by_gb_ratios = [
        (f"by_zone_hh_car_ratio_{model_zone}.csv", by_zone_hh_car_ratio),
        (f"by_zone_pop_car_ratio_{model_zone}.csv", by_zone_pop_car_ratio),
    ]
    # Save each DataFrame to a CSV file
    for filename, df in by_gb_ratios:
        utilities.write_to_csv(key_output_path / filename, df)
    # utilities.write_to_csv(key_output_path / res_stats_file, res_stats)
 
    LOG.info("Extract future year trend of ntem car profile change for pop and household")
    trend_of_pop_car = cal_ratios.ntem_pop_car_ratios(
        base_year,
        build_out_columns
    )
    print("trend_of_pop_car", trend_of_pop_car)
    trend_of_hh_car = cal_ratios.ntem_hh_car_ratios(
        base_year,
        build_out_columns
    )
    print("trend_of_hh_car", trend_of_hh_car)

    # Generate future year car ownership ratios for population
    zone_pop_car_ratio = cal_ratios.apply_ntem_trend(
        by_zone_pop_car_ratio,
        trend_of_pop_car,
        base_year,
        build_out_columns,
        ["car_ownership"],
    )
    # Generate future year car ownership ratios for household
    zone_hh_car_ratio = cal_ratios.apply_ntem_trend(
        by_zone_hh_car_ratio,
        trend_of_hh_car,
        base_year,
        build_out_columns,
        ["car_availability"],
    )

    print("zone_pop_car_ratio", zone_pop_car_ratio)
    print("zone_hh_car_ratio", zone_hh_car_ratio)

    LOG.info("Processing zonal household, population and jobs based on D-log data")   
    site_size_column = "site_size"
    # Add the site size column to the dataframes
    res_sites[site_size_column] = np.where(res_sites["site_reference_id"].isin(res_large_site_list), "large", "small")
    res_sites["prob_val"] = res_sites["web_tag_certainty"].map(probability_dict)
    res_sites[build_out_columns] = res_sites[build_out_columns].multiply(res_sites["prob_val"], axis=0)

    pop_sites[site_size_column] =  np.where(pop_sites["site_reference_id"].isin(res_large_site_list), "large", "small")
    pop_sites["prob_val"] = pop_sites["web_tag_certainty"].map(probability_dict)
    pop_sites[build_out_columns] = pop_sites[build_out_columns].multiply(pop_sites["prob_val"], axis=0)

    emp_sites[site_size_column] =  np.where(emp_sites["site_reference_id"].isin(emp_large_site_list), "large", "small")
    emp_sites["prob_val"] = emp_sites["web_tag_certainty"].map(probability_dict)
    emp_sites[build_out_columns] = emp_sites[build_out_columns].multiply(emp_sites["prob_val"], axis=0)

    job_sic_sites[site_size_column] =  np.where(job_sic_sites["site_reference_id"].isin(emp_large_site_list), "large", "small")
    job_sic_sites["prob_val"] = job_sic_sites["web_tag_certainty"].map(probability_dict)
    job_sic_sites[build_out_columns] = job_sic_sites[build_out_columns].multiply(job_sic_sites["prob_val"], axis=0)

    site_zone_processer = SiteZoneProcessor(config)
    hh_sites_zone = site_zone_processer.zone_site_geospatial_lookup(res_sites, site_geometry_col="geometry")
    pop_sites_zone = site_zone_processer.zone_site_geospatial_lookup(pop_sites, site_geometry_col="geometry")
    job_sites_zone = site_zone_processer.zone_site_geospatial_lookup(emp_sites, site_geometry_col="geometry")
    job_sic_sites_zone = site_zone_processer.zone_site_geospatial_lookup(job_sic_sites, site_geometry_col="geometry")
    hh_zone = site_zone_processer.agg_zonal_data(
        hh_sites_zone,
        build_out_columns,
        site_size_column,
        dimension_columns=None
    )


    pop_zone = site_zone_processer.agg_zonal_data(
        pop_sites_zone,
        build_out_columns,
        site_size_column,
        dimension_columns=None
    )
    job_zone = site_zone_processer.agg_zonal_data(
        job_sites_zone,
        build_out_columns,
        site_size_column,
        dimension_columns=None
    )

    job_sic_zone = site_zone_processer.agg_zonal_data(
        job_sic_sites_zone,
        build_out_columns,
        site_size_column,
        dimension_columns=["sic_2d"],
    )
    
    zonal_household_grth, zonal_population_grth, zonal_job_grth, zonal_household, zonal_population, zonal_job = zone_translator.merge_data(
        by_data=by_data_tot,
        columns_to_process=["household", "population", "jobs"],
        by_column=base_year,
        fy_columns=build_out_columns,
        new_hh_data=hh_zone, 
        new_pop_data=pop_zone, 
        new_job_data=job_zone,
        merge_translation_data = True,
        aggregate_by_zone = True,
        compute_area=False, 
        calculate_density=False,
        model_zone_data=True,
    )

    zonal_job_sic_grth = zone_translator._model_zone_data_with_dim(
        by_data=by_zone_job_sic_data,
        new_data=job_sic_zone,
        column_to_process="jobs",
        dimension_columns=["sic_2d"],
    )
    zonal_job_sic = zone_translator._cumulative_yearly_totals(
        zonal_job_sic_grth,
        base_year,
        build_out_columns,
    )

    # zone containing large sites
    zone_hh_largesites_fy = zonal_household_grth[[zone_id] + [f"{year}_large" for year in build_out_columns if f"{year}_large" in zonal_household_grth.columns]]
    zone_pop_largesites_fy = zonal_population_grth[[zone_id] + [f"{year}_large" for year in build_out_columns if f"{year}_large" in zonal_population_grth.columns]]
    zone_job_sic_largesites_fy = zonal_job_sic_grth[[zone_id, "sic_2d"] + [f"{year}_large" for year in build_out_columns if f"{year}_large" in zonal_job_sic_grth.columns]]
    # rename year columns
    zone_hh_largesites_fy = zone_hh_largesites_fy.rename(columns={f"{year}_large": f"{year}" for year in build_out_columns if f"{year}_large" in zonal_household_grth.columns})
    zone_pop_largesites_fy = zone_pop_largesites_fy.rename(columns={f"{year}_large": f"{year}" for year in build_out_columns if f"{year}_large" in zonal_population_grth.columns})
    zone_job_sic_largesites_fy = zone_job_sic_largesites_fy.rename(columns={f"{year}_large": f"{year}" for year in build_out_columns if f"{year}_large" in zonal_job_sic_grth.columns})

    LOG.info("Disaggregating zonal data into required dimensions for household, population and jobs based on D-log data")
    zone_hh_fy = zonal_household[[zone_id] + build_out_columns]
    zone_pop_fy = zonal_population[[zone_id] + build_out_columns]
    zone_job_sic_fy = zonal_job_sic[[zone_id, "sic_2d"] + build_out_columns]

    zone_hh_car_ratio_fy = zone_hh_car_ratio[[zone_id, "car_availability"] + build_out_columns]
    zone_pop_car_ratio_fy = zone_pop_car_ratio[[zone_id, "car_ownership"] + build_out_columns]

    # Get target zonal values for household and population segmented by car ownership for future year
    zone_hh_car_target = cal_ratios.expand_by_dimension(
        zone_hh_fy,
        zone_hh_car_ratio_fy,
        build_out_columns,
        ["car_availability"],
    )
    print("zone_hh_car_target", zone_hh_car_target)

    zone_pop_car_target = cal_ratios.expand_by_dimension(
        zone_pop_fy,
        zone_pop_car_ratio_fy,
        build_out_columns,
        ["car_ownership"],
    )
    print("zone_pop_car_target", zone_pop_car_target)
    # Segment future year zonal household, population and job data into full dimensions using base year ratios
    zonal_hh_segmented = cal_ratios.apply_ratio(
        zone_hh_fy,
        build_out_columns,
        hh_type_columns,
        by_zone_hh_type_ratio,
    )



    zonal_pop_segmented = cal_ratios.apply_ratio(
        zone_pop_fy,
        build_out_columns,
        pop_type_columns,
        by_zone_pop_tt_ratio,
    )


    zonal_job_sic_segmented = cal_ratios.apply_soc_over_sic_ratio(
        zone_job_sic_fy,
        build_out_columns,
        job_type_columns,
        by_zone_job_soc_over_sic_ratio,
    )

    # Aggregate segmented zonal household and population data by car ownership (without and with car) or by car availability (no car, 1 car, 2+ cars)
    zone_hh_car_estimated = cal_ratios.zone_vals_by_type_fy(
        zonal_hh_segmented,
        ["car_availability"],
        build_out_columns,
    )
    zone_pop_car_estimated = cal_ratios.zone_vals_by_type_fy(
        zonal_pop_segmented,
        ["car_ownership"],
        build_out_columns,
    )
    print("zonal_hh_segmented:", zonal_hh_segmented)
    print("zonal_pop_segmented:", zonal_pop_segmented)
    print("zonal_job_sic_segmented:", zonal_job_sic_segmented)
    print("zone_hh_car_estimated:", zone_hh_car_estimated)
    print("zone_pop_car_estimated:", zone_pop_car_estimated)

    # Calculate scalling factor to adjust estimated car profile
    # for household and population
    zone_hh_car_scaler = cal_ratios.scaling_factors(
        zone_hh_car_target,
        zone_hh_car_estimated,
        build_out_columns,
        ["car_availability"],
    )
    zone_pop_car_scaler = cal_ratios.scaling_factors(
        zone_pop_car_target,
        zone_pop_car_estimated,
        build_out_columns,
        ["car_ownership"],
    )
    print("zone_hh_car_scaler", zone_hh_car_scaler)
    print("zone_pop_car_scaler", zone_pop_car_scaler)

    # Apply scaling factor to segmented zonal household and population future year data
    zone_hh_segmented_scaled = cal_ratios.apply_scaling_factor(
        zonal_hh_segmented,
        zone_hh_car_scaler,
        build_out_columns,
        ["car_availability"],
    )
    zone_pop_segmented_scaled = cal_ratios.apply_scaling_factor(
        zonal_pop_segmented,
        zone_pop_car_scaler,
        build_out_columns,
        ["car_ownership"],
    )

    print("zone_hh_segmented_scaled", zone_hh_segmented_scaled)
    print("zone_pop_segmented_scaled", zone_pop_segmented_scaled)

    LOG.info("Checking totals across future years before and after disaggregating zonal data into dimensions")
    # Instantiate with your build-out year columns
    comparator = TotalsComparison(build_out_columns)

    # Add comparisons
    comparator.add_comparison("Household", zonal_household, zone_hh_segmented_scaled)
    comparator.add_comparison("Population", zonal_population, zone_pop_segmented_scaled)
    comparator.add_comparison("Jobs", zonal_job, zonal_job_sic_segmented)

    # Get summary
    summary_df = comparator.get_summary()

    # Export if needed
    summary_file = f"fy_totals_comparison_{model_zone}.csv"
    utilities.write_to_csv(key_output_path / summary_file, summary_df)

    LOG.info("Calculating zonal ratios for segmented zonal data for household, population and jobs for future years")
    # Work out the ratios (profile) of dimensions based on scaled zonal data for household and population, and the segmetned jobs for future year
    fy_zone_hh_type_ratio = cal_ratios.zone_ratios_by_type_fy(
        zone_hh_segmented_scaled,
        hh_type_columns,
        build_out_columns,
        0,
    )
    fy_zone_traveller_type_ratio = cal_ratios.zone_ratios_by_type_fy(
        zone_pop_segmented_scaled,
        ["tt"],
        build_out_columns,
        0,
    )[[zone_id, "tt"] + build_out_columns]

    fy_zone_job_type_ratio = cal_ratios.zone_ratios_by_type_fy(
        zonal_job_sic_segmented,
        job_type_columns,
        build_out_columns,
        [0,1],
    )
    print("fy_zone_hh_type_ratio", fy_zone_hh_type_ratio)
    print("fy_zone_traveller_type_ratio", fy_zone_traveller_type_ratio)
    print("fy_zone_job_type_ratio", fy_zone_job_type_ratio)

    LOG.info("Expand zonal data derived from large sites to full dimensions")
    zone_hh_largesites_fy_seg = cal_ratios.expand_by_dimension(
        zone_hh_largesites_fy,
        fy_zone_hh_type_ratio,
        build_out_columns,        
        hh_type_columns,
    )
    zone_pop_largesites_fy_seg = cal_ratios.expand_by_dimension(
        zone_pop_largesites_fy,
        fy_zone_traveller_type_ratio,
        build_out_columns,
        ["tt"],
    )
    zone_job_sic_largesites_fy_seg = cal_ratios.apply_soc_over_sic_ratio(
        zone_job_sic_largesites_fy,
        build_out_columns,
        job_type_columns,
        by_zone_job_soc_over_sic_ratio,
    )

    LOG.info("Checking totals across future years before and after disaggregating zonal data into dimensions for large sites")

    # Add comparisons
    comparator.add_comparison("Household", zone_hh_largesites_fy, zone_hh_largesites_fy_seg)
    comparator.add_comparison("Population", zone_pop_largesites_fy, zone_pop_largesites_fy_seg)
    comparator.add_comparison("Jobs", zone_job_sic_largesites_fy, zone_job_sic_largesites_fy_seg)

    # Get summary
    summary_df = comparator.get_summary()

    # Export if needed
    summary_file = f"fy_largesitetotals_comparison_{model_zone}.csv"
    utilities.write_to_csv(key_output_path / summary_file, summary_df)

    LOG.info("Combining zonal total and growth from large sites into a single data frame for segmented household, population and jobs for future year")
    # rename the column for year in build_out_columns
    zone_hh_largesites_fy_seg = zone_hh_largesites_fy_seg.rename(columns={year: f"{year}_large" for year in build_out_columns})
    zone_pop_largesites_fy_seg = zone_pop_largesites_fy_seg.rename(columns={year: f"{year}_large" for year in build_out_columns})
    zone_job_sic_largesites_fy_seg = zone_job_sic_largesites_fy_seg.rename(columns={year: f"{year}_large" for year in build_out_columns})
    # Combine the dataframes
    final_zonal_hh_segmented = zone_hh_segmented_scaled.merge(
        zone_hh_largesites_fy_seg,
        on=[zone_id] + hh_type_columns,
        how="left",
    )

    zone_pop_segmented_scaled = zone_pop_segmented_scaled[[zone_id, "tt"] + build_out_columns]
    final_zonal_pop_segmented = zone_pop_segmented_scaled.merge(
        zone_pop_largesites_fy_seg,
        on=[zone_id, "tt"],
        how="left",
    )

    final_zonal_job_segmented = zonal_job_sic_segmented.merge(
        zone_job_sic_largesites_fy_seg,
        on=[zone_id] + job_type_columns,
        how="left",
    )
    print("final_zonal_hh_segmented", final_zonal_hh_segmented)
    print("final_zonal_pop_segmented", final_zonal_pop_segmented)
    print("final_zonal_job_sic_segmented", final_zonal_job_segmented)

    # LOG.info("Calculating accumulated growth for future year based on segmented zonal data for household, population and jobs")
    # # zonal_growth_hh_type = zone_translator._cumulative_yearly_growth(
    # #     zonal_hh_segmented.reset_index(),
    # #     base_year,
    # #     build_out_columns,
    # # )    
    # # zonal_growth_pop_type = zone_translator._cumulative_yearly_growth(
    # #     zonal_pop_segmented.reset_index(),
    # #     base_year,
    # #     build_out_columns,
    # # )
    # # zonal_growth_job_type = zone_translator._cumulative_yearly_growth(
    # #     zonal_job_sic_segmented.reset_index(),
    # #     base_year,
    # #     build_out_columns,
    # # )

    # zonal_outputs = [
    #     (f"{model_zone}_zonal_household.csv", zonal_household),
    #     (f"{model_zone}_zonal_population.csv", zonal_population),
    #     (f"{model_zone}_zonal_job.csv", zonal_job),
    # ]

    # # Write each output to CSV
    # for file_name, df in zonal_outputs:
    #     utilities.write_to_csv(key_output_path / file_name, df)

    LOG.info("Aggregating zonal household, population and jobs to LAD")
    lad_household_ab_growth, lad_household = zone_translator.lad_summary(
        zonal_household_grth,
        base_year,
        build_out_columns,
    )
    lad_population_ab_growth, lad_population = zone_translator.lad_summary(
        zonal_population_grth,
        base_year,
        build_out_columns,
    )
    lad_job_ab_growth, lad_job = zone_translator.lad_summary(
        zonal_job_grth, 
        base_year,
        build_out_columns,
    )

    # Define filenames and corresponding DataFrames
    lad_outputs = [
        ("lad_household.csv", lad_household),
        ("lad_population.csv", lad_population),
        ("lad_job.csv", lad_job),
        ("lad_household_growth.csv", lad_household_ab_growth),
        ("lad_population_growth.csv", lad_population_ab_growth),
        ("lad_job_growth.csv", lad_job_ab_growth),
    ]

    # Write each LAD-level output to CSV
    for file_name, df in lad_outputs:
        utilities.write_to_csv(key_agg_path / file_name, df)


    
    LOG.info("Further transforming and processing land use data for tripend module")
    data_frames = {
        "soc_sic_emp": final_zonal_job_segmented,
        "tt_pop": final_zonal_pop_segmented,
        "tfn_tt": tfn_tt,
        "hh": final_zonal_hh_segmented,
    }

    output_folders = {
        "soc_sic_emp": key_output_path / "dlog_soc_sic_emp",
        "tt_pop": key_output_path / "dlog_tt_pop",
        "hh": key_output_path / "dlog_hh"
    }

    for folder in output_folders.values():
        folder.mkdir(exist_ok=True)
    

    LOG.info("Transforming and processing land use data for tripend module")
    prep_teinput = PrepTripends(config)

    for dtype in ["soc_sic_emp", "tt_pop", "hh"]:
        processed_data = prep_teinput.process_data(
            data_frames[dtype], 
            dtype, 
            data_frames["tfn_tt"] if dtype == "tt_pop" else None
        )

        for year_col, pivot_df in processed_data.items():
            category = next((cat for cat in prep_teinput.CATEGORIES if cat in year_col), '')

            output_folder = output_folders[dtype] / f"{dtype}{category}"
            output_folder.mkdir(exist_ok=True)

            output_file = output_folder / f"dlog_{dtype}_{year_col}.dvec"
            prep_teinput.perform_segmentation_and_zoning(pivot_df, dtype, output_file)

    LOG.info("Ending Development Pattern Module")

# if __name__ == "__main__":