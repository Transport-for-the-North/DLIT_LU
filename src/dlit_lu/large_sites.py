"""Performs the filtering process to select large development sites from the DLOG data."""

# standard imports
import logging
import pathlib

# from pathlib import Path
from typing import Optional, Dict, Any

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

# # from caf.base.data_structures import DVector
# import caf.base as cb


# constants
LOG = logging.getLogger(__name__)


class BaseZoneHandler:
    def __init__(
        self,
        config: inputs.DLitConfig,
        geo_boundary_override: Optional[inputs.GeoBoundary] = None,
    ):
        self.config: inputs.DLitConfig = config
        self.geo_boundary: inputs.GeoBoundary = (
            geo_boundary_override or config.large_sites.geo_boundary
        )
        self.zone_info_map: Dict[inputs.GeoBoundary, Dict[str, Any]] = {
            inputs.GeoBoundary.LSOA: {
                "shapefile_path": config.land_use.lsoa_shapefile_path,
                "group_by_column": "lsoa2021_id",
                "zone_gdf_id_col": "LSOA21CD",
                "prop_column": None,  # No proportion column needed for LSOA
                "prop_column_res": None,  # No proportion column needed for LSOA
                "prop_column_emp": None,  # No proportion column needed for LSOA
                "translation_path": None,  # No translation needed for LSOA
                "centroid_files": {
                    "hh": config.large_sites.lsoa_hh_centroids,
                    "emp": config.large_sites.lsoa_emp_centroids,
                    "pop": config.large_sites.lsoa_pop_centroids,
                },
                "zone_to_lad_path": config.large_sites.summary_data.lsoa_to_lad_file,
                "lad_id_col": "lad2013_id",
                "zone_to_lad_prop": "lsoa2021_to_lad2013",
                "ntem_hh_car_ratio": config.split.lsoa_hh_car,
                "ntem_pop_car_ratio": config.split.lsoa_pop_car,
                "ntem_pop_age_ratio": config.split.lsoa_pop_age,
            },
            inputs.GeoBoundary.NORMITS: {
                "shapefile_path": config.large_sites.normits_shapefile_path,
                "group_by_column": "normits_v3.3_id",
                "zone_gdf_id_col": "normits_id",
                "prop_column": "lsoa21_to_normits_v3.3_spatial",
                "prop_column_res": "lsoa21_to_normits_v3.3_pop",
                "prop_column_emp": "lsoa21_to_normits_v3.3_emp",
                "translation_path": config.large_sites.lsoa_to_normits,
                "centroid_files": {
                    "hh": config.large_sites.normits_hh_centroids,
                    "emp": config.large_sites.normits_emp_centroids,
                    "pop": config.large_sites.normits_pop_centroids,
                },
                "zone_to_lad_path": config.large_sites.summary_data.normits_to_lad_file,
                "lad_id_col": "lad2013_id",
                "zone_to_lad_prop": "normits_v3.3_to_lad2013",
                "ntem_hh_car_ratio": config.split.normits_hh_car,
                "ntem_pop_car_ratio": config.split.normits_pop_car,
                "ntem_pop_age_ratio": config.split.normits_pop_age,
            },
            inputs.GeoBoundary.NOHAM: {
                "shapefile_path": config.large_sites.noham_shapefile_path,
                "group_by_column": "noham_v3.7_id",
                "zone_gdf_id_col": "ZONE ID_v3",
                "prop_column": "lsoa2021_to_noham_v3.7",
                "prop_column_res": None,  # not available for now
                "prop_column_emp": None,  # not available for now
                "translation_path": config.large_sites.lsoa_to_noham,
                "centroid_files": {
                    "hh": config.large_sites.noham_hh_centroids,
                    "emp": config.large_sites.noham_emp_centroids,
                    "pop": config.large_sites.noham_pop_centroids,
                },
                "zone_to_lad_path": config.large_sites.summary_data.noham_to_lad_file,
                "lad_id_col": "lad2013_id",
                "zone_to_lad_prop": "noham_v3.7_to_lad2013",
            },
            inputs.GeoBoundary.NORMS: {
                "shapefile_path": config.large_sites.norms_shapefile_path,
                "group_by_column": "norms_v3.3_id",
                "zone_gdf_id_col": "unique_id",
                "prop_column": "lsoa2021_to_norms_v3.3",
                "prop_column_res": None,  # not available for now
                "prop_column_emp": None,  # not available for now
                "translation_path": config.large_sites.lsoa_to_norms,
                "centroid_files": {
                    "hh": config.large_sites.norms_hh_centroids,
                    "emp": config.large_sites.norms_emp_centroids,
                    "pop": config.large_sites.norms_pop_centroids,
                },
                "zone_to_lad_path": config.large_sites.summary_data.norms_to_lad_file,
                "lad_id_col": "lad2013_id",
                "zone_to_lad_prop": "norms_v3.3_to_lad2013",
            },
            inputs.GeoBoundary.MSOA: {
                "shapefile_path": config.large_sites.msoa_shapefile_path,
                "group_by_column": "msoa2021_id",
                "zone_gdf_id_col": "MSOA21CD",
                "prop_column": "lsoa2021_to_msoa2021",
                "prop_column_res": None,  # not available for now
                "prop_column_emp": None,  # not available for now
                "translation_path": config.large_sites.lsoa_to_msoa,
                "centroid_files": {
                    "hh": config.large_sites.msoa_hh_centroids,
                    "emp": config.large_sites.msoa_emp_centroids,
                    "pop": config.large_sites.msoa_pop_centroids,
                },
                "zone_to_lad_path": config.large_sites.summary_data.msoa_to_lad_file,
                "lad_id_col": "lad2013_id",
                "zone_to_lad_prop": "msoa2021_to_lad2013",
            },
        }

        if self.geo_boundary not in self.zone_info_map:
            raise ValueError(f"Unsupported geo_boundary: {self.geo_boundary}")

        self.zone_info: Dict[str, Any] = self.zone_info_map[self.geo_boundary]
        self.zone_gdf = parser.parse_zone(self.zone_info["shapefile_path"])


class ZoneTranslator(BaseZoneHandler):
    def __init__(
        self,
        config: inputs.DLitConfig,
        geo_boundary_override: Optional[inputs.GeoBoundary] = None,
    ):
        """
        Initialize the ZoneTranslator with the given configuration.

        Parameters
        ----------
        config : inputs.DLitConfig
            Configuration object containing zone information and settings.
        geo_boundary_override : Optional[inputs.GeoBoundary]
            Override for the geo boundary, if different from the default in config.
        """
        super().__init__(config, geo_boundary_override=geo_boundary_override)

    def translate_data(
        self,
        data: pd.DataFrame,
        category: str,
        columns_to_process: Optional[list[str]] = None,
        dimension_columns: Optional[list] = None,
        merge_translation_data: bool = True,
        aggregate_by_zone: bool = True,
    ) -> pd.DataFrame:
        """
        Merge zone translation data and perform aggregation.

        Parameters
        ----------
        data : pd.DataFrame
            Input data containing zone-level information.
        category : str
            category for determining the proportion used: residential or employment.
        columns_to_process : Optional[list[str]]
            List of columns to process for translation and aggregation.
        dimension_columns : Optional[list[str]]
            List of additional columns to include in the grouping.

        Returns
        -------
        pd.DataFrame
            A single processed DataFrame with translated and aggregated data.
        """
        zone_id = self.zone_info["group_by_column"]
        group_by_columns = (
            [zone_id] if dimension_columns is None else [zone_id] + dimension_columns
        )

        translation_path = self.zone_info["translation_path"]

        # Dynamically assign the appropriate proportional column
        if category == "residential":
            prop_column = self.zone_info.get(
                "prop_column_res", self.zone_info["prop_column"]
            )
        elif category == "employment":
            prop_column = self.zone_info.get(
                "prop_column_emp", self.zone_info["prop_column"]
            )
        else:
            prop_column = self.zone_info["prop_column"]

        # Merge data with translation if needed
        if merge_translation_data:
            data = self._merge_translation_data(
                data,
                translation_path,
                columns_to_process,
                prop_column,
                group_by_columns,
            )

        # Perform aggregation by group
        if aggregate_by_zone:
            data = self._aggregate_by_zone(data, group_by_columns, columns_to_process)

        return data

    def _merge_translation_data(
        self,
        data: pd.DataFrame,
        translation_path: str,
        columns_to_process: list[str],
        prop_column: str,
        group_by_columns: list[str],
    ) -> pd.DataFrame:
        """
        Merge the zone translation data with the input DataFrame.

        Parameters
        ----------
        data : pd.DataFrame
            Input DataFrame containing zone-level information.
        translation_path : str
            Path to the CSV file containing zone translation data.
        columns_to_process : list[str]
            List of columns to process for translation.
        prop_column : str
            Column name used for proportional adjustment.
        group_by_columns : list[str]
            List of columns to group by.

        Returns
        -------
        pd.DataFrame
            DataFrame with merged and proportionally adjusted data.
        """
        if translation_path is None:
            # Skip translation
            return data
        # Load translation data
        zone_translation = pd.read_csv(translation_path)
        # Merge and process data
        data = data.merge(zone_translation, on="lsoa2021_id", how="left").set_index(
            ["lsoa2021_id"] + group_by_columns
        )
        # Apply proportional adjustment and reset index
        data = data.loc[:, columns_to_process].multiply(data[prop_column], axis=0)

        return data.reset_index()

    def _aggregate_by_zone(
        self,
        data: pd.DataFrame,
        group_by_columns: list[str],
        columns_to_process: list[str],
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
        aggregated_df = data.groupby(group_by_columns, as_index=False)[
            columns_to_process
        ].sum()
        return aggregated_df


class ZoneProcessor(BaseZoneHandler):
    def __init__(
        self,
        config: inputs.DLitConfig,
        geo_boundary_override: Optional[inputs.GeoBoundary] = None,
    ):
        """
        Initialize the ZoneTranslator with the given configuration.

        Parameters
        ----------
        config : inputs.DLitConfig
            Configuration object containing zone information and settings.
        geo_boundary_override : Optional[inputs.GeoBoundary]
            Override for the geo boundary, if different from the default in config.
        """
        super().__init__(config, geo_boundary_override=geo_boundary_override)

    def merge_data(
        self,
        by_data: pd.DataFrame,
        column_to_process: str,
        dimension_columns: Optional[list] = None,
        by_column: Optional[str] = None,
        new_data: Optional[pd.DataFrame] = None,
        fy_columns: Optional[list[str]] = None,
        compute_area: bool = True,
        calculate_density: bool = True,
        model_zone_data: bool = True,
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
        new_data : Optional[pd.DataFrame], default=None
            DataFrame with updated Dlog data on future year.
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

        zone_gdf_id_col = self.zone_info["zone_gdf_id_col"]

        # Compute zonal area if enabled
        if compute_area:
            by_data = self._compute_zonal_area_dia(
                self.zone_gdf, by_data, zone_gdf_id_col, zone_id
            )

        # Calculate density if enabled
        if calculate_density:
            by_data = self._calculate_density(by_data, [column_to_process])

        # Merge zonal data if enabled
        if model_zone_data:
            zonal_growth = self._model_zone_data_flex(
                by_data,
                new_data,
                column_to_process,
                dimension_columns,
            )
            zonal_total = self._cumulative_yearly_totals(
                zonal_growth,
                by_column,
                fy_columns,
            )
            return (
                zonal_growth,
                zonal_total,
            )

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
            zone_to_lad_prop_col,
        )
        # Compute cumulative yearly totals for LAD-level data
        lad_data_annualgrowth = lad_data_annualgrowth.set_index(lad_id)
        lad_data_annualtot = self._cumulative_yearly_totals(
            lad_data_annualgrowth,
            base_year_column,
            future_year_columns,
        )
        return lad_data_annualgrowth, lad_data_annualtot

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

        zonal_household = by_data.merge(
            new_hh_data,
            on=zone_col_in_by,
            # left_on=zone_col_in_by,
            # right_on=zone_col_in_new,
            how="left",
        )
        zonal_household = zonal_household[
            [zone_col_in_by, "household"] + new_hh_data.columns.to_list()
        ]
        zonal_household = zonal_household.rename(columns={"household": "2023"})
        zonal_household = zonal_household.loc[:, ~zonal_household.columns.duplicated()]
        # Merging by_data with new_pop_data to create zonal_population
        zonal_population = by_data.merge(
            new_pop_data,
            on=zone_col_in_by,
            # left_on=zone_col_in_by,
            # right_on=zone_col_in_new,
            how="left",
        )
        zonal_population = zonal_population[
            [zone_col_in_by, "population"] + new_pop_data.columns.to_list()
        ]
        zonal_population = zonal_population.rename(columns={"population": "2023"})
        zonal_population = zonal_population.loc[
            :, ~zonal_population.columns.duplicated()
        ]
        # Merging by_data with new_job_data to create zonal_jobs
        zonal_job = by_data.merge(
            new_job_data,
            on=zone_col_in_by,
            # left_on=zone_col_in_by,
            # right_on=zone_col_in_new,
            how="left",
        )
        zonal_job = zonal_job[[zone_col_in_by, "jobs"] + new_job_data.columns.to_list()]
        zonal_job = zonal_job.rename(columns={"jobs": "2023"})
        zonal_job = zonal_job.loc[:, ~zonal_job.columns.duplicated()]
        return (
            zonal_household.fillna(0),
            zonal_population.fillna(0),
            zonal_job.fillna(0),
        )

    def _model_zone_data_flex(
        self,
        by_data: pd.DataFrame,
        new_data: pd.DataFrame,
        column_to_process: str,
        dimension_columns: Optional[list[str]],
    ) -> pd.DataFrame:
        """
        Merge new dwelling, population, and job data into the existing zonal data.

        Parameters
        ----------
        by_data : pd.DataFrame
            Existing zonal data to be updated.
        new_data : pd.DataFrame
            DataFrame containing new future year data with a zone column.
        column_to_process : str
            The column name to be processed, such as household, population, or jobs.
        dimension_columns : list[str], optional
            Optional list of dimension columns (e.g. ['SIC_2d']).

        Returns
        -------
        pd.DataFrame
            Updated zonal data with merged dwelling, population, and job information.
        """
        zone_id = self.zone_info["group_by_column"]
        if dimension_columns is None:
            dimension_columns = []

        zone_col_in_by = [zone_id] + dimension_columns

        zonal_data = by_data.merge(new_data, on=zone_col_in_by, how="left")

        zonal_data = zonal_data[
            zone_col_in_by + [column_to_process] + new_data.columns.to_list()
        ]
        zonal_data = zonal_data.rename(columns={column_to_process: "2023"})
        zonal_data = zonal_data.loc[:, ~zonal_data.columns.duplicated()]

        return zonal_data.fillna(0)

    # def _model_zone_data_with_dim(
    #     self,
    #     by_data: pd.DataFrame,
    #     new_data: pd.DataFrame,
    #     column_to_process: str,
    #     dimension_columns: list[str],
    # ) -> pd.DataFrame:
    #     """
    #     Merge new dwelling, population, and job data into the existing zonal data.

    #     Parameters
    #     ----------
    #     by_data : pd.DataFrame
    #         Existing zonal data to be updated.
    #     new_ata : pd.DataFrame
    #         DataFrame containing new future year data with a zone column.
    #     column_to_process : str
    #         The column name to be processed, such as household, population, or jobs.
    #     dimension_columns : list[str]
    #         The list of columns containing dimensions.
    #     Returns
    #     -------
    #     pd.DataFrame
    #         Updated zonal data with merged dwelling, population, and job information.
    #     """
    #     # Merging by_data with new__data to create zonal_jobs segmented by sic_2d
    #     zone_id = self.zone_info["group_by_column"]
    #     zone_col_in_by = [zone_id] + dimension_columns
    #     zonal_data = by_data.merge(
    #         new_data,
    #         on=zone_col_in_by,
    #         # left_on=zone_col_in_by,
    #         # right_on=zone_col_in_new,
    #         how="left",
    #     )
    #     zonal_data = zonal_data[
    #         zone_col_in_by + [column_to_process] + new_data.columns.to_list()
    #     ]
    #     zonal_data = zonal_data.rename(columns={column_to_process: "2023"})
    #     zonal_data = zonal_data.loc[:, ~zonal_data.columns.duplicated()]

    #     return zonal_data.fillna(0)

    def _get_ladcd(
        self,
        data: pd.DataFrame,
        lookup_path: pathlib.Path,
        zone_id: str,
        lad_id: str,
        zone_to_lad_prop_col: str,
    ) -> pd.DataFrame:
        """
        Get lookup between zone and lad.

        Parameters
        ----------
        data : pd.DataFrame
            DataFrame containing zone-level data.
        lookup_path : pathlib.Path
            Path to the lookup CSV file containing zone-to-LAD mapping and proportion columns.
        zone_id : str
            Column name in data representing the zone identifier.
        lad_id : str
            Column name in lookup representing the LAD identifier.
        zone_to_lad_prop_col : str
            Column in lookup representing the proportion of zone value allocated to each LAD.
        Returns
        -------
        pd.DataFrame
            Merged dataframe.
        """

        # Load the lookup DataFrame
        lookup_df = pd.read_csv(lookup_path)
        # Merge data with lookup to assign LADs and proportions
        merged_df = data.merge(
            lookup_df[[zone_id, lad_id, zone_to_lad_prop_col]], on=zone_id, how="left"
        )

        return merged_df

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

        # Check initial totals for all val_cols
        val_cols = [base_year_column] + future_year_columns
        totals_before = {col: data[col].sum() for col in val_cols}

        # Get merged DataFrame with LAD assignments and proportions
        merged_df = self._get_ladcd(
            data=data,
            lookup_path=lookup_path,
            zone_id=zone_id,
            lad_id=lad_id,
            zone_to_lad_prop_col=zone_to_lad_prop_col,
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
        base_year_column: str = "2023",
        future_year_columns: list[str] = [
            "2024",
            "2025",
            "2026",
            "2027",
            "2028",
            "2029",
            "2030",
        ],
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
        base_year_column: str = "2023",
        future_year_columns: list[str] = [
            "2024",
            "2025",
            "2026",
            "2027",
            "2028",
            "2029",
            "2030",
        ],
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


class SiteZoneProcessor(BaseZoneHandler):
    def __init__(
        self,
        config: inputs.DLitConfig,
        geo_boundary_override: Optional[inputs.GeoBoundary] = None,
    ):
        """
        Initialize the ZoneTranslator with the given configuration.

        Parameters
        ----------
        config : inputs.DLitConfig
            Configuration object containing zone information and settings.
        geo_boundary_override : Optional[inputs.GeoBoundary]
            Override for the geo boundary, if different from the default in config.
        """
        super().__init__(config, geo_boundary_override=geo_boundary_override)

    def zone_site_geospatial_lookup(
        self, site_data: pd.DataFrame, site_geometry_col: str
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
        site_data[site_geometry_col] = gpd.points_from_xy(
            site_data["easting"], site_data["northing"]
        )
        site_gdf = gpd.GeoDataFrame(
            site_data, geometry=site_geometry_col, crs=self.zone_gdf.crs
        )

        # Perform spatial join with the zone geometries
        joined_data = gpd.sjoin(site_gdf, self.zone_gdf, how="left")

        # Retain original site columns plus zone ID
        return joined_data[
            site_data.columns.tolist() + [self.zone_info["zone_gdf_id_col"]]
        ]

    def agg_zonal_data(
        self,
        site_data: pd.DataFrame,
        build_out_columns: list,
        site_size_column: str,
        dimension_columns: Optional[list] = None,
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
        agg_all = site_data.groupby(groupby_columns, as_index=False)[
            build_out_columns
        ].sum()

        # Aggregate for large sites
        agg_large = (
            site_data[site_data[site_size_column] == "large"]
            .groupby(groupby_columns, as_index=False)[build_out_columns]
            .sum()
        )
        # Add suffix only to the build_out_columns
        agg_large = agg_large.rename(
            columns={col: f"{col}_large" for col in build_out_columns}
        )
        # Aggregate for small sites
        agg_small = (
            site_data[site_data[site_size_column] == "small"]
            .groupby(groupby_columns, as_index=False)[build_out_columns]
            .sum()
        )
        # Add suffix only to the build_out_columns
        agg_small = agg_small.rename(
            columns={col: f"{col}_small" for col in build_out_columns}
        )
        # Merge all results
        agg_data = agg_all.merge(agg_large, on=groupby_columns, how="left").merge(
            agg_small, on=groupby_columns, how="left"
        )
        # Rename zone_id_col to rename_zone_id_col
        agg_data = agg_data.rename(columns={zone_id_col: rename_zone_id_col})
        # Infill NaNs with 0
        agg_data = agg_data.fillna(0)

        return agg_data

    @staticmethod
    def accumulated_growth(
        data: pd.DataFrame, build_out_columns: list, column_prefixes: list
    ) -> pd.DataFrame:
        """
        Computes accumulated sum for each group of columns in column_prefixes.

        Parameters:
            df (pd.DataFrame): The input DataFrame.
            build_out_columns: Future year columns.
            column_prefixes (list): List of column name patterns (e.g., "", "_large", "_small").

        Returns:
            pd.DataFrame: DataFrame with accumulated values.
        """
        df_accumulated = data.copy()

        # Loop through each prefix to compute cumulative sum for the columns with the given prefix
        for prefix in column_prefixes:
            # Create column names based on year and prefix
            cols = [f"{year}{prefix}" for year in build_out_columns]

            # Apply cumulative sum across the rows (axis=1)
            df_accumulated[cols] = data[cols].cumsum(axis=1)

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
        by_data = by_data.dropna(subset=[self.zone_info["zone_gdf_id_col"]])
        return site_data.merge(
            by_data, on=self.zone_info["zone_gdf_id_col"], how="left"
        )

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
        self, site_data: pd.DataFrame, key_columns: list[str]
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
            key_columns
            + [
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
        absize_col: str = "sum_proposed",
        absize_threshold: int = "400",
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
            data_weightindex,
            index_col,
            index_threshold,
            absize_col,
            absize_threshold,
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
            raise ValueError(
                f"Unknown category: {category}. Please specify 'residential' or 'employment'."
            )

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
            -5,
            -4,
            -3,
            -2,
            -1,
            0,
            0.2,
            0.4,
            0.6,
            0.8,
            1,
            1.1,
            1.2,
            1.3,
            1.4,
            1.5,
            1.6,
            1.7,
            1.8,
            1.9,
            2,
            3,
            4,
            5,
            10,
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
        absize_col: str,
        absize_threshold: int,
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
        absize_col : str
            The column containing the absolute size values (e.g., job counts, population).
        absize_threshold : int
            The threshold for selecting large sites based on the absolute size column.

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
        large_sites = merged_sites[
            (merged_sites[index_col] > index_threshold)
            & (merged_sites[absize_col] >= absize_threshold)
        ]
        large_site_ids = large_sites["site_reference_id"].tolist()

        return merged_sites, large_sites, large_site_ids


def tot_by_pop_dwel_emp(
    hh_data: pd.DataFrame,
    pop_data: pd.DataFrame,
    emp_data: pd.DataFrame,
) -> pd.DataFrame:
    """
    Processes household, population, and employment data, then get lsoa total.

    Parameters
    ----------
    hh_pop_data: pd.DataFrame,
        the household and population dataset.
    pop_data: pd.DataFrame,
        the population dataset.
    emp_data: pd.DataFrame
        the employment dataset.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing aggregated household, population, and employment data,
        merged on the LSOA zone identifier.
    """

    hh_data = hh_data.groupby("lsoa2021_id", as_index=False).agg(
        {
            "household": "sum",
            # "population": "sum"
        }
    )

    pop_data = pop_data.groupby("lsoa2021_id", as_index=False).agg(
        {"population": "sum"}
    )

    emp_data = emp_data.groupby("lsoa2021_id", as_index=False).agg({"jobs": "sum"})

    # Merge datasets
    all_data = hh_data.merge(pop_data, on="lsoa2021_id", how="left")
    all_data = all_data.merge(emp_data, on="lsoa2021_id", how="left")
    all_data.set_index(["lsoa2021_id"], inplace=True)
    return all_data


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
    site_data["sum_proposed"] = site_data.loc[:, build_out_columns].sum(axis=1)
    columns_to_keep = key_columns + [
        "sum_proposed",
    ]
    site_data = site_data[columns_to_keep]

    site_data["prob_val"] = site_data["web_tag_certainty"].map(probability_dict)
    if use_prob_for_size:
        site_data["sum_proposed"] = site_data["prob_val"] * site_data["sum_proposed"]
    # Map development sites to pre-defined zone
    site_zone_data = sitezone_processor.zone_site_geospatial_lookup(
        site_data, site_geometry_col="geometry"
    )

    LOG.info(f"Getting attributes associated with {site_type} development sites")
    # Add the 'value_estimated' column for zone sites
    site_zone_data["value_estimated"] = site_zone_data["site_reference_id"].apply(
        lambda x: "estimated" if x in site_reference_ids else "real"
    )

    # Merge sites with associated zonal attributes
    site_zone_data = sitezone_processor.merge_zonal_attributes(site_zone_data, by_data)

    # Calculate ratio of new development to existing development
    ratio_column = "n_e_ratio"
    if site_type == "Residential":
        # Calculate the ratio for Residential sites using household data
        site_zone_data[ratio_column] = (
            site_zone_data["sum_proposed"] / site_zone_data["household"]
        )
    elif site_type == "Employment":
        # Calculate the ratio for Employment sites using jobs data
        site_zone_data[ratio_column] = (
            site_zone_data["sum_proposed"] / site_zone_data["jobs"]
        )
    else:
        # For any other site type, default ratio calculation (can be customized as needed)
        LOG.warning(
            f"Unknown site type: {site_type}. Defaulting to a ratio using jobs."
        )
        site_zone_data[ratio_column] = (
            site_zone_data["sum_proposed"] / site_zone_data["jobs"]
        )
    # Replace inf, -inf, and NaN with zero in n_e_ratio
    site_zone_data[ratio_column] = (
        site_zone_data[ratio_column].replace([np.inf, -np.inf], 0).fillna(0)
    )

    LOG.info(f"Calculating centroid shift caused by {site_type} sites")
    centroid_types = ["hh", "emp"]

    for centroid_type in centroid_types:
        site_zone_data = sitezone_processor.compute_centroid_shift(
            site_zone_data, centroid_type
        )

    ctrd_shift_ratio_col = "ctrd_shift_ratio"
    site_zone_data[ctrd_shift_ratio_col] = (
        site_zone_data["centroid_shift"] / site_zone_data["diameter"]
    )

    site_zone_data = sitezone_processor.streamline_dataset(site_zone_data, key_columns)

    return site_zone_data


def run(input_data: global_classes.AssessData, config: inputs.DLitConfig):
    """
    Identifies and categorizes large residential and employment development sites
    based on probabilistic build-out data, base year land use statistics, and
    weighted index scores.

    Parameters
    ----------
    input_data : global_classes.AssessData
        Contains base year and forecast land use data for assessment.
    config : inputs.DLitConfig
        Configuration settings, including file paths, thresholds, and zone definitions.
    """
    if config.large_sites is None:
        raise ValueError("cannot run large_sites without any large_sites parameters")

    LOG.info("Initialising Large Sites Module")
    config.output_folder.mkdir(exist_ok=True)

    # Key parameter and paths
    LOG.info("Loading key parameters and paths")
    model_zone = config.large_sites.geo_boundary.value
    model_zone_enum = inputs.GeoBoundary(model_zone.lower())
    base_year = config.large_sites.base_year
    base_year_int = int(base_year)
    end_year = config.large_sites.end_year
    end_year_int = int(end_year)
    key_output_path = config.output_folder / f"M3_ls_key_outputs"
    key_output_path.mkdir(exist_ok=True)
    key_agg_path = config.output_folder / f"M3_ls_aggregation"
    key_agg_path.mkdir(exist_ok=True)

    key_columns = ["site_reference_id", "easting", "northing", "web_tag_certainty"]
    build_out_columns = [
        str(year) for year in range(base_year_int + 1, end_year_int + 1)
    ]

    LOG.info("Loading key inputs")
    # Create probability values for each certainty
    probability_dict_df = pd.read_csv(config.land_use.web_tag_certainty_path)
    # Convert it to a dictionary
    probability_dict = dict(
        zip(
            probability_dict_df["web_tag_certainty"], probability_dict_df["probability"]
        )
    )

    # Create res_weight_dict directly by loading the relevant columns and converting them into a dictionary
    res_weight_dict_df = pd.read_csv(
        config.large_sites.index_weights_path, usecols=["variables", "residential"]
    )

    res_weight_dict = dict(
        zip(res_weight_dict_df["variables"], res_weight_dict_df["residential"])
    )
    # Strip "_index" from each variable name
    columns_to_explore = (
        res_weight_dict_df["variables"].str.replace("_index", "", regex=False).tolist()
    )
    print(columns_to_explore)
    # Create emp_weight_dict directly by loading the relevant columns and converting them into a dictionary
    emp_weight_dict_df = pd.read_csv(
        config.large_sites.index_weights_path, usecols=["variables", "employment"]
    )
    emp_weight_dict = dict(
        zip(emp_weight_dict_df["variables"], emp_weight_dict_df["employment"])
    )

    site_assessment = lu.disagg_mixed(utilities.to_dict(input_data))
    emp_sites = pd.read_csv(config.large_sites.emp_site_data)
    res_sites = pd.read_csv(config.large_sites.res_site_data)
    pop_sites = pd.read_csv(config.large_sites.pop_site_data)
    job_sic_sites = pd.read_csv(config.large_sites.emp_sic_site_data)

    LOG.info("Loading base year data")
    lsoa_hh_column_names = [
        "accom_h",
        "ns_sec",
        "adults",
        "car_availability",
        "children",
        "lsoa2021_id",
        "household",
    ]

    lsoa_pop_by_tt_column_names = [
        "lsoa2021_id",
        "tt",
        "population",
    ]

    lsoa_jobs_column_names = [
        "sic_2d",
        "soc",
        "lsoa2021_id",
        "jobs",
    ]

    by_lsoa_hh_type = pd.read_csv(
        config.land_use.lsoa_hh_types_path,
        names=lsoa_hh_column_names,
        header=0,
        index_col=None,
    )

    by_lsoa_pop_tt = pd.read_csv(
        config.land_use.lsoa_traveller_type_path,
        names=lsoa_pop_by_tt_column_names,
        header=0,
        index_col=None,
    )

    by_lsoa_jobs_sic_soc = pd.read_csv(
        config.land_use.lsoa_jobs_path,
        names=lsoa_jobs_column_names,
        header=0,
        index_col=None,
    )

    LOG.info("Instantiating the key classes")
    zt = ZoneTranslator(config)
    zp = ZoneProcessor(config)
    site_zone_processer = SiteZoneProcessor(config)
    zone_info = zp.zone_info_map.get(zp.geo_boundary, {})
    if not zone_info:
        LOG.error(f"No zone information found for zone {zp.geo_boundary}")
        return

    zone_id = zone_info.get("group_by_column")
    # zone_to_lad_path = zone_info.get("zone_to_lad_path")
    # lad_id = zone_info.get("lad_id_col")
    # zone_to_lad_prop = zone_info.get("zone_to_lad_prop")

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
    by_data_tot = tot_by_pop_dwel_emp(
        by_lsoa_hh_type,
        by_lsoa_pop_tt,
        by_lsoa_jobs_sic_soc,
    ).reset_index()

    # Prepare the base data depending on zone type
    by_translated_data = {}
    by_processed_data = {}

    for col in ["household", "population", "jobs"]:
        base_df = by_data_tot[["lsoa2021_id", col]]
        # Determine the category based on column name
        if col in ["household", "population"]:
            category = "residential"
        elif col == "jobs":
            category = "employment"
        else:
            category = None  # fallback, though this shouldn't be triggered

        if model_zone_enum == inputs.GeoBoundary.LSOA:
            translated = base_df
        else:
            translated = zt.translate_data(
                data=base_df,
                category=category,
                columns_to_process=[col],
                merge_translation_data=True,
                aggregate_by_zone=True,
            )

        by_translated_data[col] = translated

        # Process the translated data
        by_processed_data[col] = zp.merge_data(
            by_data=translated,
            column_to_process=col,
            compute_area=True,
            calculate_density=True,
            model_zone_data=False,
        )

    by_hh = by_processed_data["household"]
    by_pop = by_processed_data["population"]
    by_job = by_processed_data["jobs"]

    # Concatenate the DataFrames along the columns
    combined_by = pd.concat([by_hh, by_pop, by_job], axis=1)
    # by_columns_stats = [
    #     "household",
    #     "population",
    #     "jobs",
    #     "ho_den",
    #     "po_den",
    #     "jo_den",
    # ]
    # Optionally, remove duplicate columns if they exist
    combined_by = combined_by.loc[:, ~combined_by.columns.duplicated()]

    if model_zone_enum == inputs.GeoBoundary.LSOA:
        pass
    else:
        LOG.info(
            "Checking base year totals before and after converting LSOA to the pre-defined model zone"
        )

        # get totals before zone translation
        by_tot_hhs_prev = by_data_tot["household"].sum()
        by_tot_pops_prev = by_data_tot["population"].sum()
        by_tot_jobs_prev = by_data_tot["jobs"].sum()

        # get totals after zone translation and other calculations
        by_tot_hhs_post = by_processed_data["household"]["household"].sum()
        by_tot_pops_post = by_processed_data["population"]["population"].sum()
        by_tot_jobs_post = by_processed_data["jobs"]["jobs"].sum()

        # Check totals before and after zone translation
        print(
            f"Sum of Input Totals-- Total Household: {by_tot_hhs_prev}, Total Population: {by_tot_pops_prev}, Total Jobs: {by_tot_jobs_prev}"
        )
        print(
            f"Sum of Totals after translating LSOA to pre-defined zone-- Total Household: {by_tot_hhs_post}, Total Population: {by_tot_pops_post}, Total Jobs: {by_tot_jobs_post}"
        )

        totals_dict = {
            "Category": ["Household", "Population", "Jobs"],
            "Before Translation": [by_tot_hhs_prev, by_tot_pops_prev, by_tot_jobs_prev],
            "After Translation": [by_tot_hhs_post, by_tot_pops_post, by_tot_jobs_post],
        }
        totals_df = pd.DataFrame(totals_dict)
        # Format the numbers with commas as thousand separators
        totals_df["Before Translation"] = totals_df["Before Translation"].apply(
            lambda x: "{:,}".format(x)
        )
        totals_df["After Translation"] = totals_df["After Translation"].apply(
            lambda x: "{:,}".format(x)
        )
        # Specify the output file path
        totals_df_file = f"totals_before_after_{model_zone}_translation.csv"

        # Save the DataFrame to a CSV file
        utilities.write_to_csv(key_output_path / totals_df_file, totals_df)

    # # print(combined_by)

    # # Check for duplicated LSOA21CD values
    # # Boolean mask of duplicated values
    # duplicates_mask = combined_by["LSOA21CD"].duplicated(keep=False)
    # # Print how many are duplicated
    # print("Number of duplicated LSOA21CD values:", duplicates_mask.sum())
    # # Optionally view the duplicates
    # print(combined_by[duplicates_mask].sort_values("LSOA21CD"))

    LOG.info("Processing base year emp with sic_2d")

    # Translate LSOA to pre-defined model zone
    if model_zone_enum == inputs.GeoBoundary.LSOA:

        by_zone_job_sic_data = zt.translate_data(
            data=by_lsoa_jobs_sic_soc,
            category="employment",
            columns_to_process=["jobs"],
            dimension_columns=["sic_2d"],
            merge_translation_data=False,  # No need to merge translation again
            aggregate_by_zone=True,
        )
    else:

        by_zone_job_sic_data = zt.translate_data(
            data=by_lsoa_jobs_sic_soc,
            category="employment",
            columns_to_process=["jobs"],
            dimension_columns=["sic_2d"],
            merge_translation_data=True,
            aggregate_by_zone=True,
        )

    # LOG.info("Processing base year stats")

    # by_data_stats = stats.basic_statistics(combined_by, by_columns_stats)
    # by_data_file = f"by_{model_zone}_data.csv"
    # by_data_stats_file = f"by_{model_zone}_data_stats.csv"
    # utilities.write_to_csv(key_output_path / by_data_file, combined_by)
    # utilities.write_to_csv(key_output_path / by_data_stats_file, by_data_stats)

    LOG.info("Processing site data for year 2024 upwards")
    res_zone_sites = process_site_data(
        res_sites,
        combined_by,
        "Residential",
        resi_estsite_reference_ids,
        key_columns,
        build_out_columns,
        probability_dict,
        site_zone_processer,
        use_prob_for_size=False,
    ).fillna(0)
    # print("res_zone_sites column headers", res_zone_sites.columns)

    emp_zone_sites = process_site_data(
        emp_sites,
        combined_by,
        "Employment",
        emp_estsite_reference_ids,
        key_columns,
        build_out_columns,
        probability_dict,
        site_zone_processer,
        use_prob_for_size=False,
    ).fillna(0)
    # print("emp_zone_sites column headers", emp_zone_sites.columns)

    LOG.info("Starting process to categorise large sites")

    zscore_suffix = "zscore"  #'zscore', 'robust_zscore', 'modified_zscore'
    absize_col = "sum_proposed"
    index_col = "weighted_index"
    large_sites_res = LargeSites(
        site_data=res_zone_sites, columns_to_explore=columns_to_explore
    )
    large_sites_emp = LargeSites(
        site_data=emp_zone_sites, columns_to_explore=columns_to_explore
    )

    LOG.info("Procesing stats and calculating zscores of each attribute")
    res_site_count, res_site_zone, res_large_site_zone, res_large_site_list = (
        large_sites_res.large_sites_selection(
            category="residential",
            zscore_suffix=zscore_suffix,
            weight_dict=res_weight_dict,
            index_col=index_col,
            index_threshold=0.8,
            absize_col=absize_col,
            absize_threshold=400,
        )
    )
    emp_site_count, emp_site_zone, emp_large_site_zone, emp_large_site_list = (
        large_sites_emp.large_sites_selection(
            category="employment",
            zscore_suffix=zscore_suffix,
            weight_dict=emp_weight_dict,
            index_col=index_col,
            index_threshold=0.2,
            absize_col=absize_col,
            absize_threshold=500,
        )
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
    emp_stats = large_sites_emp._calculate_statistics()

    res_stats_file = f"residential_sites_stats_{model_zone}.csv"
    emp_stats_file = f"employment_sites_stats_{model_zone}.csv"

    utilities.write_to_csv(key_output_path / res_stats_file, res_stats)
    utilities.write_to_csv(key_output_path / emp_stats_file, emp_stats)

    visualization = config.large_sites.viz_distribution

    if visualization:
        LOG.info("Attribute value distribution plot")
        plot_path = config.output_folder / "M3_plot_distribution_attributes"
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

    LOG.info("Processing zonal household, population and jobs based on D-log site data")
    site_size_column = "site_size"
    # Add the site size column to the dataframes
    res_sites[site_size_column] = np.where(
        res_sites["site_reference_id"].isin(res_large_site_list), "large", "small"
    )
    res_sites["prob_val"] = res_sites["web_tag_certainty"].map(probability_dict)
    res_sites[build_out_columns] = res_sites[build_out_columns].multiply(
        res_sites["prob_val"], axis=0
    )

    pop_sites[site_size_column] = np.where(
        pop_sites["site_reference_id"].isin(res_large_site_list), "large", "small"
    )
    pop_sites["prob_val"] = pop_sites["web_tag_certainty"].map(probability_dict)
    pop_sites[build_out_columns] = pop_sites[build_out_columns].multiply(
        pop_sites["prob_val"], axis=0
    )

    emp_sites[site_size_column] = np.where(
        emp_sites["site_reference_id"].isin(emp_large_site_list), "large", "small"
    )
    emp_sites["prob_val"] = emp_sites["web_tag_certainty"].map(probability_dict)
    emp_sites[build_out_columns] = emp_sites[build_out_columns].multiply(
        emp_sites["prob_val"], axis=0
    )

    job_sic_sites[site_size_column] = np.where(
        job_sic_sites["site_reference_id"].isin(emp_large_site_list), "large", "small"
    )
    job_sic_sites["prob_val"] = job_sic_sites["web_tag_certainty"].map(probability_dict)
    job_sic_sites[build_out_columns] = job_sic_sites[build_out_columns].multiply(
        job_sic_sites["prob_val"], axis=0
    )

    hh_sites_zone = site_zone_processer.zone_site_geospatial_lookup(
        res_sites, site_geometry_col="geometry"
    )
    pop_sites_zone = site_zone_processer.zone_site_geospatial_lookup(
        pop_sites, site_geometry_col="geometry"
    )
    job_sites_zone = site_zone_processer.zone_site_geospatial_lookup(
        emp_sites, site_geometry_col="geometry"
    )
    job_sic_sites_zone = site_zone_processer.zone_site_geospatial_lookup(
        job_sic_sites, site_geometry_col="geometry"
    )
    hh_zone = site_zone_processer.agg_zonal_data(
        hh_sites_zone, build_out_columns, site_size_column, dimension_columns=None
    )

    pop_zone = site_zone_processer.agg_zonal_data(
        pop_sites_zone, build_out_columns, site_size_column, dimension_columns=None
    )
    job_zone = site_zone_processer.agg_zonal_data(
        job_sites_zone, build_out_columns, site_size_column, dimension_columns=None
    )

    job_sic_zone = site_zone_processer.agg_zonal_data(
        job_sic_sites_zone,
        build_out_columns,
        site_size_column,
        dimension_columns=["sic_2d"],
    )

    # get zonal total for future years, and growth of large site for future years
    (
        zonal_household_grth,
        zonal_household,
    ) = zp.merge_data(
        by_data=by_processed_data["household"],
        column_to_process="household",
        by_column=base_year,
        new_data=hh_zone,
        fy_columns=build_out_columns,
        compute_area=False,
        calculate_density=False,
        model_zone_data=True,
    )
    (
        zonal_population_grth,
        zonal_population,
    ) = zp.merge_data(
        by_data=by_processed_data["population"],
        column_to_process="population",
        by_column=base_year,
        new_data=pop_zone,
        fy_columns=build_out_columns,
        compute_area=False,
        calculate_density=False,
        model_zone_data=True,
    )
    (
        zonal_job_grth,
        zonal_job,
    ) = zp.merge_data(
        by_data=by_processed_data["jobs"],
        column_to_process="jobs",
        by_column=base_year,
        new_data=job_zone,
        fy_columns=build_out_columns,
        compute_area=False,
        calculate_density=False,
        model_zone_data=True,
    )
    (
        zonal_job_sic_grth,
        zonal_job_sic,
    ) = zp.merge_data(
        by_data=by_zone_job_sic_data,
        column_to_process="jobs",
        dimension_columns=["sic_2d"],
        by_column=base_year,
        new_data=job_sic_zone,
        fy_columns=build_out_columns,
        compute_area=False,
        calculate_density=False,
        model_zone_data=True,
    )

    zone_hh_fy = zonal_household[[zone_id] + build_out_columns]
    zone_pop_fy = zonal_population[[zone_id] + build_out_columns]
    zone_job_fy = zonal_job[[zone_id] + build_out_columns]
    zone_job_sic_fy = zonal_job_sic[[zone_id, "sic_2d"] + build_out_columns]

    # Call function for accumulated sum
    column_prefixes = ["", "_large", "_small"]
    hh_grth_zone_fy = site_zone_processer.accumulated_growth(
        zonal_household_grth, build_out_columns, column_prefixes
    )
    pop_grth_zone_fy = site_zone_processer.accumulated_growth(
        zonal_population_grth, build_out_columns, column_prefixes
    )
    job_grth_zone_fy = site_zone_processer.accumulated_growth(
        zonal_job_grth, build_out_columns, column_prefixes
    )
    job_sic_grth_zone_fy = site_zone_processer.accumulated_growth(
        zonal_job_sic_grth, build_out_columns, column_prefixes
    )
    zone_hh_largesites_fy = hh_grth_zone_fy[
        [zone_id]
        + [
            f"{year}_large"
            for year in build_out_columns
            if f"{year}_large" in zonal_household_grth.columns
        ]
    ]
    zone_pop_largesites_fy = pop_grth_zone_fy[
        [zone_id]
        + [
            f"{year}_large"
            for year in build_out_columns
            if f"{year}_large" in zonal_population_grth.columns
        ]
    ]
    zone_job_largesites_fy = job_grth_zone_fy[
        [zone_id]
        + [
            f"{year}_large"
            for year in build_out_columns
            if f"{year}_large" in zonal_job_grth.columns
        ]
    ]
    zone_job_sic_largesites_fy = job_sic_grth_zone_fy[
        [zone_id, "sic_2d"]
        + [
            f"{year}_large"
            for year in build_out_columns
            if f"{year}_large" in zonal_job_sic_grth.columns
        ]
    ]

    # rename year columns for hh and pop
    zone_hh_largesites_fy = zone_hh_largesites_fy.rename(
        columns={
            f"{year}_large": f"{year}"
            for year in build_out_columns
            if f"{year}_large" in zonal_household_grth.columns
        }
    )
    zone_pop_largesites_fy = zone_pop_largesites_fy.rename(
        columns={
            f"{year}_large": f"{year}"
            for year in build_out_columns
            if f"{year}_large" in zonal_population_grth.columns
        }
    )
    zone_job_largesites_fy = zone_job_largesites_fy.rename(
        columns={
            f"{year}_large": f"{year}"
            for year in build_out_columns
            if f"{year}_large" in zonal_job_grth.columns
        }
    )
    zone_job_sic_largesites_fy = zone_job_sic_largesites_fy.rename(
        columns={
            f"{year}_large": f"{year}"
            for year in build_out_columns
            if f"{year}_large" in zonal_job_sic_grth.columns
        }
    )

    # Set translator with override to NORMITS
    translator = ZoneTranslator(
        config, geo_boundary_override=inputs.GeoBoundary.NORMITS
    )
    te_zone = translator.geo_boundary.value

    te_zone_enum = inputs.GeoBoundary(te_zone.lower())
    # Log the boundaries for debugging
    LOG.info(f"Input model zone: {model_zone}")
    LOG.info(f"Target translation zone (override): {te_zone}")

    # Perform translation only if we're converting from LSOA to NORMITS
    if (
        model_zone_enum == inputs.GeoBoundary.LSOA
        and te_zone_enum == inputs.GeoBoundary.NORMITS
    ):
        LOG.info("Translating LSOA data to NORMITS zone and exporting CSVs...")
        # zone_hh_fy = zonal_household[[zone_id] + build_out_columns]
        # zone_pop_fy = zonal_population[[zone_id] + build_out_columns]
        # zone_job_fy = zonal_job[[zone_id] + build_out_columns]

        normits_zone_hh = translator.translate_data(
            data=zone_hh_fy,
            category="residential",
            columns_to_process=build_out_columns,
            dimension_columns=None,
            merge_translation_data=True,
            aggregate_by_zone=True,
        )
        normits_zone_pop = translator.translate_data(
            data=zone_pop_fy,
            category="residential",
            columns_to_process=build_out_columns,
            dimension_columns=None,
            merge_translation_data=True,
            aggregate_by_zone=True,
        )
        normits_zone_job = translator.translate_data(
            data=zone_job_fy,
            category="employment",
            columns_to_process=build_out_columns,
            dimension_columns=None,
            merge_translation_data=True,
            aggregate_by_zone=True,
        )

        normits_zone_hh_ls = translator.translate_data(
            data=zone_hh_largesites_fy,
            category="residential",
            columns_to_process=build_out_columns,
            dimension_columns=None,
            merge_translation_data=True,
            aggregate_by_zone=True,
        )

        normits_zone_pop_ls = translator.translate_data(
            data=zone_pop_largesites_fy,
            category="residential",
            columns_to_process=build_out_columns,
            dimension_columns=None,
            merge_translation_data=True,
            aggregate_by_zone=True,
        )

        normits_zone_job_ls = translator.translate_data(
            data=zone_job_largesites_fy,
            category="employment",
            columns_to_process=build_out_columns,
            dimension_columns=None,
            merge_translation_data=True,
            aggregate_by_zone=True,
        )
        normits_zone_hh_ls["sum"] = normits_zone_hh_ls[build_out_columns].sum(axis=1)
        normits_zone_pop_ls["sum"] = normits_zone_pop_ls[build_out_columns].sum(axis=1)
        normits_zone_job_ls["sum"] = normits_zone_job_ls[build_out_columns].sum(axis=1)

        normits_datasets = {
            "normits_zone_hh_fy": normits_zone_hh,
            "normits_zone_pop_fy": normits_zone_pop,
            "normits_zone_job_fy": normits_zone_job,
            "normits_zone_hh_largesites_fy": normits_zone_hh_ls,
            "normits_zone_pop_largesites_fy": normits_zone_pop_ls,
            "normits_zone_job_largesites_fy": normits_zone_job_ls,
        }
        for name, df in normits_datasets.items():
            # Write to CSV
            utilities.write_to_csv(key_output_path / f"{name}.csv", df)

    else:
        LOG.info(
            "Zone translation from LSOA to NORMITS was not triggered (zone type mismatch)."
        )

    LOG.info("Creating D-log zone data for household, population and jobs")
    zone_fy_data = global_classes.DlogZoneData(
        zone_hh_fy,
        zone_pop_fy,
        zone_job_fy,
        zone_job_sic_fy,
        zone_hh_largesites_fy,
        zone_pop_largesites_fy,
        zone_job_largesites_fy,
        zone_job_sic_largesites_fy,
    )
    zone_fy_data.export_to_csv(key_output_path)

    LOG.info("Aggregating zonal household, population and jobs to LAD")
    lad_household_ab_growth, lad_household = zp.lad_summary(
        zonal_household_grth,
        base_year,
        build_out_columns,
    )
    lad_population_ab_growth, lad_population = zp.lad_summary(
        zonal_population_grth,
        base_year,
        build_out_columns,
    )
    lad_job_ab_growth, lad_job = zp.lad_summary(
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

    LOG.info("Ending Large Sites Module")
    return zone_fy_data


# if __name__ == "__main__":
