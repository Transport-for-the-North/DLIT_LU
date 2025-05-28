"""Performs the filtering process to select large development sites from the DLOG data."""

# standard imports
import logging
import pathlib
from pathlib import Path
from typing import Iterator, Tuple, Optional, Dict, Any, List, Union

# third party imports
import dask.dataframe as dd
import pandas as pd
import numpy as np

# from sklearn.preprocessing import MinMaxScaler
# from scipy.spatial import cKDTree


# import os

# local imports
from dlit_lu import utilities, global_classes, inputs

# from dlit_lu import land_use as lu
from dlit_lu import large_sites as ls
from caf.base.data_structures import DVector
import caf.base as cb


# constants
LOG = logging.getLogger(__name__)


class Ratios(ls.BaseZoneHandler):
    """A class for calculating GB-wide distributions for households, travellers, and employment types."""

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

    def gb_hh_type_ratios(
        self, data: pd.DataFrame, hh_type_columns: List[str]
    ) -> pd.DataFrame:
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
        zonal_ratios["ratios"] = zonal_ratios["household"] / zonal_ratios.groupby(
            level=0
        )["household"].transform("sum")

        # Step 3: Fill NaNs caused by division by zero with 0
        zonal_ratios["ratios"] = zonal_ratios["ratios"].fillna(0)

        return zonal_ratios.reset_index()

    def gb_traveller_type_ratios(
        self, data: pd.DataFrame, pop_type_columns: List[str]
    ) -> pd.DataFrame:
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
        zonal_ratios["ratios"] = zonal_ratios["population"] / zonal_ratios.groupby(
            level=0
        )["population"].transform("sum")

        # Step 3: Fill NaNs caused by division by zero with 0
        zonal_ratios["ratios"] = zonal_ratios["ratios"].fillna(0)

        return zonal_ratios.reset_index()

    def gb_job_type_ratios(
        self, data: pd.DataFrame, job_type_columns: List[str]
    ) -> pd.DataFrame:
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

    def gb_soc_over_sic_ratios(
        self, data: pd.DataFrame, job_type_columns: List[str]
    ) -> pd.DataFrame:
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

    # def zone_soc_over_sic_ratios(
    #     self, df: pd.DataFrame, job_type_columns: List[str]
    # ) -> pd.DataFrame:
    #     """
    #     Calculates the SOC ratio within each (zone_id, sic_2d) based on jobs, using MultiIndex.

    #     Parameters
    #     ----------
    #     df : pd.DataFrame
    #         Input DataFrame with columns: 'zone_id', 'sic_2d', 'soc', 'jobs'

    #     Returns
    #     -------
    #     pd.DataFrame
    #         DataFrame with MultiIndex (zone_id, sic_2d, soc) and 'jobs' and 'soc_ratio' columns.
    #     """
    #     zone_id = self.zone_info["group_by_column"]
    #     group_by = [zone_id] + job_type_columns
    #     # Set MultiIndex
    #     zonal_ratios = df.groupby(group_by)["jobs"].sum().to_frame()
    #     # Calculate SOC ratios
    #     zonal_ratios["ratios"] = zonal_ratios["jobs"] / zonal_ratios.groupby(
    #         level=[0, 1]
    #     )["jobs"].transform("sum")

    #     # Step 3: Fill NaNs caused by division by zero with 0
    #     zonal_ratios["ratios"] = zonal_ratios["ratios"].fillna(0)

    #     return zonal_ratios.reset_index()

    def zone_soc_over_sic_ratios(
        self,
        df: pd.DataFrame,
        job_type_columns: List[str],
        input_col: str = "jobs",
        output_col: str = "soc_ratio",
    ) -> pd.DataFrame:
        """
        Calculates the SOC ratio within each (zone_id, sic_2d) group based on job counts.

        Parameters
        ----------
        df : pd.DataFrame
            Input DataFrame with job counts in the column specified by jobs_col.
        job_type_columns : List[str]
            Columns defining job classification (e.g., ['sic_2d', 'soc']).
        input_col : str
            Column name in df to be used for further calculation
        output_col : str
            Name for the output column containing the computed SOC ratios.

        Returns
        -------
        pd.DataFrame
            DataFrame with MultiIndex (zone_id, sic_2d, soc) and the job count and ratio columns.
        """
        zone_id = self.zone_info["group_by_column"]
        group_by = [zone_id] + job_type_columns

        # Group and sum job counts
        zonal_ratios = df.groupby(group_by)[input_col].sum().to_frame()

        # Calculate the SOC ratio within each (zone_id, sic_2d)
        zonal_ratios[output_col] = zonal_ratios[input_col] / zonal_ratios.groupby(
            level=[0, 1]
        )[input_col].transform("sum")

        # Replace NaNs from division by zero
        zonal_ratios[output_col] = zonal_ratios[output_col].fillna(0)

        return zonal_ratios.reset_index()

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

        # Aggregate to get total household counts per group
        grouped = data.groupby(index_cols)[build_out_columns].sum()

        grouped = grouped.reset_index()

        return grouped

    def zone_ratios_by_type_fy(
        self,
        data: pd.DataFrame,
        dimension_columns: List[str],
        build_out_columns: List[str],
        level: Union[int, List[int]] = 0,
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

        grouped = data.groupby(index_cols)[build_out_columns].sum()
        grouped = grouped.reset_index().set_index(index_cols)

        for year in build_out_columns:
            group_totals = grouped.groupby(level=level)[year].sum()
            grouped[year] = grouped[year] / grouped.index.get_level_values(level).map(
                group_totals
            )

        return grouped.fillna(0).reset_index()
        # zone_id = self.zone_info["group_by_column"]
        # index_cols = [zone_id] + dimension_columns

        # # Aggregate to get total household counts per group
        # grouped = data.groupby(index_cols)[build_out_columns].sum()

        # # Compute ratios
        # # Precompute denominators (total households per group and year)
        # group_totals = grouped.groupby(level=level)[build_out_columns].transform("sum")
        # # Compute all ratios in one vectorized operation
        # ratios = grouped[build_out_columns] / group_totals
        # # Rename ratio columns to include '_ratio' suffix
        # ratios.columns = [f"{col}" for col in build_out_columns]

        # # Optional: drop raw counts if only ratios are needed
        # grouped = grouped.drop(columns=build_out_columns)

        # # Add ratio columns to the grouped DataFrame
        # grouped = grouped.join(ratios)
        # grouped = grouped.fillna(0)
        # # for year in build_out_columns:
        # #     grouped[(year, "ratio")] = grouped[year] / grouped.groupby(level=level)[
        # #         year
        # #     ].transform("sum")

        # # # Drop raw count columns, keep only ratio columns
        # # ratio_cols = [(year, "ratio") for year in build_out_columns]
        # # zonal_ratios = grouped[ratio_cols]

        # # # Rename columns to just year
        # # zonal_ratios.columns = [str(year) for year, _ in zonal_ratios.columns]
        # # # Fill NaN values with zero
        # # zonal_ratios = zonal_ratios.fillna(0)
        # return grouped.reset_index()

    def zone_ratios_by_type_fy_dask(
        self,
        data: dd.DataFrame,
        dimension_columns: Optional[List[str]],
        build_out_columns: List[str],
        npartitions: int,
    ) -> pd.DataFrame:
        """
        Calculates the proportion of each household type per zone and year using Dask.

        Parameters
        ----------
        data : dd.DataFrame
            Input dask data by zone and data type, with counts in year columns.
        dimension_columns : Optional[List[str]]
            Columns that define household segmentations (e.g. size, age, income).
        build_out_columns : List[str]
            List of year-based columns (e.g., ['2024', '2025', ...]) with household counts.
        npartitions : int, default=4
            Number of partitions to divide the Dask DataFrame into. Higher values may improve parallelism
            but increase overhead. Lower values reduce overhead but may use more memory per partition.

        Returns
        -------
        dd.DataFrame
            Dask DataFrame with household type proportions per zone and year.
        """
        zone_id = self.zone_info["group_by_column"]
        index_cols = [zone_id] + (dimension_columns if dimension_columns else [])

        data = data.repartition(npartitions=npartitions)

        # Optionally, persist intermediate results to avoid recomputation
        data = data.persist()

        # Convert columns to float32 to reduce memory usage
        for col in build_out_columns:
            data[col] = data[col].astype("float32")

        # Group by zone + dimension(s) to get household counts
        grouped = data.groupby(index_cols)[build_out_columns].sum()

        # Group by zone only to get totals
        total_per_zone = data.groupby(zone_id)[build_out_columns].sum()

        # Reset index to merge
        grouped = grouped.reset_index()
        total_per_zone = total_per_zone.reset_index()

        # Rename total columns
        total_per_zone = total_per_zone.rename(
            columns={col: f"{col}_total" for col in build_out_columns}
        )

        # Merge and compute proportions
        merged = grouped.merge(total_per_zone, on=zone_id, how="left")
        for year in build_out_columns:
            merged[year] = merged[year] / merged[f"{year}_total"]

        merged = merged.drop(columns=[f"{col}_total" for col in build_out_columns])
        merged = merged.fillna(0)
        # Convert merged columns to float32 to reduce memory usage after computation
        for year in build_out_columns:
            merged[year] = merged[year].astype("float32")

        return merged

    def zone_ratios_by_type_single_year(
        self,
        data: pd.DataFrame,
        dimension_columns: List[str],
        output_dir: Path = None,
        file_name: str = None,
        export_csv: bool = False,
    ) -> pd.DataFrame:
        """
        Calculates the proportion of each household type per zone and year, using MultiIndex.

        Parameters
        ----------
        data : pd.DataFrame
            Household data by zone and household type, with household counts in year columns.
        Dimension_columns : List[str]
            Columns that define household segmentations (e.g. size, age, income).
        level : int or List[int], default=0
            The index level(s) to group by when calculating zone-level totals for ratio computation.
        output_dir : Path, optional
            Directory to save the CSV file.
        file_name : str, optional
            Name of the CSV file to export.
        export_csv : bool, default=False
            Whether to export the result to CSV.
        Returns
        -------
        pd.DataFrame
            MultiIndexed DataFrame with ratios for each [zone_id, *hh_type_columns], and
            one column per year showing the household type proportions.
        """
        zone_id = self.zone_info["group_by_column"]
        index_cols = [zone_id] + dimension_columns

        grouped = data.groupby(index_cols, as_index=False)["value"].sum()
        # Calculate total per group defined by `level`
        totals = grouped.groupby(zone_id)["value"].transform("sum")
        # Compute ratios
        grouped["value"] = grouped["value"] / totals
        grouped = grouped.fillna(0).reset_index(drop=True)
        if export_csv:
            output_file = output_dir / f"{file_name}.csv.bz2"
            utilities.write_to_csv(output_file, grouped)

        return grouped

    @staticmethod
    def expand_gb_ratios_by_zone(
        zone_list_dataframe: pd.DataFrame, gb_ratios: pd.DataFrame
    ) -> pd.DataFrame:
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
        by_zone_type_ratio = pd.merge(zone_list_copy, gb_ratios_copy, on="key").drop(
            columns="key"
        )

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
        data_ratios = data.reset_index(drop=False).merge(
            ratios.reset_index(drop=False),
            on=zone_id,
            how="left",
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
        data_ratios = data.reset_index(drop=False).merge(
            ratios.reset_index(drop=False),
            on=[zone_id, "sic_2d"],
        )
        data_ratios = data_ratios[key_cols + unit_cols + ["ratios"]].set_index(key_cols)
        data_ratios = data_ratios.loc[:, unit_cols].multiply(
            data_ratios["ratios"], axis=0
        )
        return data_ratios.reset_index()

    def ntem_pop_car_ratios(
        self, base_year_column: str, build_out_columns: List[str]
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
        ntem_future_year_list = [
            y for y in year_cols_in_data if int(y) >= int(base_year_column)
        ]
        extrapolated_years = [
            y for y in build_out_columns if y not in ntem_future_year_list
        ]

        extended_data = data.copy()
        x_known = np.array([int(y) for y in ntem_future_year_list])

        for row_idx, row in extended_data.iterrows():
            y_known = row[ntem_future_year_list].values.astype("float32")

            # Fit linear trend
            coeffs = np.polyfit(x_known, y_known, deg=1)
            y_pred = np.polyval(coeffs, [int(y) for y in extrapolated_years])

            # Fill extrapolated values
            for i, year in enumerate(extrapolated_years):
                extended_data.at[row_idx, year] = y_pred[i]

        # Calculate ratios for all years in build_out_column vs base_year
        for year in build_out_columns:
            extended_data[year] = extended_data[year].astype("float32") / extended_data[
                base_year_column
            ].astype("float32")

        return extended_data.drop(columns=[base_year_column])

    def ntem_hh_car_ratios(
        self, base_year_column: str, build_out_columns: List[str]
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
        ntem_future_year_list = [
            y for y in year_cols_in_data if int(y) >= int(base_year_column)
        ]
        extrapolated_years = [
            y for y in build_out_columns if y not in ntem_future_year_list
        ]

        extended_data = data.copy()
        x_known = np.array([int(y) for y in ntem_future_year_list])

        for row_idx, row in extended_data.iterrows():
            y_known = row[ntem_future_year_list].values.astype("float32")

            # Fit linear trend
            coeffs = np.polyfit(x_known, y_known, deg=1)
            y_pred = np.polyval(coeffs, [int(y) for y in extrapolated_years])

            # Fill extrapolated values
            for i, year in enumerate(extrapolated_years):
                extended_data.at[row_idx, year] = y_pred[i]

        # Calculate ratios for all years in build_out_column vs base_year
        for year in build_out_columns:
            extended_data[year] = extended_data[year].astype("float32") / extended_data[
                base_year_column
            ].astype("float32")

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
            zone_ratios[year] = zone_ratios[year].astype("float32") * zone_ratios[
                base_year_column
            ].astype("float32")
        # Normalize each year's values within each zone so they sum to 1 across categories
        for year in build_out_columns:
            total_per_zone = zone_ratios.groupby(zone_id)[year].transform("sum")
            zone_ratios[year] = zone_ratios[year] / total_per_zone

        return zone_ratios

    def expand_by_dimension(
        self,
        zone_data: pd.DataFrame,
        zone_ratio: pd.DataFrame,
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

        zone_ratio : pd.DataFrame
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
        if "sic_2d" in zone_data.columns:
            id_list = [zone_id, "sic_2d"]
        else:
            id_list = [zone_id]  # Add 'sic_2d' for jobs data

        # Melt both dataframes to long format
        data_long = zone_data.melt(
            id_vars=id_list,
            value_vars=build_out_columns,
            var_name="year_variant",
            value_name="vals",
        )

        ratio_long = zone_ratio.melt(
            id_vars=id_list + dimension_columns,
            value_vars=build_out_columns,
            var_name="year_variant",
            value_name="ratios",
        )

        # Merge on zone_id and year
        merged = data_long.merge(ratio_long, on=id_list + ["year_variant"], how="left")

        # Calculate households by car availability
        merged["val_by_dimension"] = merged["vals"].astype("float32") * merged[
            "ratios"
        ].astype("float32")

        # Pivot to wide format (optional)
        wide = merged.pivot_table(
            index=id_list + dimension_columns,
            columns="year_variant",
            values="val_by_dimension",
        ).reset_index()

        wide.columns.name = None  # Clean up column index name
        return wide

    def dask_pivot_table_like(
        self,
        ddf: dd.DataFrame,
        index_cols: list[str],
        column: str,
        values: str,
    ) -> dd.DataFrame:
        ddf[column] = ddf[column].astype(str)

        grouped = ddf.groupby(index_cols + [column])[values].sum().reset_index()

        def pivot_partition(pdf):
            return pdf.pivot_table(
                index=index_cols,
                columns=column,
                values=values,
            )

        pivoted = grouped.map_partitions(pivot_partition)
        pivoted = pivoted.reset_index().fillna(0)

        return pivoted

    def expand_by_dimension_dask(
        self,
        zone_data: pd.DataFrame,
        zone_ratio_ddf: dd.DataFrame,
        build_out_columns: list[str],
        dimension_columns: list[str],
        npartitions: int,
    ) -> pd.DataFrame:
        """
        Calculate the number of households by given dimension categories per zone and year using Dask.

        Parameters
        ----------
        zone_data : pd.DataFrame
            DataFrame with total projected households per zone and year.
            Expected columns: [zone_id] + build_out_columns.

        zone_ratio : pd.DataFrame
            DataFrame with ratios per zone and dimension (e.g., car availability) for each year.
            Expected columns: [zone_id, *dimension_columns] + build_out_columns.

        build_out_columns : list[str]
            List of column names representing future years (e.g. ['2024', '2025', ..., '2066']).

        dimension_columns : list[str]
            List of columns representing the dimensions (e.g., car availability, income group).
            Must be explicitly provided.
        npartitions : int
            Number of partitions to divide the Dask DataFrame into. Higher values may improve parallelism
            but increase overhead. Lower values reduce overhead but may use more memory per partition.


        Returns
        -------
        pd.DataFrame
            A wide-format DataFrame with [zone_id, dimension_categories] and one column per year,
            representing the number of households by each dimension category.
        """
        zone_id = self.zone_info["group_by_column"]
        # Ensure year columns (e.g., build_out_columns) are float32 before Dask conversion
        zone_data[build_out_columns] = zone_data[build_out_columns].astype("float32")
        # Dask conversions
        zone_data_ddf = dd.from_pandas(zone_data, npartitions=npartitions)
        zone_data_ddf[build_out_columns] = zone_data_ddf[build_out_columns].astype(
            "float32"
        )
        zone_ratio_ddf = zone_ratio_ddf.repartition(npartitions=npartitions)
        # zone_ratio_ddf = dd.from_pandas(zone_ratio, npartitions=npartitions)

        # Determine ID columns
        id_list = [zone_id, "sic_2d"] if "sic_2d" in zone_data.columns else [zone_id]

        # Melt to long format
        data_long = dd.melt(
            zone_data_ddf,
            id_vars=id_list,
            value_vars=build_out_columns,
            var_name="year_variant",
            value_name="vals",
        )

        ratio_long = dd.melt(
            zone_ratio_ddf,
            id_vars=id_list + dimension_columns,
            value_vars=build_out_columns,
            var_name="year_variant",
            value_name="ratios",
        )

        # Merge on keys
        merged = data_long.merge(ratio_long, on=id_list + ["year_variant"], how="left")

        # Compute values by dimension
        merged["val_by_dimension"] = merged["vals"].astype("float32") * merged[
            "ratios"
        ].astype("float32")

        # Aggregate by group
        group_cols = id_list + dimension_columns + ["year_variant"]
        grouped = merged.groupby(group_cols)["val_by_dimension"].sum().reset_index()

        # # Pivot to wide format
        # pivoted = grouped.pivot_table(
        #     index=id_list + dimension_columns,
        #     columns="year_variant",
        #     values="val_by_dimension",
        # ).reset_index()

        # # Final conversion back to pandas
        # result_df = pivoted.compute()
        # result_df.columns.name = None
        # return result_df

        # # Compute before pivoting
        # grouped_pd = grouped.compute()

        # # Pivot using Pandas
        # pivot_result = self.dask_pivot_table_like(
        #     grouped,
        #     index_cols=id_list + dimension_columns,
        #     column="year_variant",
        #     values="val_by_dimension",
        #     # aggfunc="sum",  # explicitly add aggfunc
        # ).reset_index()

        return grouped

    def slice_dask_df_by_year(
        self,
        ddf: dd.DataFrame,
        year_column: str,
        value_column: str,
        index_columns: List[str],
        years: List[str],
    ) -> Dict[str, dd.DataFrame]:
        """
        Slice a Dask DataFrame into separate DataFrames by year.

        Parameters
        ----------
        df : dd.DataFrame
            The Dask DataFrame in long format.
        year_column : str
            Column name containing year identifiers (e.g., "year_variant").
        value_column : str
            Name of the value column to be renamed per year (e.g., "val_by_dimension").
        index_columns : List[str]
            List of columns to retain (e.g., ['zone_id', 'dimension_1', ...]).
        years : List[str]
            List of years (as strings or values convertible to str) to extract.

        Returns
        -------
        Dict[str, dd.DataFrame]
            Dictionary mapping each year to its corresponding sliced and renamed Dask DataFrame.
        """
        results = {}

        for year in years:
            temp = ddf[ddf[year_column] == year][index_columns + [value_column]]
            temp = temp.rename(columns={value_column: year})

            results[year] = temp

        return results

    # def expand_by_dimension_single_year(
    #     self,
    #     zone_data_dict: dict[str, pd.DataFrame],
    #     zone_ratio_dict: dict[str, pd.DataFrame],
    #     dimension_columns: list[str],
    # ) -> dict[str, pd.DataFrame]:
    #     """
    #     Expand zone-level totals by a dimension (e.g., car availability), using ratios for each zone and dimension.

    #     Parameters
    #     ----------
    #     zone_data_dict : dict[str, pd.DataFrame]
    #         Dictionary of DataFrames keyed by year (e.g., "2024"), each containing zone totals with columns: [zone_id, "value"].

    #     zone_ratio_dict : dict[str, pd.DataFrame]
    #         Dictionary of DataFrames keyed by year (e.g., "2024"), each with ratios by zone and dimension columns:
    #         [zone_id, *dimension_columns, "value"].

    #     dimension_columns : list[str]
    #         List of columns representing the segmentation (e.g., ["car_availability"]).

    #     Returns
    #     -------
    #     dict[str, pd.DataFrame]
    #         Dictionary of DataFrames for each year with expanded values: [zone_id, *dimension_columns, "value"].
    #     """

    #     zone_id = self.zone_info["group_by_column"]
    #     result = {}

    #     for year in zone_data_dict:
    #         zone_df = zone_data_dict[year]
    #         ratio_df = zone_ratio_dict.get(year)

    #         if ratio_df is None:
    #             raise ValueError(f"Ratio data for year {year} is missing.")

    #         # Dynamically determine if 'sic_2d' is present
    #         if "sic_2d" in zone_df.columns:
    #             id_list = [zone_id, "sic_2d"]
    #         else:
    #             id_list = [zone_id]

    #         # Merge on zone_id (+ sic_2d if present)
    #         merged = ratio_df.merge(
    #             zone_df[id_list + ["value"]],
    #             on=id_list,
    #             how="left",
    #             suffixes=("_ratio", "_total"),
    #         )

    #         # Calculate disaggregated value
    #         merged["value"] = merged["value_total"] * merged["value_ratio"]

    #         # Return only relevant columns
    #         result[year] = merged[id_list + dimension_columns + ["value"]]

    #     return result

    def scaling_factors(
        self,
        target_df: pd.DataFrame,
        estimated_df: pd.DataFrame,
        build_out_columns: list[str],
        dimension_columns: list[str],
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
            suffixes=("_target", "_estimated"),
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

    def scaling_factors_dask(
        self,
        target_df: pd.DataFrame,
        estimated_df: pd.DataFrame,
        build_out_columns: list[str],
        dimension_columns: Optional[list[str]],
        npartitions: int,
    ) -> pd.DataFrame:
        """
        Calculate scaling factors using Dask by dividing target values by estimated values.
        Converts Pandas DataFrames to Dask for scalability and returns result as Pandas.

        Parameters
        ----------
        target_df : pd.DataFrame
            DataFrame with target values.
        estimated_df : pd.DataFrame
            DataFrame with estimated values.
        build_out_columns : list[str]
            List of column names representing years (e.g., ['2024', '2025', ..., '2066']).
        dimension_columns : Optional[list[str]]
            List of dimension columns (e.g., ['car_availability']). Defaults to None.
        npartitions : int
            Number of partitions to use when converting DataFrames to Dask for parallel computation.

        Returns
        -------
        pd.DataFrame
            DataFrame with zone_id, dimension columns (if any), and scaling factors per year.
        """
        import dask.dataframe as dd

        zone_id = self.zone_info["group_by_column"]
        dims = dimension_columns if dimension_columns else []

        # Convert pandas to dask
        target_ddf = dd.from_pandas(target_df, npartitions=npartitions)
        estimated_ddf = dd.from_pandas(estimated_df, npartitions=npartitions)

        # Merge the dataframes
        merged_ddf = target_ddf.merge(
            estimated_ddf,
            on=[zone_id] + dims,
            how="outer",
            suffixes=("_target", "_estimated"),
        )

        # Compute scaling factors for each year
        for year in build_out_columns:
            target_col = f"{year}_target"
            estimated_col = f"{year}_estimated"

            if (
                target_col not in merged_ddf.columns
                or estimated_col not in merged_ddf.columns
            ):
                print(
                    f"Warning: Missing column(s): {target_col} or {estimated_col} in merged data. Defaulting scaling factor to 1 for {year}."
                )
                merged_ddf[year] = 1
            else:
                merged_ddf[year] = merged_ddf[target_col] / merged_ddf[estimated_col]

        # Fill NaNs (e.g., from division by zero) with 1
        merged_ddf[build_out_columns] = merged_ddf[build_out_columns].fillna(1)

        # Select final columns
        final_cols = [zone_id] + dims + build_out_columns
        result_ddf = merged_ddf[final_cols]

        return result_ddf.compute()

    def apply_scaling_factor(
        self,
        segmented_df: pd.DataFrame,
        scaler_df: pd.DataFrame,
        build_out_columns: list[str],
        dimension_columns: list[str],
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
            suffixes=("", "_scaler"),
        )

        # Apply scaling for each year
        for year in build_out_columns:
            merged[year] = merged[year] * merged[f"{year}_scaler"]

        # Drop the scaler columns (optional)
        scaler_cols = [f"{year}_scaler" for year in build_out_columns]
        merged = merged.drop(columns=scaler_cols)

        return merged

    # def apply_scaling_factor_dask(
    #     segmented_ddf: dd.DataFrame,
    #     scaler_ddf: dd.DataFrame,
    #     build_out_columns: list[str],
    #     dimension_columns: list[str],
    #     zone_col: str = "zone_id",
    # ) -> dd.DataFrame:
    #     """
    #     Apply scaling factors to a wide-format Dask DataFrame over multiple years.
    #     """
    #     scaled_frames = []

    #     for year in build_out_columns:
    #         # Merge on the dimension and zone columns
    #         merged = segmented_ddf[[zone_col] + dimension_columns + [year]].merge(
    #             scaler_ddf[[zone_col] + dimension_columns + [year]],
    #             on=[zone_col] + dimension_columns,
    #             suffixes=("", "_scaler"),
    #         )

    #         # Scale values
    #         merged[year] = merged[year] * merged[f"{year}_scaler"]

    #         # Drop the scaler column
    #         merged = merged.drop(columns=[f"{year}_scaler"])

    #         scaled_frames.append(merged)

    #     # Combine scaled yearly frames column-wise
    #     from functools import reduce

    #     scaled_df = reduce(
    #         lambda left, right: left.merge(right, on=[zone_col] + dimension_columns),
    #         scaled_frames,
    #     )

    #     return scaled_df

    def apply_scaling_factor_dask(
        self,
        df: pd.DataFrame,
        scaler_df: pd.DataFrame,
        year_columns: List[str],
        dimension_cols: Optional[List[str]] = None,
        npartitions: int = 8,
    ) -> pd.DataFrame:
        """
        Applies scaling factors to a wide-format DataFrame using Dask for memory efficiency.

        Parameters:
            df (pd.DataFrame): Main DataFrame with values to scale.
            scaler_df (pd.DataFrame): DataFrame with scaling factors (must align on zone_id + dimensions).
            year_columns (List[str]): List of year columns to scale.
            dimension_cols (Optional[List[str]]): List of dimension columns to join on.
            npartitions : int, default=4
                Number of partitions to divide the Dask DataFrame into. Higher values may improve parallelism
                but increase overhead. Lower values reduce overhead but may use more memory per partition.

        Returns:
            pd.DataFrame: Scaled DataFrame in wide format (back as Pandas).
        """
        zone_id = self.zone_info["group_by_column"]
        # Convert both to Dask DataFrames
        ddf = dd.from_pandas(df, npartitions=npartitions)
        ddf_scaler = dd.from_pandas(scaler_df, npartitions=npartitions)

        # Determine merge columns
        merge_cols = [col for col in df.columns if col not in year_columns]
        if dimension_cols:
            merge_cols = list(
                set(merge_cols).intersection(set([zone_id] + dimension_cols))
            )

        # Merge Dask DataFrames
        ddf_merged = ddf.merge(ddf_scaler, on=merge_cols, suffixes=("", "_scaler"))

        # Apply scaling to each year column
        for year in year_columns:
            ddf_merged[year] = ddf_merged[year] * ddf_merged[f"{year}_scaler"]

        # Drop scaler columns
        cols_to_drop = [f"{year}_scaler" for year in year_columns]
        ddf_result = ddf_merged.drop(columns=cols_to_drop)

        # Convert back to Pandas
        return ddf_result  # .compute()

    # def apply_scaling_factor_single_year(
    #     self,
    #     segmented_dict: Dict[str, pd.DataFrame],
    #     scaler_dict: Dict[str, pd.DataFrame],
    #     zone_col: str,
    #     dimension_columns: List[str],
    # ) -> Dict[str, pd.DataFrame]:
    #     """
    #     Apply scaling factors for each year to a dictionary of segmented DataFrames.

    #     Parameters
    #     ----------
    #     segmented_dict : Dict[str, pd.DataFrame]
    #         Dictionary of year-specific segmented DataFrames with 'value'.
    #     scaler_dict : Dict[str, pd.DataFrame]
    #         Dictionary of year-specific scaler DataFrames with 'value'.
    #     zone_col : str
    #         Column name for zone ID.
    #     dimension_columns : list[str]
    #         Dimension columns to match on.

    #     Returns
    #     -------
    #     Dict[str, pd.DataFrame]
    #         Dictionary of scaled DataFrames per year with updated 'value'.
    #     """
    #     scaled_dict = {}

    #     for year in segmented_dict:
    #         segmented_df = segmented_dict[year]
    #         scaler_df = scaler_dict.get(year)

    #         if scaler_df is None:
    #             raise ValueError(f"No scaler data available for year {year}")

    #         merged = segmented_df.merge(
    #             scaler_df,
    #             on=[zone_col] + dimension_columns,
    #             how="left",
    #             suffixes=("", "_scaler"),
    #         )

    #         merged["value"] = merged["value"] * merged["value_scaler"]
    #         scaled_dict[year] = merged.drop(columns=["value_scaler"])

    #     return scaled_dict

    def apply_scaling_factor_single_year_df(
        self,
        segmented_df: pd.DataFrame,
        scaler_df: pd.DataFrame,
        zone_col: str,
        dimension_columns: list[str],
    ) -> pd.DataFrame:
        """
        Apply scaling factors to a segmented single-year DataFrame.

        Parameters
        ----------
        segmented_df : pd.DataFrame
            Year-specific segmented DataFrame with 'value'.
        scaler_df : pd.DataFrame
            Year-specific scaler DataFrame with 'value'.
        zone_col : str
            Column name for zone ID.
        dimension_columns : list[str]
            Dimension columns to match on.

        Returns
        -------
        pd.DataFrame
            Scaled DataFrame with updated 'value'.
        """
        merged = segmented_df.merge(
            scaler_df,
            on=[zone_col] + dimension_columns,
            how="left",
            suffixes=("", "_scaler"),
        )
        merged["value"] = merged["value"] * merged["value_scaler"]
        return merged.drop(columns=["value_scaler"])

    # def export_singleyear_file(
    #     self,
    #     df: Union[pd.DataFrame, dd.DataFrame],
    #     dimension_cols: Optional[List[str]],
    #     year_cols: List[str],
    #     output_dir: Path,
    #     file_name: str,
    # ) -> None:
    #     output_dir.mkdir(parents=True, exist_ok=True)

    #     zone_id = self.zone_info["group_by_column"]

    #     for year in year_cols:
    #         columns_to_export = [zone_id] + (dimension_cols or []) + [year]
    #         df_year = df[columns_to_export].rename(columns={year: "value"})

    #         # If it's a Dask DataFrame, compute it
    #         if isinstance(df_year, dd.DataFrame):
    #             df_year = df_year.compute()

    #         output_path = output_dir / f"{file_name}_{year}.csv.bz2"
    #         utilities.write_to_csv(output_path, df_year)

    # def export_expanded_singleyear_file(
    #     self,
    #     dimension_cols: list[str],
    #     year_cols: List[str],
    #     input_dir: Path,
    #     output_dir: Path,
    #     zone_prefix: str = "fy_zone_lsgrth_hh",
    #     ratio_prefix: str = "fy_zone_hh_type_ratio",
    # ) -> None:
    #     """
    #     For each year, load zone and ratio data, apply `expand_by_dimension`, and export the expanded result.

    #     Parameters
    #     ----------
    #     build_out_columns : list of str
    #         List of future year strings (e.g., ['2025', '2026', ..., '2061']).
    #     input_dir : Path
    #         Directory where input CSVs (zone + ratio) are stored.
    #     output_dir : Path
    #         Directory to write the expanded CSVs.
    #     zone_prefix : str
    #         Prefix for zone file, e.g. "fy_zone_lsgrth_hh".
    #     ratio_prefix : str
    #         Prefix for ratio file, e.g. "fy_zone_hh_type_ratio".
    #     dimension_cols : list of str
    #         List of dimension column names (e.g. ['hh_type'] or similar).
    #     """
    #     output_dir.mkdir(parents=True, exist_ok=True)
    #     zone_id = self.zone_info["group_by_column"]
    #     # Check if zone_data contains 'sic_2d' (i.e., jobs data) and adjust id_vars

    #     for year in year_cols:
    #         LOG.info(f"Expanding data for year {year}...")

    #         zone_file = input_dir / f"{zone_prefix}_{year}.csv.bz2"
    #         ratio_file = input_dir / f"{ratio_prefix}_{year}.csv.bz2"

    #         zone_data = pd.read_csv(zone_file)
    #         zone_ratio = pd.read_csv(ratio_file)

    #         if "sic_2d" in zone_data.columns:
    #             id_list = [zone_id, "sic_2d"]
    #         else:
    #             id_list = [zone_id]  # Add 'sic_2d' for jobs data

    #         # Merge on zone_id (+ sic_2d if present)
    #         merged = zone_ratio.merge(
    #             zone_data[id_list + ["value"]],
    #             on=id_list,
    #             how="left",
    #             suffixes=("_ratio", "_total"),
    #         )

    #         # Calculate disaggregated value
    #         merged["value"] = merged["value_total"] * merged["value_ratio"]
    #         merged = merged[id_list + dimension_cols + ["value"]]

    #         output_file = output_dir / f"{zone_prefix}_segmented_{year}.csv.bz2"
    #         utilities.write_to_csv(output_file, merged)

    def create_singleyear_df(
        self,
        df: Union[pd.DataFrame, dd.DataFrame],
        dimension_cols: Optional[List[str]],
        year_cols: List[str],
        output_dir: Path,
        file_name: str,
        export_csv: bool,
    ) -> dict[str, pd.DataFrame]:
        """
        Create a dictionary of pandas DataFrames, one for each year in year_cols.

        Parameters
        ----------
        df : pd.DataFrame or dd.DataFrame
            The DataFrame containing 'zone_id', optional dimension columns, and year columns.
        dimension_cols : list of str, optional
            Dimension columns to include alongside zone_id and year. If None, only 'zone_id' and year are used.
        year_cols : list of str
            The list of year columns to extract.
        output_dir : Path
            Directory to write the expanded CSVs.
        file_name : str
            Name prefix for the output files.
        export_csv : bool, default True
            Whether to export each year's DataFrame to a CSV file.
        Returns
        -------
        dict[str, pd.DataFrame]
            Dictionary where key is the year (str), and value is the corresponding pandas DataFrame.
        """
        zone_id = self.zone_info["group_by_column"]
        year_df_dict = {}

        # # Persist Dask DataFrame if applicable
        # if isinstance(df, dd.DataFrame):
        #     df = df.persist()

        for year in year_cols:
            columns_to_extract = [zone_id] + (dimension_cols or []) + [year]
            df_year = df.loc[:, columns_to_extract].rename(columns={year: "value"})

            # Convert Dask to Pandas if needed
            if isinstance(df_year, dd.DataFrame):
                df_year = df_year.compute()

            if export_csv:
                output_file = output_dir / f"{file_name}_{year}.csv.bz2"
                utilities.write_to_csv(output_file, df_year)

            year_df_dict[year] = df_year

        return year_df_dict

    def expand_dim_singleyear_df(
        self,
        zone_df: pd.DataFrame,
        ratio_df: pd.DataFrame,
        dimension_cols: list[str],
    ) -> pd.DataFrame:
        """
        Expand a single year's zone and ratio DataFrame and return the expanded DataFrame.
        """
        zone_id = self.zone_info["group_by_column"]

        id_list = [zone_id]
        if "sic_2d" in zone_df.columns:
            id_list.append("sic_2d")

        merged = ratio_df.merge(
            zone_df[id_list + ["value"]],
            on=id_list,
            how="left",
            suffixes=("_ratio", "_total"),
        )

        merged["value"] = merged["value_total"] * merged["value_ratio"]
        return merged[id_list + dimension_cols + ["value"]]

    def expand_all_singleyear(
        self,
        zone_df_dict: dict[str, pd.DataFrame],
        ratio_df_dict: dict[str, pd.DataFrame],
        dimension_cols: list[str],
        year_cols: list[str],
        output_dir: Path,
        zone_prefix: str,
        export_csv: bool,
    ) -> dict[str, pd.DataFrame]:
        """
        Loop through all years and apply `expand_dim_singleyear_df` to the corresponding data,
        then export to file and return a dictionary of expanded DataFrames.

        Parameters
        ----------
        zone_df_dict : dict[str, pd.DataFrame]
            Dictionary of zone-level data keyed by year.
        ratio_df_dict : dict[str, pd.DataFrame]
            Dictionary of ratio (dimension share) data keyed by year.
        dimension_cols : list[str]
            Columns used to expand the zone-level values into dimensions.
        year_cols : list[str]
            List of year strings to process.
        output_dir : Path
            Directory to write output CSV files.
        zone_prefix : str
            Prefix for naming output files.
        export_csv : bool, default True
            Whether to write each year's expanded DataFrame to a CSV file.
        Returns
        -------
        dict[str, pd.DataFrame]
            Dictionary with year as key and expanded DataFrame as value.
        """
        output_dir.mkdir(parents=True, exist_ok=True)

        expanded_dict = {}

        for year in year_cols:
            LOG.info(f"Expanding single-year dimension data for {year}...")

            zone_df = zone_df_dict[year]
            ratio_df = ratio_df_dict[year]

            expanded_df = self.expand_dim_singleyear_df(
                zone_df=zone_df,
                ratio_df=ratio_df,
                dimension_cols=dimension_cols,
            )

            if export_csv:
                output_file = output_dir / f"{zone_prefix}_segmented_{year}.csv.bz2"
                utilities.write_to_csv(output_file, expanded_df)

            expanded_dict[year] = expanded_df

        return expanded_dict


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
        df = pd.DataFrame(
            {
                f"{label_prefix}_Zonal": df_predisagg[self.build_out_columns].sum(),
                f"{label_prefix}_Segmented": df_postdisagg[
                    self.build_out_columns
                ].sum(),
            }
        )

        df[f"{label_prefix}_AbsDiff"] = (
            df[f"{label_prefix}_Segmented"] - df[f"{label_prefix}_Zonal"]
        )
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


class PrepTripends:

    # CATEGORIES = ["", "_large"]
    CATEGORIES = [""]

    def __init__(self, config: inputs.DLitConfig) -> None:
        """Initialize the class with dynamic YEARS based on the base year."""
        self.base_year_int = int(config.large_sites.base_year)
        # self.end_year_int = int(
        #     config.large_sites.end_year
        # )  # Ensure config has base_year_int
        # # self.years = range(self.base_year_int + 1, self.end_year_int + 1)
        # self.years = range(self.base_year_int, self.base_year_int + 1)
        self.years = [str(year) for year in config.split.future_years]
        self.data_config = {
            "hh": {
                "index_cols": [
                    "accom_h",
                    "ns_sec",
                    "adults",
                    "car_availability",
                    "children",
                ],
                "segments": [
                    "accom_h",
                    "ns_sec",
                    "adults",
                    "car_availability",
                    "children",
                ],
            },
            "pop": {
                "index_cols": ["tt"],
                "segments": ["gender_3", "aws", "soc", "ns_sec", "hh_type"],
            },
            "emp": {
                "index_cols": ["sic_2_digit", "soc"],
                "rename_cols": {"sic_2d": "sic_2_digit"},
                "segments": ["sic_2_digit", "soc"],
            },
        }

        self.cols_to_merge = ["gender_3", "aws", "ns_sec", "soc", "hh_type"]
        self.rename_mapping = {
            "gender": "gender_3",
            "aws": "aws",
            "ns": "ns_sec",
            "soc": "soc",
            "hh_type": "hh_type",
        }
        # Initialize the BaseZoneHandler for geo_boundary and other zone info
        self.base_zone_handler = ls.BaseZoneHandler(config)
        self.zone_info = self.base_zone_handler.zone_info
        self.zone_id = self.zone_info["group_by_column"]

    def merge_and_rename(
        self, data: pd.DataFrame, lookup: pd.DataFrame
    ) -> pd.DataFrame:
        """Merge and rename columns for tt_pop data."""

        return data.merge(lookup, on="tt", how="left").rename(
            columns=self.rename_mapping
        )

    def prepare_segmentation_input(self, data_type: str) -> cb.SegmentationInput:
        """Prepare segmentation input based on data type."""

        if data_type not in self.data_config:

            raise ValueError(f"Unknown data type for segmentation: {data_type}")

        return cb.SegmentationInput(
            enum_segments=self.data_config[data_type]["segments"],
            naming_order=self.data_config[data_type]["segments"],
        )

    def export_csv(self, data: pd.DataFrame, output_path: Path):
        """export csv file."""
        utilities.write_to_csv(output_path, data)

    def perform_segmentation_and_zoning(
        self, data: pd.DataFrame, data_type: str, output_path: Path
    ):
        """Perform segmentation and zoning, then save the result."""
        # utilities.write_to_csv(output_path, data)

        seg = cb.Segmentation(self.prepare_segmentation_input(data_type))
        zoning = cb.ZoningSystem.get_zoning("lsoa_2021")

        dvec = DVector(import_data=data, zoning_system=zoning, segmentation=seg)
        if data_type == "pop":
            dvec = dvec.add_segments(["adult_nssec"])

        if data_type == "hh":
            dvec = dvec.add_segments(["adult_nssec", "total"])

        if data_type == "emp":
            dvec = dvec.add_segments(["sic_1_digit"])

        dvec.save(output_path)
        # df = dvec.data
        # df.to_csv(output_path)

    def run_all_segmentation(
        self,
        data_dicts: Dict[str, Dict[int, pd.DataFrame]],
        output_dir: Path,
        lookup: pd.DataFrame = None,
        large_suffix: str = "_large",
    ):
        # Define output folders for each data type
        output_folders = {
            "emp": output_dir / "dvec_emp",
            "pop": output_dir / "dvec_pop",
            "hh": output_dir / "dvec_hh",
        }

        # Create output folders if they don't exist
        for folder in output_folders.values():
            folder.mkdir(exist_ok=True)

        for data_type_base in ["hh", "pop", "emp"]:
            for is_large in [False, True]:
                suffix = large_suffix if is_large else ""
                full_key = f"{data_type_base}{suffix}"
                data_by_year = data_dicts.get(full_key, {})

                for year, df in data_by_year.items():
                    if year not in self.years:
                        continue  # Skip years outside configured range

                    print(f"Processing {full_key} for year {year}")

                    # Rename columns if needed
                    config = self.data_config[data_type_base]
                    if "rename_cols" in config:
                        df = df.rename(columns=config["rename_cols"])

                    if data_type_base == "pop" and lookup is not None:
                        df = self.merge_and_rename(df, lookup)

                    # Export csv file
                    # self.export_csv(
                    #     df,
                    #     output_folders[data_type_base]
                    #     / f"{data_type_base}{suffix}_{year}.csv.bz2",
                    # )

                    pivoted = df.pivot_table(
                        index=config[
                            "index_cols" if data_type_base != "pop" else "segments"
                        ],
                        columns=self.zone_id,
                        values="value",
                        fill_value=0,
                    )

                    # Define output path
                    output_file = (
                        output_folders[data_type_base]
                        / f"{data_type_base}{suffix}_{year}.dvec"
                    )
                    self.perform_segmentation_and_zoning(
                        pivoted, data_type_base, output_file
                    )
                    print(f"✅ Saved: {output_file}")


def run(input_data: global_classes.DlogZoneData, config: inputs.DLitConfig):
    """runs process for converting DLOG to zone build out profiles

    disaggregaes D-log estimated zonal household, population and job into required dimensions for future year

    Parameters
    ----------
    input_data : global_classes.DlogZoneData
        fy zonal data to be further disaggregated into required dimensions
    config : inputs.DLitConfig
        config file
    """
    if config.split is None:
        raise ValueError("cannot run split without any split parameters")

    LOG.info("Initialising Split Module")
    config.output_folder.mkdir(exist_ok=True)

    LOG.info("Loading key inputs")
    tfn_tt = pd.read_csv(config.split.tfn_tt)

    LOG.info("Loading base year data")

    # Get base year zonal total pop, hh and jobs with full dimensions
    lsoa_hh_column_names = [
        "accom_h",
        "ns_sec",
        "adults",
        "car_availability",
        "children",
        "lsoa2021_id",
        "household",
    ]
    hh_type_columns = [
        "accom_h",
        "ns_sec",
        "adults",
        "car_availability",  # no car, 1 car, 2+ cars
        "children",
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
        "car_ownership",  # without and with car
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
        index_col=None,
    )

    by_lsoa_pop_tt = pd.read_csv(
        config.land_use.lsoa_traveller_type_path,
        names=lsoa_pop_by_tt_column_names,
        header=0,
        index_col=None,
    )
    by_lsoa_pop_tt = by_lsoa_pop_tt.merge(tfn_tt, on="tt", how="left")[
        ["lsoa2021_id", "population"] + pop_type_columns[:-1]
    ]

    # Define the mapping
    without_car = [1, 3, 6]
    by_lsoa_pop_tt["car_ownership"] = by_lsoa_pop_tt["hh_type"].apply(
        lambda x: 1 if x in without_car else 2
    )
    by_lsoa_pop_tt = by_lsoa_pop_tt[["lsoa2021_id"] + pop_type_columns + ["population"]]

    by_lsoa_jobs_sic_soc = pd.read_csv(
        config.land_use.lsoa_jobs_path,
        names=lsoa_jobs_column_names,
        header=0,
        index_col=None,
    )

    LOG.info("Loading key parameter and paths")
    model_zone = config.large_sites.geo_boundary.value
    model_zone_enum = inputs.GeoBoundary(model_zone.lower())
    base_year = config.large_sites.base_year
    base_year_int = int(base_year)
    end_year = config.large_sites.end_year
    end_year_int = int(end_year)
    key_output_path = config.output_folder / f"M4_split"
    key_output_path.mkdir(exist_ok=True)

    build_out_columns = [
        str(year) for year in range(base_year_int + 1, end_year_int + 1)
    ]
    future_years = config.split.future_years
    future_year_columns = [str(year) for year in future_years]
    LOG.info("Instantiating the key classes")
    zt = ls.ZoneTranslator(config)
    cal_ratios = Ratios(config)
    zone_info = cal_ratios.zone_info_map.get(cal_ratios.geo_boundary, {})
    if not zone_info:
        LOG.error(f"No zone information found for zone {cal_ratios.geo_boundary}")
        return

    zone_id = zone_info.get("group_by_column")
    # zone_to_lad_path = zone_info.get("zone_to_lad_path")
    # lad_id = zone_info.get("lad_id_col")
    # zone_to_lad_prop = zone_info.get("zone_to_lad_prop")

    LOG.info(
        "Processing base year data to get profiles for household, population and jobs"
    )
    # Process base year data for household, population and jobs with required dimensions
    # Translate LSOA to pre-defined model zone
    if model_zone_enum == inputs.GeoBoundary.LSOA:
        # No translation needed; use LSOA data directly
        by_zone_hh_type_data = by_lsoa_hh_type
        by_zone_pop_tt_data = by_lsoa_pop_tt
        by_zone_job_sic_soc_data = by_lsoa_jobs_sic_soc
    else:
        # Perform translation for non-LSOA model zones
        by_zone_hh_type_data = zt.translate_data(
            data=by_lsoa_hh_type,
            category="residential",
            columns_to_process=["household"],
            dimension_columns=hh_type_columns,
            merge_translation_data=True,
            aggregate_by_zone=True,
        )
        by_zone_pop_tt_data = zt.translate_data(
            data=by_lsoa_pop_tt,
            category="residential",
            columns_to_process=["population"],
            dimension_columns=pop_type_columns,
            merge_translation_data=True,
            aggregate_by_zone=True,
        )
        by_zone_job_sic_soc_data = zt.translate_data(
            data=by_lsoa_jobs_sic_soc,
            category="employment",
            columns_to_process=["jobs"],
            dimension_columns=job_type_columns,
            merge_translation_data=True,
            aggregate_by_zone=True,
        )

    # Calculate ratios across dimensions for base year household, population and jobs
    # Calculating zonal ratio by each household type
    by_zone_hh_type_ratio = cal_ratios.zone_hh_type_ratios(
        by_zone_hh_type_data,
        hh_type_columns,
    )
    # Calculating zonal ratio by household car availability
    by_zone_hh_car_ratio = (
        cal_ratios.zone_hh_type_ratios(
            by_zone_hh_type_data,
            ["car_availability"],
        )
        .drop(columns=["household"])
        .rename(columns={"ratios": base_year})
    )
    # Calculating zonal ratio by population traveller type
    by_zone_pop_tt_ratio = cal_ratios.zone_traveller_type_ratios(
        by_zone_pop_tt_data,
        pop_type_columns,
    )
    # Calculating zonal ratio by population's household car ownership
    by_zone_pop_car_ratio = (
        cal_ratios.zone_traveller_type_ratios(
            by_zone_pop_tt_data,
            ["car_ownership"],
        )
        .drop(columns=["population"])
        .rename(columns={"ratios": base_year})
    )
    # Calculate default soc ratios for jobs (for whole gb)
    by_gb_job_soc_ratio = cal_ratios.gb_job_type_ratios(
        by_zone_job_sic_soc_data,
        ["soc"],
    )
    by_gb_job_soc_ratio = by_gb_job_soc_ratio.rename(
        columns={"ratios": "default_ratio"}
    )
    # Calculate zonal soc over sic ratio
    by_zone_job_soc_over_sic_ratio = cal_ratios.zone_soc_over_sic_ratios(
        by_zone_job_sic_soc_data,
        job_type_columns,
        "jobs",
        "soc_ratios",
    )
    by_zone_job_soc_over_sic_ratio = by_zone_job_soc_over_sic_ratio.merge(
        by_gb_job_soc_ratio,
        on="soc",
        how="left",
    )
    # infill nan with default ratio
    by_zone_job_soc_over_sic_ratio["soc_ratios"] = by_zone_job_soc_over_sic_ratio[
        "soc_ratios"
    ].fillna(by_zone_job_soc_over_sic_ratio["default_ratio"])
    by_zone_job_soc_over_sic_ratio = by_zone_job_soc_over_sic_ratio.drop(
        columns=["default_ratio"]
    )
    print("sum of soc_ratios", by_zone_job_soc_over_sic_ratio["soc_ratios"].sum())
    # further adjust ratios to make sure the sum is 1
    by_zone_job_soc_over_sic_ratio_aj = cal_ratios.zone_soc_over_sic_ratios(
        by_zone_job_soc_over_sic_ratio,
        job_type_columns,
        "soc_ratios",
        "ratios",
    )
    print("sum of ratios", by_zone_job_soc_over_sic_ratio_aj["ratios"].sum())
    # Define filenames and corresponding DataFrames
    by_zone_ratios = [
        (f"by_zone_hh_car_ratio_{model_zone}.csv", by_zone_hh_car_ratio),
        (f"by_zone_pop_car_ratio_{model_zone}.csv", by_zone_pop_car_ratio),
    ]
    # Save each DataFrame to a CSV file
    for filename, df in by_zone_ratios:
        utilities.write_to_csv(key_output_path / filename, df)

    LOG.info("Extracting fy trend of ntem car profile change for pop and household")
    trend_of_pop_car = cal_ratios.ntem_pop_car_ratios(base_year, build_out_columns)

    trend_of_hh_car = cal_ratios.ntem_hh_car_ratios(base_year, build_out_columns)

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

    LOG.info(
        "Disaggregating fy zonal data into required dimensions for household, population and jobs based on D-log data"
    )
    zone_hh_fy = input_data.fy_zone_tot_hh
    zone_pop_fy = input_data.fy_zone_tot_pop
    zone_job_sic_fy = input_data.fy_zone_tot_emp_sic
    negatives_df_before = zone_job_sic_fy[
        zone_job_sic_fy[build_out_columns].lt(0).any(axis=1)
    ]
    if not negatives_df_before.empty:
        LOG.info(
            "Negative values found in zone_job_sic_fy before clipping: %s",
            negatives_df_before.shape[0],
        )

    zone_job_sic_fy[build_out_columns] = zone_job_sic_fy[build_out_columns].clip(
        lower=0
    )
    negatives_df_after = zone_job_sic_fy[
        zone_job_sic_fy[build_out_columns].lt(0).any(axis=1)
    ]
    if not negatives_df_after.empty:
        LOG.info(
            "Negative values found in zone_job_sic_fy after clipping: %s",
            negatives_df_after.shape[0],
        )

    zone_hh_car_ratio_fy = zone_hh_car_ratio[
        [zone_id, "car_availability"] + build_out_columns
    ]
    zone_pop_car_ratio_fy = zone_pop_car_ratio[
        [zone_id, "car_ownership"] + build_out_columns
    ]

    # Get target zonal values for household and population segmented by car ownership for future year
    zone_hh_car_target = cal_ratios.expand_by_dimension(
        zone_hh_fy,
        zone_hh_car_ratio_fy,
        build_out_columns,
        ["car_availability"],
    )

    zone_pop_car_target = cal_ratios.expand_by_dimension(
        zone_pop_fy,
        zone_pop_car_ratio_fy,
        build_out_columns,
        ["car_ownership"],
    )

    # Segment future year zonal household, population and job data into full dimensions using base year ratios
    zone_hh_segmented = cal_ratios.apply_ratio(
        zone_hh_fy,
        build_out_columns,
        hh_type_columns,
        by_zone_hh_type_ratio,
    )
    zone_pop_segmented = cal_ratios.apply_ratio(
        zone_pop_fy,
        build_out_columns,
        pop_type_columns,
        by_zone_pop_tt_ratio,
    )
    zone_job_sic_segmented = cal_ratios.apply_soc_over_sic_ratio(
        zone_job_sic_fy,
        build_out_columns,
        job_type_columns,
        by_zone_job_soc_over_sic_ratio_aj,
    )

    LOG.info(
        "Adjusting fy zonal data initially segmented using base year profile further to be compliant with desired car profile for household and population"
    )
    # Aggregate segmented zonal household and population data by car ownership (without and with car) or by car availability (no car, 1 car, 2+ cars)
    zone_hh_car_estimated = cal_ratios.zone_vals_by_type_fy(
        zone_hh_segmented,
        ["car_availability"],
        build_out_columns,
    )
    zone_pop_car_estimated = cal_ratios.zone_vals_by_type_fy(
        zone_pop_segmented,
        ["car_ownership"],
        build_out_columns,
    )

    # Calculate scalling factor to adjust estimated car profile for all future years
    # for household and population
    zone_hh_car_scaler = cal_ratios.scaling_factors_dask(
        zone_hh_car_target,
        zone_hh_car_estimated,
        build_out_columns,
        ["car_availability"],
        10,
    )
    zone_pop_car_scaler = cal_ratios.scaling_factors_dask(
        zone_pop_car_target,
        zone_pop_car_estimated,
        build_out_columns,
        ["car_ownership"],
        10,
    )

    # Apply scaling factor to segmented zonal household and population future year data for selected future years
    zone_hh_segmented_scaled = cal_ratios.apply_scaling_factor_dask(
        zone_hh_segmented,
        zone_hh_car_scaler,
        future_year_columns,
        ["car_availability"],
        10,
    )
    zone_pop_segmented_scaled = cal_ratios.apply_scaling_factor_dask(
        zone_pop_segmented,
        zone_pop_car_scaler,
        future_year_columns,
        ["car_ownership"],
        20,
    )
    zone_pop_segmented_scaled = zone_pop_segmented_scaled[
        [zone_id, "tt"] + future_year_columns
    ]

    # Creating dict to contain yearly dataframes for further calculation on hh and pop data
    yearly_output_folder = key_output_path / f"yearly_zonal_data"
    yearly_output_folder.mkdir(exist_ok=True)

    fy_zone_hh_seged_dict = cal_ratios.create_singleyear_df(
        df=zone_hh_segmented_scaled,
        dimension_cols=hh_type_columns,
        year_cols=future_year_columns,
        output_dir=yearly_output_folder,
        file_name="fy_zone_hh_segmented",
        export_csv=False,
    )
    fy_zone_pop_seged_dict = cal_ratios.create_singleyear_df(
        df=zone_pop_segmented_scaled,
        dimension_cols=["tt"],
        year_cols=future_year_columns,
        output_dir=yearly_output_folder,
        file_name="fy_zone_pop_segmented",
        export_csv=False,
    )

    LOG.info(
        "Calculating zonal ratios for segmented zonal data for household, population and jobs for future years"
    )
    fy_zone_hh_type_ratio_dict = {}
    fy_zone_traveller_type_ratio_dict = {}

    for year in future_year_columns:
        # Create a dictionary for each year
        fy_zone_hh_type_ratio_dict[year] = cal_ratios.zone_ratios_by_type_single_year(
            data=fy_zone_hh_seged_dict[year],
            dimension_columns=hh_type_columns,
            output_dir=yearly_output_folder,
            file_name=f"fy_zone_hh_type_ratio_{year}",
            export_csv=False,
        )
        # Check that the sum of ratios equals the number of unique zones
        hh_ratio_sum = fy_zone_hh_type_ratio_dict[year]["value"].sum()
        hh_zone_count = fy_zone_hh_seged_dict[year][zone_id].nunique()

        LOG.info(f"Sum of ratios: {hh_ratio_sum}, Number of zones: {hh_zone_count}")

        fy_zone_traveller_type_ratio_dict[year] = (
            cal_ratios.zone_ratios_by_type_single_year(
                data=fy_zone_pop_seged_dict[year],
                dimension_columns=["tt"],
                output_dir=yearly_output_folder,
                file_name=f"fy_zone_traveller_type_ratio_{year}",
                export_csv=False,
            )
        )
        # Check that the sum of ratios equals the number of unique zones
        tt_ratio_sum = fy_zone_traveller_type_ratio_dict[year]["value"].sum()
        tt_zone_count = fy_zone_pop_seged_dict[year][zone_id].nunique()
        LOG.info(f"Sum of ratios: {tt_ratio_sum}, Number of zones: {tt_zone_count}")

    # # Work out the ratios (profile) of dimensions based on scaled zonal data for household and population, and the segmetned jobs for future year
    # fy_zone_hh_type_ratio = cal_ratios.zone_ratios_by_type_fy_dask(
    #     zone_hh_segmented_scaled,
    #     hh_type_columns,
    #     future_year_columns,
    #     10,
    # )
    # # print("fy_zone_hh_type_ratio", fy_zone_hh_type_ratio.head(10))

    # fy_zone_traveller_type_ratio = cal_ratios.zone_ratios_by_type_fy_dask(
    #     zone_pop_segmented_scaled,
    #     ["tt"],
    #     future_year_columns,
    #     30,
    # )

    LOG.info(
        "Slicing yearly dataframe (optionally exporting compressed csv file) for household and pop before expanding zonal data derived from large sites to full dimensions"
    )

    # Creating dict to contain yearly dataframes for further calculation on hh and pop data
    # fy_zone_hh_type_ratio_dict = cal_ratios.create_singleyear_df(
    #     fy_zone_hh_type_ratio,
    #     hh_type_columns,
    #     future_year_columns,
    #     yearly_output_folder,
    #     "fy_zone_hh_type_ratio",
    #     False,
    # )
    # fy_zone_traveller_type_ratio_dict = cal_ratios.create_singleyear_df(
    #     fy_zone_traveller_type_ratio,
    #     ["tt"],
    #     future_year_columns,
    #     yearly_output_folder,
    #     "fy_zone_traveller_type_ratio",
    #     False,
    # )

    fy_zone_lsgrth_hh_dict = cal_ratios.create_singleyear_df(
        df=input_data.fy_zone_lsgrth_hh,
        dimension_cols=None,
        year_cols=future_year_columns,
        output_dir=yearly_output_folder,
        file_name="fy_zone_lsgrth_hh",
        export_csv=False,
    )
    fy_zone_lsgrth_pop_dict = cal_ratios.create_singleyear_df(
        df=input_data.fy_zone_lsgrth_pop,
        dimension_cols=None,
        year_cols=future_year_columns,
        output_dir=yearly_output_folder,
        file_name="fy_zone_lsgrth_pop",
        export_csv=False,
    )

    LOG.info(
        "Expanding zonal hh and pop data derived from large sites to full dimensions"
    )

    # Loading dfs from dict to expand fy ls grth data and creating dict of expanded dataframes

    fy_zone_lsgrth_hh_seged_dict = cal_ratios.expand_all_singleyear(
        fy_zone_lsgrth_hh_dict,
        fy_zone_hh_type_ratio_dict,
        hh_type_columns,
        future_year_columns,
        yearly_output_folder,
        "fy_zone_lsgrth_hh",
        False,
    )

    fy_zone_lsgrth_pop_seged_dict = cal_ratios.expand_all_singleyear(
        fy_zone_lsgrth_pop_dict,
        fy_zone_traveller_type_ratio_dict,
        ["tt"],
        future_year_columns,
        yearly_output_folder,
        "fy_zone_lsgrth_pop",
        False,
    )

    # Expand all years for lsgrth of job data
    zone_job_sic_largesites_fy_seg = cal_ratios.apply_soc_over_sic_ratio(
        input_data.fy_zone_lsgrth_emp_sic,
        build_out_columns,
        job_type_columns,
        by_zone_job_soc_over_sic_ratio_aj,
    )

    LOG.info(
        "Creating dictionaries of segmented future year zonal data for all key outputs"
    )
    # fy_zone_hh_seged_dict = cal_ratios.create_singleyear_df(
    #     df=zone_hh_segmented_scaled,
    #     dimension_cols=hh_type_columns,
    #     year_cols=future_year_columns,
    #     output_dir=yearly_output_folder,
    #     file_name="fy_zone_hh_segmented",
    #     export_csv=False,
    # )
    # fy_zone_pop_seged_dict = cal_ratios.create_singleyear_df(
    #     df=zone_pop_segmented_scaled,
    #     dimension_cols=["tt"],
    #     year_cols=future_year_columns,
    #     output_dir=yearly_output_folder,
    #     file_name="fy_zone_pop_segmented",
    #     export_csv=False,
    # )
    fy_zone_job_seged_dict = cal_ratios.create_singleyear_df(
        df=zone_job_sic_segmented,
        dimension_cols=job_type_columns,
        year_cols=future_year_columns,
        output_dir=yearly_output_folder,
        file_name="fy_zone_job_segmented",
        export_csv=False,
    )

    fy_zone_lsgrth_job_seged_dict = cal_ratios.create_singleyear_df(
        df=zone_job_sic_largesites_fy_seg,
        dimension_cols=job_type_columns,
        year_cols=future_year_columns,
        output_dir=yearly_output_folder,
        file_name="fy_zone_lsgrth_job_segmented",
        export_csv=False,
    )

    LOG.info(
        "Checking totals across future years before and after disaggregating zonal data into dimensions"
    )
    # Instantiate with your build-out year columns

    comparator = TotalsComparison(build_out_columns)

    # Add comparisons

    comparator.add_comparison(
        "Jobs", input_data.fy_zone_tot_emp_sic, zone_job_sic_segmented
    )

    comparator.add_comparison(
        "Jobs_ls_grth",
        input_data.fy_zone_lsgrth_emp_sic,
        zone_job_sic_largesites_fy_seg,
    )

    # Get summary
    summary_df = comparator.get_summary()

    # Export if needed
    summary_file = f"fy_totals_comparison_{model_zone}.csv"
    utilities.write_to_csv(key_output_path / summary_file, summary_df)

    LOG.info("Checking detailed zonal results")
    # zone_list = ["E01013306", "E01011232", "E01011233", "E01011235", "E01011237"]

    for year in future_year_columns:
        fy_zone_hh = fy_zone_hh_seged_dict[year].merge(
            fy_zone_lsgrth_hh_seged_dict[year],
            on=[zone_id] + hh_type_columns,
            how="left",
            suffixes=("_tot", "_lsgrth"),
        )

        fy_zone_hh["diff"] = fy_zone_hh["value_tot"] - fy_zone_hh["value_lsgrth"]
        neg_diff_hh = fy_zone_hh[fy_zone_hh["diff"] < 0]
        if not neg_diff_hh.empty:
            LOG.info(
                f"Negative difference in household totals for year {year}: {neg_diff_hh.shape[0]} zones"
            )
        else:
            LOG.info(f"No negative household difference identified for year {year}")

        # filtered_zone_hh = fy_zone_hh[fy_zone_hh[zone_id].isin(zone_list)]

        fy_zone_pop = fy_zone_pop_seged_dict[year].merge(
            fy_zone_lsgrth_pop_seged_dict[year],
            on=[zone_id] + ["tt"],
            how="left",
            suffixes=("_tot", "_lsgrth"),
        )
        fy_zone_pop["diff"] = fy_zone_pop["value_tot"] - fy_zone_pop["value_lsgrth"]
        neg_diff_pop = fy_zone_pop[fy_zone_pop["diff"] < 0]
        if not neg_diff_pop.empty:
            LOG.info(
                f"Negative difference in population totals for year {year}: {neg_diff_pop.shape[0]} zones"
            )
        else:
            LOG.info(f"No negative population difference identified for year {year}")

        # filtered_zone_pop = fy_zone_pop[
        #     fy_zone_pop[zone_id].isin(zone_list)
        # ]

        fy_zone_job = fy_zone_job_seged_dict[year].merge(
            fy_zone_lsgrth_job_seged_dict[year],
            on=[zone_id] + job_type_columns,
            how="left",
            suffixes=("_tot", "_lsgrth"),
        )
        fy_zone_job["diff"] = fy_zone_job["value_tot"] - fy_zone_job["value_lsgrth"]
        neg_diff_job = fy_zone_job[fy_zone_job["diff"] < 0]

        if not neg_diff_job.empty:
            LOG.info(
                f"Negative difference in job totals for year {year}: {neg_diff_job.shape[0]} zones"
            )
        else:
            LOG.info(f"No negative job difference identified for year {year}")

        # filtered_zone_job = fy_zone_job[
        #     fy_zone_job[zone_id].isin(zone_list)
        # ]

        # Save to CSV
        utilities.write_to_csv(
            key_output_path / f"neg_zone_hh_diff_{year}.csv",
            neg_diff_hh,
        )
        utilities.write_to_csv(
            key_output_path / f"neg_zone_pop_diff_{year}.csv",
            neg_diff_pop,
        )
        utilities.write_to_csv(
            key_output_path / f"neg_zone_job_diff_{year}.csv",
            neg_diff_job,
        )

    LOG.info("Exporting segmented zonal results on future year totals")

    write_large_files = False  # Set to False if you want to skip the big files
    zonal_outputs = []  # Always define it, even if empty
    if write_large_files:
        zonal_outputs = [
            (f"{model_zone}_zonal_fy_job_by_sic_soc.csv.bz2", zone_job_sic_segmented),
        ]

    for file_name, df in zonal_outputs:
        utilities.write_to_csv(key_output_path / file_name, df)

    LOG.info("Further transforming and processing land use data for tripend module")
    data_dicts = {
        "hh": fy_zone_hh_seged_dict,
        "pop": fy_zone_pop_seged_dict,
        "emp": fy_zone_job_seged_dict,
        "hh_large": fy_zone_lsgrth_hh_seged_dict,
        "pop_large": fy_zone_lsgrth_pop_seged_dict,
        "emp_large": fy_zone_lsgrth_job_seged_dict,
    }

    # Lookup table for 'tt_pop' mapping (if needed)
    tt_lookup = pd.read_csv(config.split.tfn_tt)

    prep_teinput = PrepTripends(config)

    prep_teinput.run_all_segmentation(
        data_dicts=data_dicts,
        output_dir=key_output_path,
        lookup=tt_lookup,
        large_suffix="_large",
    )

    LOG.info("Ending Development Pattern Module")


# if __name__ == "__main__":
