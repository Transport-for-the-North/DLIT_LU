import pandas as pd
import logging
from typing import Dict, Any
import numpy as np

# Local imports
from dlit_lu import inputs, utilities


LOG = logging.getLogger(__name__)


class ConstraintProcessor:
    """
    A class to process constraints for geographical aggregations.

    Attributes:
        sector (inputs.Sector): The sector type defined in the configuration.
        config (inputs.DLitConfig): The configuration settings for constraints.
        sector_info_map (Dict[inputs.Sector, Dict[str, Any]]): Mapping of sector types to their corresponding metadata.
        sector_info (Dict[str, Any]): The sector metadata for the selected sector.
    """

    # def __init__(self, config: inputs.DLitConfig) -> None:
    #     """
    #     Initializes the ConstraintProcessor with configuration settings.

    #     Args:
    #         config (inputs.DLitConfig): The configuration object containing constraint details.
    #     """
    #     self.sector: inputs.Sector = config.constraint.sector
    #     self.config: inputs.DLitConfig = config
    #     self.sector_info_map: Dict[inputs.Sector, Dict[str, Any]] = {
    #         inputs.Sector.REGION: {
    #             "lookup_path": self.config.constraint.lad_to_region_file,
    #             "zone_id": "lad2013_id",
    #             "base_year_column": self.config.dev_pattern.base_year,
    #             "sector_id": "ntem_region_id",
    #             "zone_to_sector_prop_col": "lad2013_to_ntem_region",
    #             "sector_name": self.config.constraint.region_name,
    #         }
    #     }

    #     self.sector_info: Dict[str, Any] = self.sector_info_map[self.sector]
    def __init__(self, config: inputs.DLitConfig) -> None:
        """
        Initializes the ConstraintProcessor with configuration settings.

        Args:
            config (inputs.DLitConfig): The configuration object containing constraint details.
        """
        self.config: inputs.DLitConfig = config
        self.sector: inputs.Sector = config.constraint.sector
        self.sector_info_map = inputs.SECTOR_INFO_MAP
        self.base_year_column = config.dev_pattern.base_year

        # Validate sector
        if self.sector not in self.sector_info_map:
            raise ValueError(f"Unsupported sector: {self.sector}")

        # Retrieve sector-specific metadata from predefined SECTOR_INFO_MAP
        self.sector_info: Dict[str, Any] = self.sector_info_map[self.sector]

        # Override lookup path dynamically from config
        self.sector_info["lookup_path"] = self.config.constraint.lad_to_region_file
        # Sector name from configuration
        self.sector_info["sector_name"] = self.config.constraint.region_name

    def sector_agg(
        self,
        data: pd.DataFrame,
        base_year_column: str,
        future_year_columns: list[str],
    ) -> pd.DataFrame:
        """
        Aggregates LAD-level data to the regional level.

        Parameters
        ----------
        data (pd.DataFrame):
            The DataFrame containing LAD-level data.
        base_year_column (str):
            The column name representing the base year.
        future_year_columns (List[str]):
            A list of column names representing future years.

        Returns:
            pd.DataFrame: Aggregated data at the regional level.
        """
        base_year_column = self.base_year_column
        lookup_path = self.sector_info["lookup_path"]
        zone_id = self.sector_info["zone_id"]
        sector_id = self.sector_info["sector_id"]
        zone_to_sector_prop_col = self.sector_info["zone_to_sector_prop_col"]

        sector_data_annual = self.aggregate_to_sector(
            data,
            lookup_path,
            zone_id,
            base_year_column,
            future_year_columns,
            sector_id,
            zone_to_sector_prop_col,
        )

        return sector_data_annual

    def aggregate_to_sector(
        self,
        data: pd.DataFrame,
        lookup_path: str,
        zone_id: str,
        base_year_column: str,
        future_year_columns: list[str],
        sector_id: str,
        zone_to_sector_prop_col: str,
    ) -> pd.DataFrame:
        """
        Aggregate data from lower geographical  level to higher geographical level.
        e.g.
        This function aggregates data from the LAD level to the region level using a lookup table
        that maps LADs to regions. It adjusts the values based on a proportional column and ensures
        that the total values before and after aggregation remain consistent.

        Parameters
        ----------
        data : pd.DataFrame
            DataFrame containing LAD-level data with columns for the base year and future years.
        lookup_path : str
            Path to the CSV file containing the lookup table that maps LADs to regions.
        zone_id : str
            Column name in the data and lookup table representing the LAD identifier.
        base_year_column : str
            Column name in the data representing the base year.
        future_year_columns : list
            List of column names in the data representing future years.
        sector_id : str
            Column name in the lookup table representing the region identifier.
        zone_to_sector_prop_col : str
            Column name in the lookup table representing the proportion of each LAD that belongs to a region.

        Returns
        ------
        pd.DataFrame
            DataFrame containing aggregated region-level data with columns for the base year and future years.

        Raises
        ------
        ValueError
            If the total values before and after aggregation do not match, indicating a discrepancy in the aggregation process.
        """

        lookup_df = pd.read_csv(lookup_path)

        val_cols = [base_year_column] + future_year_columns
        totals_before = {col: data[col].sum() for col in val_cols}

        merged_df = data.merge(
            lookup_df[[zone_id, sector_id, zone_to_sector_prop_col]],
            on=zone_id,
            how="left",
        )

        for col in val_cols:
            merged_df[col] = merged_df[col] * merged_df[zone_to_sector_prop_col]

        sector_agg = merged_df.groupby(sector_id, as_index=False)[val_cols].sum()

        totals_after = {col: sector_agg[col].sum() for col in val_cols}

        for col in val_cols:
            if not np.all(np.isclose(totals_before[col], totals_after[col])):
                raise ValueError(
                    f"Total of '{col}' changed after aggregation: before={totals_before[col]}, after={totals_after[col]}"
                )

        return sector_agg

    def add_names_to_lu_data(
        self,
        data: pd.DataFrame,
        id_column: str,
        name_column: str,
        use_sector_name: bool = False,
        use_sector_cols: bool = False,
    ) -> pd.DataFrame:
        """
        Adds human-readable names to a dataset by merging with a name file.

        Parameters
        ----------
        data (pd.DataFrame):
            The dataset to which names should be added.
        id_column (str):
            Column representing the identifier to be matched.
        name_column (str):
            Column to store the added names.
        use_sector_name (bool, optional):
            Whether to use sector names instead of LAD names. Defaults to False.
        use_sector_cols (bool, optional):
            Whether to use sector-specific columns. Defaults to False.

        Returns:
            pd.DataFrame: The dataset with added names.
        """
        # Determine which name file to use
        if use_sector_name:
            name_file = self.sector_info["sector_name"]
        else:
            name_file = self.config.constraint.lad_name

        # Read the name file
        name_df = pd.read_csv(name_file)

        if use_sector_cols:
            # Merge
            data_with_name = data.merge(
                name_df, left_on=id_column, right_on="zone_id", how="left"
            )
            data_with_name = data_with_name.drop(columns=["zone_id"])
            data_with_name = data_with_name.rename(
                columns={id_column: "REGIONCD", "zone_name": f"REGIONNM"}
            )
            columns = list(data_with_name.columns)
            columns.remove("REGIONNM")
            columns.insert(1, "REGIONNM")
            data_with_name = data_with_name[columns]
        else:
            data_with_name = data.merge(
                name_df, left_on=id_column, right_on="zone_name", how="left"
            )
            data_with_name = data_with_name.drop(columns=["zone_id", "zone_name"])
            data_with_name = data_with_name.rename(
                columns={id_column: "LAD13CD", "descriptions": "LADNM"}
            )
            columns = list(data_with_name.columns)
            columns.remove("LADNM")
            columns.insert(1, "LADNM")
            data_with_name = data_with_name[columns]

            # Add sector names to LAD data
            if "LAD13CD" in data_with_name.columns:
                lad_to_region_df = pd.read_csv(
                    self.config.constraint.lad_to_region_file
                )
                region_name_df = pd.read_csv(self.config.constraint.region_name)
                data_with_name = data_with_name.merge(
                    lad_to_region_df[["lad2013_id", "ntem_region_id"]],
                    left_on="LAD13CD",
                    right_on="lad2013_id",
                    how="left",
                )
                data_with_name = data_with_name.merge(
                    region_name_df[["zone_id", "zone_name"]],
                    left_on="ntem_region_id",
                    right_on="zone_id",
                    how="left",
                )
                data_with_name = data_with_name.rename(
                    columns={"zone_name": "REGIONNM"}
                )
                data_with_name = data_with_name.drop(
                    columns=["lad2013_id", "ntem_region_id", "zone_id"]
                )
                cols = list(data_with_name.columns)
                cols.insert(cols.index("LADNM") + 1, cols.pop(cols.index("REGIONNM")))
                data_with_name = data_with_name[cols]

        LOG.info(f"Added {name_column} to dataset with shape {data_with_name.shape}")

        return data_with_name

    def ntem_data_extrapolator(
        self, data, base_year_column: str, build_out_columns: list[str]
    ) -> pd.DataFrame:
        """
        Extrapolate missing future year values and calculate ratios
        relative to a specified base year.

        Parameters:
        -----------
        data : pd.Dataframe
            Dataframe to be extrapolated.
        base_year_column : str
            The year used as the reference for ratio calculations (e.g., "2023").
        build_out_column : List[str]
            Full list of desired build-out years as strings (e.g., ["2024", ..., "2066"]).

        Returns:
        --------
        pd.DataFrame
            DataFrame with extrapolated values and ratio columns.
        """

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
            y_known = row[ntem_future_year_list].values.astype(float)

            # Fit linear trend
            coeffs = np.polyfit(x_known, y_known, deg=1)
            y_pred = np.polyval(coeffs, [int(y) for y in extrapolated_years])

            # Fill extrapolated values
            for i, year in enumerate(extrapolated_years):
                extended_data.at[row_idx, year] = y_pred[i]

        return extended_data


class GrowthCalculator:

    def calculate_growth(
        self,
        data: pd.DataFrame,
        base_year_column: str,
        build_out_columns: list[str],
        growth_type="absolute",
    ) -> pd.DataFrame:
        """
        General method to calculate absolute growth, growth ratio, or growth rate for each year.

        Parameters
        ----------
        data : pd.DataFrame
            DataFrame containing year columns.
        base_year_column: str
            The base year for calculations.
        build_out_columns : list
            List of future year columns to calculate the growth type.
        growth_type : str, optional (default='absolute')
            The type of growth calculation:
            - 'absolute': Absolute growth (difference from base year)
            - 'ratio': Growth ratio (future year / base year)
            - 'rate': Growth rate ((future year / base year) - 1)

        Returns
        -------
        pd.DataFrame
            DataFrame with growth calculations for each year,
            retaining the first three columns from the original data.

        Raises
        ------
        ValueError
            If `growth_type` is not one of the allowed values.
        """
        # Filter build_out_columns to only include years present in the data
        available_years = [year for year in build_out_columns if year in data.columns]

        valid_growth_types = {"absolute", "ratio", "rate"}
        if growth_type not in valid_growth_types:
            raise ValueError(
                f"Invalid growth_type '{growth_type}'. Choose from {valid_growth_types}."
            )

        base_year = base_year_column
        growth_funcs = {
            "absolute": lambda future_year: data[future_year] - data[base_year],
            "ratio": lambda future_year: data[future_year] / data[base_year],
            "rate": lambda future_year: (data[future_year] / data[base_year]) - 1,
        }

        growth = data.copy()
        # growth = growth.reset_index(drop=True)
        # Dynamically get the index columns count before the "source" column
        index_column_count = growth.columns.get_loc(base_year)
        # Determine the columns to be used for the index based on the index_column_count
        index_columns = list(growth.columns[:index_column_count])

        # # Set specified columns as index for both DataFrames
        # growth_indexed = growth.set_index(index_columns)
        for future_year in available_years:
            growth[future_year] = growth_funcs[growth_type](future_year)

        return growth[index_columns + available_years]

    def calculate_annual_growth_rate(self, data, year_columns):
        """
        Calculate the Compound Annual Growth Rate (CAGR) for each period between consecutive years.

        Parameters
        ----------
        data : pd.DataFrame
            DataFrame containing data with columns for each year to calculate the growth rate.
        year_columns : list
            List of column names representing the years for which to calculate the CAGR.

        Returns
        -------
        pd.DataFrame
            DataFrame with CAGR columns of each period between consecutive years.
        """
        available_years = sorted(
            [int(year) for year in year_columns if year in data.columns]
        )
        growth_rate = data.copy()
        cagr_columns = []

        for i in range(1, len(available_years)):
            start_year = available_years[i - 1]
            end_year = available_years[i]
            years = end_year - start_year

            def calculate_row_growth(row):
                start_value = row[str(start_year)]
                end_value = row[str(end_year)]
                return (
                    None
                    if start_value == 0
                    else ((end_value / start_value) ** (1 / years) - 1) * 100
                )

            cagr_column_name = str(end_year)
            growth_rate[cagr_column_name] = data.apply(calculate_row_growth, axis=1)
            cagr_columns.append(cagr_column_name)

        result = growth_rate.iloc[
            :,
            [0, 1, 2, 3, 4, 5]
            + [growth_rate.columns.get_loc(col) for col in cagr_columns],
        ]
        return result

    def target_growth(
        self,
        data_base: pd.DataFrame,
        data_source: pd.DataFrame,
        base_year_column: str,
        build_out_columns: list[str],
    ) -> pd.DataFrame:
        """
        Calculate target growth using original data.

        Parameters
        ----------
        data_base : pd.DataFrame
            DataFrame containing the base year total and dlog future year annual total.
        data_source : pd.DataFrame
            DataFrame containing the source annual total.
        base_year_column : str
            The base year for calculations.
        build_out_columns : list
            List of future year columns to calculate target growth.
        Returns
        -------
        pd.DataFrame
            DataFrame with target growth for each year.
        """
        # Dynamically get the index columns count before the "source" column
        index_column_count = data_base.columns.get_loc("source")
        # Determine the columns to be used for the index based on the index_column_count
        index_columns = list(data_base.columns[:index_column_count])

        # Ensure that the columns specified for the index match in both data_base and data_source
        if list(data_base[index_columns].columns) != list(
            data_source[index_columns].columns
        ):
            raise ValueError(
                f"Mismatch in column names specified for index between data_base and data_source."
            )

        # Extract base year data (keeping specified columns as index + base year values)
        base_year = base_year_column
        target_growth = data_base[index_columns + [base_year]].copy()

        # Calculate growth rates using _calculate_growth
        growth_result = self.calculate_growth(
            data_source, base_year_column, build_out_columns, growth_type="rate"
        ).fillna(0)

        # Merge growth rates with base year data
        target_growth = target_growth.merge(growth_result, on=index_columns, how="left")
        # Multiply base year values by (1 + growth rate) to get future values
        for year in build_out_columns:
            target_growth[year] = target_growth[base_year] * target_growth[year]
        # # Reset index so output matches original data format
        # target_growth = target_growth.reset_index()

        return target_growth[index_columns + [base_year] + build_out_columns]

    def target_yeartot(
        self,
        data_base: pd.DataFrame,
        data_source: pd.DataFrame,
        base_year_column: str,
        build_out_columns: list[str],
    ) -> pd.DataFrame:
        """
        Calculate target growth using original data.

        Parameters
        ----------
        data_base : pd.DataFrame
            DataFrame containing the base year data and dlog annual total.
        data_source : pd.DataFrame
            DataFrame containing the source annual total.
        base_year_column : str
            The base year for calculations.
        build_out_columns : list
            List of future year columns to calculate target growth.
        Returns
        -------
        pd.DataFrame
            DataFrame with target growth for each year.
        """
        # Dynamically get the index columns count before the "source" column
        index_column_count = data_base.columns.get_loc("source")
        # Determine the columns to be used for the index based on the index_column_count
        index_columns = list(data_base.columns[:index_column_count])

        # Ensure that the columns specified for the index match in both data_base and data_source
        if list(data_base[index_columns].columns) != list(
            data_source[index_columns].columns
        ):
            raise ValueError(
                f"Mismatch in column names specified for index between data_base and data_source."
            )

        # Extract base year data (keeping specified columns as index + base year values)
        base_year = base_year_column
        target_growth = data_base[index_columns + [base_year]].copy()

        # Calculate growth rates using _calculate_growth
        growth_result = self.calculate_growth(
            data_source,
            base_year_column,
            build_out_columns,
            growth_type="ratio",
        ).fillna(1)
        # Merge growth rates with base year data
        target_growth = target_growth.merge(growth_result, on=index_columns, how="left")
        # Multiply base year values by (1 + growth rate) to get future values
        for year in build_out_columns:
            target_growth[year] = target_growth[base_year] * target_growth[year]

        # # Reset index so output matches original data format
        # target = target.reset_index()

        return target_growth[index_columns + [base_year] + build_out_columns]

    def cumulative_yearly_totals(
        self,
        data: pd.DataFrame,
        base_year_column: str,
        build_out_columns: list[str],
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
        missing_cols = [col for col in build_out_columns if col not in data.columns]
        if missing_cols:
            raise ValueError(f"Missing columns in data: {missing_cols}")

        # Compute cumulative totals year by year
        for idx, year in enumerate(build_out_columns):
            prev_year = base_year_column if idx == 0 else build_out_columns[idx - 1]
            updated_data[year] = updated_data[prev_year] + data[year]

        return updated_data

    def yearly_totals_from_base(
        self,
        data: pd.DataFrame,
        base_year_column: str,
        build_out_columns: list[str],
    ) -> pd.DataFrame:
        """
        Calculate yearly totals from the base year onwards, where each year's total is
        the sum of the base year's total and the corresponding year's value.

        Parameters
        ----------
        data : pd.DataFrame
            DataFrame with a base year column and future year columns.
        base_year_column : str
            The base year column from which the totals are calculated.
        build_out_columns : list of str
            List of future year columns.

        Returns
        -------
        pd.DataFrame
            Updated DataFrame with new totals for each year.
        """
        updated_data = data.copy()

        # Ensure all specified columns exist in the data
        missing_cols = [col for col in build_out_columns if col not in data.columns]
        if missing_cols:
            raise ValueError(f"Missing columns in data: {missing_cols}")

        # Calculate totals by adding the base year column value to each of the years in build_out_columns
        for year in build_out_columns:
            updated_data[year] = updated_data[base_year_column] + data[year]

        # Identify all other columns (excluding base_year_column and build_out_columns)
        other_columns = [
            col
            for col in updated_data.columns
            if col != base_year_column and col not in build_out_columns
        ]

        # Rearrange columns to ensure base_year_column comes first, followed by build_out_columns, and other columns to the right
        updated_data = updated_data[
            other_columns + [base_year_column] + build_out_columns
        ]

        return updated_data


class ConstraintCalculation:
    """
    A class for performing constraint-based calculations related to growth gaps, scaling, and weighting of data.
    """

    @staticmethod
    def calculate_zone_weights(
        zone_data: pd.DataFrame,
        build_out_columns: list,
        zone_index_columns: list,
        sector_index_columns: list,
    ) -> pd.DataFrame:
        """
        Calculates the zone weights based on the growth gaps and aggregates them by sector/region.

        Parameters:
        ----------
        zone_data : pd.DataFrame
            DataFrame containing the zone gap growth data with sectors/regions and growth-related columns.

        build_out_columns : list
            List of column names representing growth-related data.

        zone_index_comlumns: list
            List of columns used for indexing and merging zone-level data (e.g., ['ZONE_ID']).

        sector_index_comlumns: list
            List of columns representing the sector or region (e.g., 'REGIONNM') and segementation p and m in the DataFrame.

        Returns:
        -------
        pd.DataFrame
            DataFrame containing the computed zone weights, with values aggregated by region.
        """

        agg_zone_data = zone_data.groupby(sector_index_columns)[build_out_columns].sum()
        agg_zone_data.columns = [f"{col}_agg" for col in build_out_columns]

        zone_weight = zone_data.merge(
            agg_zone_data, on=sector_index_columns, how="left"
        )

        # Compute weights with safeguard against division by zero
        for col in build_out_columns:
            agg_col = f"{col}_agg"
            zone_weight[col] = zone_weight.apply(
                lambda row: 1 if row[agg_col] == 0 else row[col] / row[agg_col], axis=1
            )

        return zone_weight[zone_index_columns + build_out_columns]

    @staticmethod
    def calculate_ratio(
        cap_ratio: float,
        target_data: pd.DataFrame,
        estimated_data: pd.DataFrame,
        build_out_columns: list,
        sector_index_columns: list,
    ) -> pd.DataFrame:
        """
        Computes the ratio of target growth values to estimated growth values for each sector,
        scaled by a given cap ratio.

        Parameters:
        -----------
        cap_ratio : float
            A scaling factor applied to the target values before computing the ratio.

        target_data : pd.DataFrame
            DataFrame containing the target growth values for different sectors.

        estimated_data : pd.DataFrame
            DataFrame containing the estimated growth values for the same sectors.

        build_out_columns : list
            List of column names representing growth-related data.

        sector_index_columns : list
            List of columns used for indexing and merging sector-level data.

        Returns:
        --------
        pd.DataFrame
            DataFrame containing the computed ratios of target to estimated growth
            for each sector, scaled by `cap_ratio`. If the estimated value is zero,
            the corresponding ratio is set to `1` to avoid division errors.
        """
        # Rename columns for clarity in merging
        target_data = target_data.rename(
            columns={col: f"{col}_tgt" for col in build_out_columns}
        )
        estimated_data = estimated_data.rename(
            columns={col: f"{col}_etmt" for col in build_out_columns}
        )

        # Merge on sector index columns
        merged_data = target_data.merge(
            estimated_data, on=sector_index_columns, how="left"
        )

        # Compute ratio while handling division by zero safely
        for col in build_out_columns:
            merged_data[col] = (
                merged_data[f"{col}_tgt"]
                * cap_ratio
                / merged_data[f"{col}_etmt"].replace(0, np.nan)
            ).fillna(1)

        return merged_data[sector_index_columns + build_out_columns]

    @staticmethod
    def calculate_gap(
        target_data: pd.DataFrame,
        estimated_data: pd.DataFrame,
        build_out_columns: list,
        sector_index_columns: list,
    ) -> pd.DataFrame:
        """
        Computes the difference (gap) between the target and estimated growth values for each sector.

        Parameters:
        -----------
        target_data : pd.DataFrame
            DataFrame containing the target growth values for different sectors.

        estimated_data : pd.DataFrame
            DataFrame containing the estimated growth values for the same sectors.

        build_out_columns : list
            List of column names representing growth-related data.

        sector_index_columns : list
            List of columns used for indexing and merging sector-level data.

        Returns:
        --------
        pd.DataFrame
            DataFrame containing the computed growth gaps for each sector.
        """
        target_data = target_data.rename(
            columns={col: f"{col}_tgt" for col in build_out_columns}
        )
        estimated_data = estimated_data.rename(
            columns={col: f"{col}_etmt" for col in build_out_columns}
        )

        merged_data = target_data.merge(
            estimated_data, on=sector_index_columns, how="left"
        )

        for col in build_out_columns:
            merged_data[col] = merged_data[f"{col}_tgt"] - merged_data[f"{col}_etmt"]

        return merged_data[sector_index_columns + build_out_columns]

    @staticmethod
    def calculate_sum(
        bg_data: pd.DataFrame,
        estimated_data: pd.DataFrame,
        build_out_columns: list,
        index_columns: list,
    ) -> pd.DataFrame:
        """
        Computes the sum of background and estimated growth values for each sector.

        Parameters:
        -----------
        bg_data : pd.DataFrame
            DataFrame containing the background growth values for different sectors.

        estimated_data : pd.DataFrame
            DataFrame containing the estimated growth values for the same sectors.

        build_out_columns : list
            List of column names representing growth-related data.

        sector_index_columns : list
            List of columns used for indexing and merging sector-level data.

        Returns:
        --------
        pd.DataFrame
            DataFrame containing the computed sum of background and estimated growth values for each sector.
        """
        bg_data = bg_data.rename(
            columns={col: f"{col}_bg" for col in build_out_columns}
        )
        estimated_data = estimated_data.rename(
            columns={col: f"{col}_etmt" for col in build_out_columns}
        )

        merged_data = bg_data.merge(estimated_data, on=index_columns, how="left")

        for col in build_out_columns:
            merged_data[col] = merged_data[f"{col}_bg"] + merged_data[f"{col}_etmt"]

        return merged_data[index_columns + build_out_columns]

    @staticmethod
    def calculate_product(
        data1: pd.DataFrame,
        data2: pd.DataFrame,
        build_out_columns: list,
        zone_index_columns: list,
    ) -> pd.DataFrame:
        """
        Scales the estimated growth values using provided scaling factors.

        Parameters:
        -----------
        data1 : pd.DataFrame
            DataFrame 1.

        data2 : pd.DataFrame
            DataFrame 2.

        build_out_columns : list
            List of column names representing growth-related data to be scaled.

        zone_index_columns : list
            List of columns used for indexing and merging zone-level data
            (e.g., ['ZONE_ID']).

        Returns:
        --------
        pd.DataFrame
            A DataFrame containing the scaled growth values for each zone.
        """

        # Rename columns to differentiate estimated values and scaling factors
        data1 = data1.rename(columns={col: f"{col}_d1" for col in build_out_columns})
        data2 = data2.rename(columns={col: f"{col}_d2" for col in build_out_columns})

        # Merge estimated data with scaling factors
        merged_data = data1.merge(data2, on=zone_index_columns, how="left")

        # Apply scaling factor to each growth column
        for col in build_out_columns:
            merged_data[col] = merged_data[f"{col}_d1"] * merged_data[f"{col}_d2"]

        # Retain only relevant columns
        return merged_data[zone_index_columns + build_out_columns]


def run(config: inputs.DLitConfig):

    if config.constraint is None:
        raise ValueError(
            "Cannot run constraint module without any constraint parameters"
        )

    LOG.info("Initialising Constraint Module")

    config.output_folder.mkdir(exist_ok=True)

    key_constraint_path = config.output_folder / f"M5_constraint"
    key_constraint_path.mkdir(exist_ok=True)

    LOG.info("Instantiating the Classes")
    # Instantiate the Classes
    processor = ConstraintProcessor(config)
    growth_calculator = GrowthCalculator()
    calc = ConstraintCalculation()

    LOG.info("Loading Key Inputs")
    cap_ratio = config.constraint.cap_ratio
    sector = config.constraint.sector.value
    # Load data into a dictionary
    lad_data = {
        "ddg_pop": pd.read_csv(config.constraint.ddg_pop),
        "ddg_emp": pd.read_csv(config.constraint.ddg_emp),
        "dlog_pop": pd.read_csv(config.constraint.dlog_pop),
        "dlog_emp": pd.read_csv(config.constraint.dlog_emp),
        "ntem_pop": pd.read_csv(config.constraint.ntem_pop),
        "ntem_emp": pd.read_csv(config.constraint.ntem_emp),
    }

    # Process data
    year_columns = [col for col in lad_data["dlog_pop"].columns if col.isdigit()]
    base_year_column = config.dev_pattern.base_year
    base_year_int = int(base_year_column)
    end_year_column = config.dev_pattern.end_year
    end_year_int = int(end_year_column)
    build_out_columns = [
        str(year) for year in range(base_year_int + 1, end_year_int + 1)
    ]
    # year_cols_ntem = [str(year) for year in range(base_year_int + 1, 2062)]

    ddg_col = "LAD13CD"

    # Process ntem data: extrapolate value for year 2062 onwards
    for key in ["ntem_pop", "ntem_emp"]:
        df = lad_data[key]
        df = processor.ntem_data_extrapolator(df, base_year_column, build_out_columns)
        lad_data[key] = df  # Save back to the dictionary
    # Process ddg data: Rename columns, filter, and drop "LON" rows
    for key in ["ddg_pop", "ddg_emp"]:
        df = lad_data[key]
        df.drop(
            columns=[
                col
                for col in df.columns
                if col not in [ddg_col] + [base_year_column] + build_out_columns
            ],
            inplace=True,
        )
        df.rename(columns={ddg_col: "lad2013_id"}, inplace=True)
        df.drop(df[df["lad2013_id"].str.startswith("LON")].index, inplace=True)
        # Sort the DataFrame by 'lad2013_id' (you can replace 'lad2013_id' with 'ddg_col' if needed)
        df.sort_values(by="lad2013_id", ascending=True, inplace=True)

        lad_data[key] = df  # Save back to the dictionary

    # Accessing sector-specific variables directly from the instance's sector_info_map
    sector_info = processor.sector_info_map.get(processor.sector, {})
    if not sector_info:
        LOG.error(f"No sector information found for sector {processor.sector}")
        return

    zone_id = sector_info.get("zone_id", "default_lad_id")
    name_column = "descriptions"
    sector_id = sector_info.get("sector_id", "default_region_id")
    name_column_sector = "zone_id"

    # Aggregate data at the regional level
    sector_data = {
        "ddg_pop": processor.sector_agg(
            lad_data["ddg_pop"], base_year_column, build_out_columns
        ),
        "ddg_emp": processor.sector_agg(
            lad_data["ddg_emp"], base_year_column, build_out_columns
        ),
        "dlog_pop": processor.sector_agg(
            lad_data["dlog_pop"], base_year_column, build_out_columns
        ),
        "dlog_emp": processor.sector_agg(
            lad_data["dlog_emp"], base_year_column, build_out_columns
        ),
        "ntem_pop": processor.sector_agg(
            lad_data["ntem_pop"], base_year_column, build_out_columns
        ),
        "ntem_emp": processor.sector_agg(
            lad_data["ntem_emp"], base_year_column, build_out_columns
        ),
    }
    LOG.info("Process to get datasets for LAD and aggregated LAD completed")
    # Define dataset mappings dynamically by looping through pop and emp keys
    datasets_process = []

    # Define purposes for looping
    ids = ["pop", "emp"]
    sources = ["ddg", "dlog", "ntem"]

    # Define geographies dynamically
    geographies = [("lad", lad_data), (sector, sector_data)]

    # Create datasets_process dynamically for LAD and Sector data
    for geo, data_dict in geographies:
        for source in sources:
            for id in ids:
                key = f"{source}_{id}"
                # Add the "source" column dynamically to the dataframe
                data_dict[key]["source"] = source
                # Find the index of the base_year_column
                base_year_index = data_dict[key].columns.get_loc(base_year_column)

                # Insert "source" column before base_year_column
                data_dict[key].insert(
                    base_year_index, "source", data_dict[key].pop("source")
                )

                datasets_process.append(
                    (
                        f"{geo}_{key}",
                        data_dict[key],
                        zone_id if geo == "lad" else sector_id,
                        name_column if geo == "lad" else name_column_sector,
                        False if geo == "lad" else True,
                        False if geo == "lad" else True,
                    )
                )

    # Initialize results dictionary to store processed data

    results = {}
    # Process each dataset with add_names_to_data
    for (
        data_name,
        data,
        id_col,
        name_col,
        use_sector_name,
        use_sector_cols,
    ) in datasets_process:
        processed_data = processor.add_names_to_lu_data(
            data,
            id_col,
            name_col,
            use_sector_name=use_sector_name,
            use_sector_cols=use_sector_cols,
        )
        # Add processed data to results, using data_name as the key
        results[data_name] = {
            "YearTotal": processed_data,
            "AbsoluteGrowth": growth_calculator.calculate_growth(
                processed_data,
                base_year_column,
                build_out_columns,
                growth_type="absolute",
            ),
            "GrowthRatio": growth_calculator.calculate_growth(
                processed_data, base_year_column, build_out_columns, growth_type="ratio"
            ).fillna(1),
            "GrowthRate": growth_calculator.calculate_growth(
                processed_data, base_year_column, build_out_columns, growth_type="rate"
            ).fillna(0),
            "AnnualGrowthRate": growth_calculator.calculate_annual_growth_rate(
                processed_data, year_columns
            ).fillna(0),
        }
        # processed_datasets.append(processed_data)

    # data_names = [data_name for data_name, _, _, _, _, _ in datasets_process]
    # print(data_names)

    LOG.info("Export datasets for LAD and Aggregated LAD")
    # List of subkeys in the required order
    subkeys = [
        "YearTotal",
        "AbsoluteGrowth",
        "GrowthRatio",
        "GrowthRate",
        "AnnualGrowthRate",
    ]
    output_path = key_constraint_path / f"output_for_viz"
    output_path.mkdir(exist_ok=True)
    for geography, _ in geographies:  # Ignore geo_data
        for subkey in subkeys:
            for id in ids:
                # Define Excel file name
                file_name = f"{geography}_{subkey}_{id}.xlsx"
                file_path = f"{output_path}/{file_name}"

                outputs = {}

                for source in sources:
                    key = f"{geography}_{source}_{id}"
                    if key in results:
                        df = results[key].get(subkey)
                        if df is not None:
                            sheet_name = f"{geography}_{source}_{id}"[
                                :31
                            ]  # Excel sheet name limit
                            outputs[sheet_name] = df

                if outputs:
                    # Check for empty DataFrames before writing
                    empty_sheets = [sheet for sheet, df in outputs.items() if df.empty]
                    if empty_sheets:
                        LOG.warning(
                            f"Skipping empty sheets in {file_path}: {empty_sheets}"
                        )

                    try:
                        utilities.write_to_excel(file_path, outputs)
                        LOG.info(f"Successfully wrote {file_path}")
                    except Exception as e:
                        LOG.error(f"Failed to write {file_path}: {e}")

    LOG.info(f"Constraining dlog data with DDG data")
    for id in ids:

        LOG.info(f"Working out the background growth at sector level for {id}")

        # sector_target_tot = growth_calculator.target_yeartot(
        #     results[f"{sector}_dlog_{id}"]["YearTotal"],
        #     results[f"{sector}_ddg_{id}"]["YearTotal"],
        #     base_year_column,
        #     build_out_columns,
        # )
        sector_target_tot = growth_calculator.target_yeartot(
            results[f"{sector}_ddg_{id}"]["YearTotal"],  # use ddg base year
            results[f"{sector}_ddg_{id}"]["YearTotal"],
            base_year_column,
            build_out_columns,
        )
        # zone_target_tot = growth_calculator.target_yeartot(
        #     results[f"lad_dlog_{id}"]["YearTotal"],
        #     results[f"lad_ddg_{id}"]["YearTotal"],
        #     base_year_column,
        #     build_out_columns,
        # )
        zone_target_tot = growth_calculator.target_yeartot(
            results[f"lad_ddg_{id}"]["YearTotal"],  # use ddg base year
            results[f"lad_ddg_{id}"]["YearTotal"],
            base_year_column,
            build_out_columns,
        )
        # sector_target_growth = growth_calculator.target_growth(
        #     results[f"{sector}_dlog_{id}"]["YearTotal"],
        #     results[f"{sector}_ddg_{id}"]["YearTotal"],
        #     base_year_column,
        #     build_out_columns,
        # )
        sector_target_growth = growth_calculator.target_growth(
            results[f"{sector}_ddg_{id}"]["YearTotal"],  # use ddg base year
            results[f"{sector}_ddg_{id}"]["YearTotal"],
            base_year_column,
            build_out_columns,
        )

        sector_estimated_growth = results[f"{sector}_dlog_{id}"]["AbsoluteGrowth"]

        sector_index_column_count = sector_target_growth.columns.get_loc(
            base_year_column
        )
        sector_index_columns = list(
            sector_target_growth.columns[:sector_index_column_count]
        )
        sector_ratio = calc.calculate_ratio(
            cap_ratio,
            sector_target_growth,
            sector_estimated_growth,
            build_out_columns,
            sector_index_columns,
        )
        # Apply conditional transformation
        for col in build_out_columns:
            sector_ratio[col] = np.where(
                (sector_ratio[col] > 1),
                1,  # set gap to zero to avoid additional background growth when estimated growth is not zero and the gap is negative (estimated exceeds target)
                sector_ratio[col],  # Keep original value otherwise
            )
        # print(sector_ratio)

        # Dlog estimated growth at lower geographical level
        zone_estimated_growth = results[f"lad_dlog_{id}"]["AbsoluteGrowth"]

        # Relative DDG related growth at lower geographical level
        zone_target_growth = growth_calculator.target_growth(
            results[f"lad_ddg_{id}"]["YearTotal"],  # use ddg base year
            results[f"lad_ddg_{id}"]["YearTotal"],
            base_year_column,
            build_out_columns,
        )

        # Create a list of columns for index
        zone_index_column_count = zone_target_growth.columns.get_loc(base_year_column)
        zone_index_columns = list(zone_target_growth.columns[:zone_index_column_count])
        zone_base_year = zone_target_growth[zone_index_columns + [base_year_column]]
        # Adjust zone_etmt growth using scaler to make sure the sector level total estimated growth won't exceed 95% of target growth
        zone_etmt_growth = zone_estimated_growth[zone_index_columns + build_out_columns]

        # Get zone scaler from sector gratio
        zone_scaler = zone_etmt_growth[zone_index_columns].copy()
        zone_scaler = zone_scaler.merge(sector_ratio, on="REGIONNM")
        zone_scaler = zone_scaler[zone_index_columns + build_out_columns]
        zone_scaled_etmt_growth = calc.calculate_product(
            zone_etmt_growth,
            zone_scaler,
            build_out_columns,
            zone_index_columns,
        )

        agg_zone_scaled_etmt_growth = zone_scaled_etmt_growth.groupby("REGIONNM")[
            build_out_columns
        ].sum()
        sector_scaled_etmt_growth = sector_target_growth[sector_index_columns].copy()
        sector_scaled_etmt_growth = sector_scaled_etmt_growth.merge(
            agg_zone_scaled_etmt_growth, on="REGIONNM"
        )

        sector_bg_growth = calc.calculate_gap(
            sector_target_growth,
            sector_scaled_etmt_growth,
            build_out_columns,
            sector_index_columns,
        )

        LOG.info(
            f"Calculating weight to distribute background growth for each LAD for {id}"
        )

        # calculate gap of growth between target (trend-based forecast) and estimated (from dlog) for each zone (lad)
        zone_gap_growth = calc.calculate_gap(
            zone_target_growth,
            zone_scaled_etmt_growth,
            build_out_columns,
            zone_index_columns,
        )
        zone_etmt_growth = zone_etmt_growth.rename(
            columns={col: f"{col}_etmt" for col in build_out_columns}
        )
        zone_gap_growth = zone_gap_growth.merge(
            zone_etmt_growth, on=zone_index_columns, how="left"
        )
        # Apply conditional transformation
        for col in build_out_columns:
            zone_gap_growth[col] = np.where(
                (zone_gap_growth[col] < 0) & (zone_gap_growth[f"{col}_etmt"] != 0),
                0,  # set gap to zero to avoid additional background growth when estimated growth is not zero and the gap is negative (estimated exceeds target)
                zone_gap_growth[col],  # Keep original value otherwise
            )

        # calculate weight to be used to distribute sector level background growth
        zone_weight = calc.calculate_zone_weights(
            zone_gap_growth,
            build_out_columns,
            zone_index_columns,
            sector_index_columns=["REGIONNM"],
        )
        # Get lower geographical level background growth
        zone_bg_growth = zone_target_growth[zone_index_columns].copy()
        zone_bg_growth = zone_bg_growth.merge(sector_bg_growth, on="REGIONNM")
        zone_bg_growth = calc.calculate_product(
            zone_bg_growth,
            zone_weight,
            build_out_columns,
            zone_index_columns,
        )
        agg_zone_bg_growth = zone_bg_growth.groupby("REGIONNM")[build_out_columns].sum()

        # Generating final adjusted data
        zone_adjusted_growth = calc.calculate_sum(
            zone_bg_growth,
            zone_scaled_etmt_growth,
            build_out_columns,
            zone_index_columns,
        )
        zone_adjusted_growth = zone_adjusted_growth.merge(
            zone_base_year, on=zone_index_columns, how="left"
        )
        agg_zone_adj_growth = zone_adjusted_growth.groupby("REGIONNM")[
            build_out_columns
        ].sum()
        # Create target year total
        zone_forecast = growth_calculator.yearly_totals_from_base(
            zone_adjusted_growth, base_year_column, build_out_columns
        )

        # # Get Scotland zonal data from DDG
        # zone_forecast_scotland = results[f"lad_ddg_{id}"]["YearTotal"].copy()
        # zone_forecast_scotland = zone_forecast_scotland[
        #     zone_forecast_scotland["REGIONNM"].str.strip().str.lower() == "scotland"
        # ]
        # zone_forecast_scotland = zone_forecast_scotland.drop(
        #     "source", axis=1, errors="ignore"
        # )

        # # Merge the dataframes on the index columns (LAD13CD, LADNM, REGIONNM)
        # zone_forecast = zone_forecast.merge(
        #     zone_forecast_scotland[zone_index_columns + build_out_columns],
        #     on=zone_index_columns,
        #     how="left",  # Ensure we keep all rows from zone_forecast
        #     suffixes=("", "_scotland"),
        # )

        # # Now update the year columns in zone_forecast with the values from zone_forecast_scotland
        # for year in build_out_columns:
        #     zone_forecast[year] = zone_forecast[year].fillna(
        #         zone_forecast[f"{year}_scotland"]
        #     )

        # # Drop the extra columns from the merge (e.g., the "_scotland" suffixed columns)
        # zone_forecast = zone_forecast.drop(
        #     columns=[f"{year}_scotland" for year in build_out_columns]
        # )

        # Agg zone forecast to sector level
        agg_zone_forecast = zone_forecast.groupby("REGIONNM")[build_out_columns].sum()
        agg_zone_forecast = agg_zone_forecast.reset_index()

        LOG.info(
            f"Exporting the intermediate outputs for {sector} and {id} for checking purpose for ddg_related constraining process"
        )
        inter_output_path = key_constraint_path / f"ddg_output_intermediate"
        inter_output_path.mkdir(exist_ok=True)
        # Define file names and corresponding data in a dictionary

        file_data_mapping = {
            "sector_target_tot": {
                "data": sector_target_tot,
                "file": f"{sector}_target_tot_{id}.csv",
            },
            "zone_target_tot": {
                "data": zone_target_tot,
                "file": f"lad_target_tot_{id}.csv",
            },
            "sector_target_growth": {
                "data": sector_target_growth,
                "file": f"{sector}_target_growth_{id}.csv",
            },
            "sector_estimated_growth": {
                "data": sector_estimated_growth,
                "file": f"{sector}_estimated_growth_{id}.csv",
            },
            "sector_bg_growth": {
                "data": sector_bg_growth,
                "file": f"{sector}_background_growth_{id}.csv",
            },
            "zone_target_growth": {
                "data": zone_target_growth,
                "file": f"lad_target_growth_{id}.csv",
            },
            "zone_bg_growth": {
                "data": zone_bg_growth,
                "file": f"lad_background_growth_{id}.csv",
            },
            "agg_zone_bg_growth": {
                "data": agg_zone_bg_growth,
                "file": f"agg_lad_background_growth_{id}.csv",
            },
            "zone_estimated_growth": {
                "data": zone_estimated_growth,
                "file": f"lad_estimated_growth_{id}.csv",
            },
            "zone_scaler": {"data": zone_scaler, "file": f"lad_scaler_{id}.csv"},
            "zone_scaled_etmt_growth": {
                "data": zone_scaled_etmt_growth,
                "file": f"lad_scaled_estimated_growth_{id}.csv",
            },
            "zone_adjusted_growth": {
                "data": zone_adjusted_growth,
                "file": f"lad_adjusted_growth_{id}.csv",
            },
            "agg_zone_adj_growth": {
                "data": agg_zone_adj_growth,
                "file": f"agg_lad_adjusted_growth_{id}.csv",
            },
        }
        for key, value in file_data_mapping.items():
            utilities.write_to_csv(inter_output_path / value["file"], value["data"])

        final_output_mapping = {
            "zone_forecast": {
                "data": zone_forecast,
                "file": f"lad_forecast_{id}_ddg.csv",
            },
            "agg_zone_forecast": {
                "data": agg_zone_forecast,
                "file": f"agg_lad_forecast_{id}_ddg.csv",
            },
        }

        for key, value in final_output_mapping.items():
            utilities.write_to_csv(key_constraint_path / value["file"], value["data"])

    LOG.info(f"Constraining dlog data with NTEM data")
    for id in ids:

        LOG.info(f"Working out the background growth at sector level for {id}")

        # sector_target_tot = growth_calculator.target_yeartot(
        #     results[f"{sector}_dlog_{id}"]["YearTotal"],
        #     results[f"{sector}_ddg_{id}"]["YearTotal"],
        #     base_year_column,
        #     build_out_columns,
        # )
        sector_target_tot = growth_calculator.target_yeartot(
            results[f"{sector}_dlog_{id}"]["YearTotal"],  # use dlog base year
            results[f"{sector}_ntem_{id}"]["YearTotal"],
            base_year_column,
            build_out_columns,
        )
        # zone_target_tot = growth_calculator.target_yeartot(
        #     results[f"lad_dlog_{id}"]["YearTotal"],
        #     results[f"lad_ddg_{id}"]["YearTotal"],
        #     base_year_column,
        #     build_out_columns,
        # )
        zone_target_tot = growth_calculator.target_yeartot(
            results[f"lad_dlog_{id}"]["YearTotal"],  # use dlog base year
            results[f"lad_ntem_{id}"]["YearTotal"],
            base_year_column,
            build_out_columns,
        )
        # sector_target_growth = growth_calculator.target_growth(
        #     results[f"{sector}_dlog_{id}"]["YearTotal"],
        #     results[f"{sector}_ddg_{id}"]["YearTotal"],
        #     base_year_column,
        #     build_out_columns,
        # )
        sector_target_growth = growth_calculator.target_growth(
            results[f"{sector}_dlog_{id}"]["YearTotal"],  # use dlog base year
            results[f"{sector}_ntem_{id}"]["YearTotal"],
            base_year_column,
            build_out_columns,
        )

        sector_estimated_growth = results[f"{sector}_dlog_{id}"]["AbsoluteGrowth"]

        sector_index_column_count = sector_target_growth.columns.get_loc(
            base_year_column
        )
        sector_index_columns = list(
            sector_target_growth.columns[:sector_index_column_count]
        )
        sector_ratio = calc.calculate_ratio(
            cap_ratio,
            sector_target_growth,
            sector_estimated_growth,
            build_out_columns,
            sector_index_columns,
        )
        # Apply conditional transformation
        for col in build_out_columns:
            sector_ratio[col] = np.where(
                (sector_ratio[col] > 1),
                1,  # set gap to zero to avoid additional background growth when estimated growth is not zero and the gap is negative (estimated exceeds target)
                sector_ratio[col],  # Keep original value otherwise
            )
        # print(sector_ratio)

        # Dlog estimated growth at lower geographical level
        zone_estimated_growth = results[f"lad_dlog_{id}"]["AbsoluteGrowth"]

        # Relative DDG related growth at lower geographical level
        zone_target_growth = growth_calculator.target_growth(
            results[f"lad_dlog_{id}"]["YearTotal"],  # use dlog base year
            results[f"lad_ntem_{id}"]["YearTotal"],
            base_year_column,
            build_out_columns,
        )

        # Create a list of columns for index
        zone_index_column_count = zone_target_growth.columns.get_loc(base_year_column)
        zone_index_columns = list(zone_target_growth.columns[:zone_index_column_count])
        zone_base_year = zone_target_growth[zone_index_columns + [base_year_column]]
        # Adjust zone_etmt growth using scaler to make sure the sector level total estimated growth won't exceed 95% of target growth
        zone_etmt_growth = zone_estimated_growth[zone_index_columns + build_out_columns]

        # Get zone scaler from sector gratio
        zone_scaler = zone_etmt_growth[zone_index_columns].copy()
        zone_scaler = zone_scaler.merge(sector_ratio, on="REGIONNM")
        zone_scaler = zone_scaler[zone_index_columns + build_out_columns]
        zone_scaled_etmt_growth = calc.calculate_product(
            zone_etmt_growth,
            zone_scaler,
            build_out_columns,
            zone_index_columns,
        )

        agg_zone_scaled_etmt_growth = zone_scaled_etmt_growth.groupby("REGIONNM")[
            build_out_columns
        ].sum()
        sector_scaled_etmt_growth = sector_target_growth[sector_index_columns].copy()
        sector_scaled_etmt_growth = sector_scaled_etmt_growth.merge(
            agg_zone_scaled_etmt_growth, on="REGIONNM"
        )

        sector_bg_growth = calc.calculate_gap(
            sector_target_growth,
            sector_scaled_etmt_growth,
            build_out_columns,
            sector_index_columns,
        )

        LOG.info(
            f"Calculating weight to distribute background growth for each LAD for {id}"
        )

        # calculate gap of growth between target (trend-based forecast) and estimated (from dlog) for each zone (lad)
        zone_gap_growth = calc.calculate_gap(
            zone_target_growth,
            zone_scaled_etmt_growth,
            build_out_columns,
            zone_index_columns,
        )
        zone_etmt_growth = zone_etmt_growth.rename(
            columns={col: f"{col}_etmt" for col in build_out_columns}
        )
        zone_gap_growth = zone_gap_growth.merge(
            zone_etmt_growth, on=zone_index_columns, how="left"
        )
        # Apply conditional transformation
        for col in build_out_columns:
            zone_gap_growth[col] = np.where(
                (zone_gap_growth[col] < 0) & (zone_gap_growth[f"{col}_etmt"] != 0),
                0,  # set gap to zero to avoid additional background growth when estimated growth is not zero and the gap is negative (estimated exceeds target)
                zone_gap_growth[col],  # Keep original value otherwise
            )

        # calculate weight to be used to distribute sector level background growth
        zone_weight = calc.calculate_zone_weights(
            zone_gap_growth,
            build_out_columns,
            zone_index_columns,
            sector_index_columns=["REGIONNM"],
        )
        # Get lower geographical level background growth
        zone_bg_growth = zone_target_growth[zone_index_columns].copy()
        zone_bg_growth = zone_bg_growth.merge(sector_bg_growth, on="REGIONNM")
        zone_bg_growth = calc.calculate_product(
            zone_bg_growth,
            zone_weight,
            build_out_columns,
            zone_index_columns,
        )
        agg_zone_bg_growth = zone_bg_growth.groupby("REGIONNM")[build_out_columns].sum()

        # Generating final adjusted data
        zone_adjusted_growth = calc.calculate_sum(
            zone_bg_growth,
            zone_scaled_etmt_growth,
            build_out_columns,
            zone_index_columns,
        )
        zone_adjusted_growth = zone_adjusted_growth.merge(
            zone_base_year, on=zone_index_columns, how="left"
        )
        agg_zone_adj_growth = zone_adjusted_growth.groupby("REGIONNM")[
            build_out_columns
        ].sum()
        # Create target year total
        zone_forecast = growth_calculator.yearly_totals_from_base(
            zone_adjusted_growth, base_year_column, build_out_columns
        )

        # # Get Scotland zonal data from DDG
        # zone_forecast_scotland = results[f"lad_ddg_{id}"]["YearTotal"].copy()
        # zone_forecast_scotland = zone_forecast_scotland[
        #     zone_forecast_scotland["REGIONNM"].str.strip().str.lower() == "scotland"
        # ]
        # zone_forecast_scotland = zone_forecast_scotland.drop(
        #     "source", axis=1, errors="ignore"
        # )

        # # Merge the dataframes on the index columns (LAD13CD, LADNM, REGIONNM)
        # zone_forecast = zone_forecast.merge(
        #     zone_forecast_scotland[zone_index_columns + build_out_columns],
        #     on=zone_index_columns,
        #     how="left",  # Ensure we keep all rows from zone_forecast
        #     suffixes=("", "_scotland"),
        # )

        # # Now update the year columns in zone_forecast with the values from zone_forecast_scotland
        # for year in build_out_columns:
        #     zone_forecast[year] = zone_forecast[year].fillna(
        #         zone_forecast[f"{year}_scotland"]
        #     )

        # # Drop the extra columns from the merge (e.g., the "_scotland" suffixed columns)
        # zone_forecast = zone_forecast.drop(
        #     columns=[f"{year}_scotland" for year in build_out_columns]
        # )

        # Agg zone forecast to sector level
        agg_zone_forecast = zone_forecast.groupby("REGIONNM")[build_out_columns].sum()
        agg_zone_forecast = agg_zone_forecast.reset_index()

        LOG.info(
            f"Exporting the intermediate outputs for {sector} and {id} for checking purpose for ntem_related constraining process"
        )
        inter_output_path = key_constraint_path / f"ntem_output_intermediate"
        inter_output_path.mkdir(exist_ok=True)
        # Define file names and corresponding data in a dictionary

        file_data_mapping = {
            "sector_target_tot": {
                "data": sector_target_tot,
                "file": f"{sector}_target_tot_{id}.csv",
            },
            "zone_target_tot": {
                "data": zone_target_tot,
                "file": f"lad_target_tot_{id}.csv",
            },
            "sector_target_growth": {
                "data": sector_target_growth,
                "file": f"{sector}_target_growth_{id}.csv",
            },
            "sector_estimated_growth": {
                "data": sector_estimated_growth,
                "file": f"{sector}_estimated_growth_{id}.csv",
            },
            "sector_bg_growth": {
                "data": sector_bg_growth,
                "file": f"{sector}_background_growth_{id}.csv",
            },
            "zone_target_growth": {
                "data": zone_target_growth,
                "file": f"lad_target_growth_{id}.csv",
            },
            "zone_bg_growth": {
                "data": zone_bg_growth,
                "file": f"lad_background_growth_{id}.csv",
            },
            "agg_zone_bg_growth": {
                "data": agg_zone_bg_growth,
                "file": f"agg_lad_background_growth_{id}.csv",
            },
            "zone_estimated_growth": {
                "data": zone_estimated_growth,
                "file": f"lad_estimated_growth_{id}.csv",
            },
            "zone_scaler": {"data": zone_scaler, "file": f"lad_scaler_{id}.csv"},
            "zone_scaled_etmt_growth": {
                "data": zone_scaled_etmt_growth,
                "file": f"lad_scaled_estimated_growth_{id}.csv",
            },
            "zone_adjusted_growth": {
                "data": zone_adjusted_growth,
                "file": f"lad_adjusted_growth_{id}.csv",
            },
            "agg_zone_adj_growth": {
                "data": agg_zone_adj_growth,
                "file": f"agg_lad_adjusted_growth_{id}.csv",
            },
        }
        for key, value in file_data_mapping.items():
            utilities.write_to_csv(inter_output_path / value["file"], value["data"])

        final_output_mapping = {
            "zone_forecast": {
                "data": zone_forecast,
                "file": f"lad_forecast_{id}_ntem.csv",
            },
            "agg_zone_forecast": {
                "data": agg_zone_forecast,
                "file": f"agg_lad_forecast_{id}_ntem.csv",
            },
        }

        for key, value in final_output_mapping.items():
            utilities.write_to_csv(key_constraint_path / value["file"], value["data"])

    LOG.info("Data processing, aggregation completed")
