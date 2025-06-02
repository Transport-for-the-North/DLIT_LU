import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, List, Optional
from pathlib import Path

# Local imports
from dlit_lu import constraint, inputs, utilities
from dlit_lu import large_sites as ls

#
# from caf.base.data_structures import DVector
import caf.base as cb


LOG = logging.getLogger(__name__)


import os
import pandas as pd


class TEMOutputProcessor(ls.BaseZoneHandler):
    def __init__(
        self,
        config: inputs.DLitConfig,
        geo_boundary_override: Optional[inputs.GeoBoundary],
        base_dir: Path,
    ):
        """
        Initializes the TEMOutputProcessor with configuration settings and paths.
        Args:
            config (inputs.DLitConfig): The configuration object containing paths and settings.
            geo_boundary_override (Optional[inputs.GeoBoundary]): Optional override for geo boundary.
            base_dir (str): Base directory for the output files.
        """
        # Call the parent class's __init__ to inherit all other methods and attributes
        super().__init__(config, geo_boundary_override=geo_boundary_override)
        self.base_dir = base_dir
        self.subfolders = {
            "hb_productions": "hb_normits_tem_segmented_{}_dvec.h5",
            "hb_attractions": "hb_normits_tem_segmented_{}_dvec.h5",
            "nhb_productions": "nhb_normits_tem_segmented_{}_dvec.h5",
            "nhb_attractions": "nhb_normits_tem_segmented_{}_dvec.h5",
        }
        self.zone_dvec_id = self.zone_info["zone_gdf_id_col"]
        self.zone_id = self.zone_info["group_by_column"]

    def process_output(self, subfolder: str, years: list[str]) -> pd.DataFrame | None:
        if subfolder not in self.subfolders:
            raise ValueError(
                f"Unknown subfolder '{subfolder}'. Must be one of {list(self.subfolders.keys())}"
            )

        folder_path = self.base_dir / subfolder
        file_template = self.subfolders[subfolder]
        processed_dataframes = []

        for year in years:
            filename = file_template.format(year)
            file_path = os.path.join(folder_path, filename)
            zone_dvec_id = self.zone_dvec_id
            zone_id = self.zone_id
            if not os.path.exists(file_path):
                print(f"File not found: {file_path}")
                continue

            # df = pd.read_hdf(file_path, key="data")
            dvec = cb.DVector.load(file_path)
            zone = cb.ZoningSystem.get_zoning("normits")
            zone_dvec = dvec.aggregate_comp_zones(zone)
            df = zone_dvec.data
            df = df.reset_index()
            # print(df)

            # Universal transformation for all subfolders
            df = df[~df["tp"].isin([5, 6])]
            df = df[df["m"] != 7]
            df["m"] = df["m"].replace(4, 3)
            df = df.drop(columns=["hh_type", "tp"])

            df = df.groupby(["p", "m"]).sum().reset_index()

            id_vars = ["p", "m"]
            value_vars = [col for col in df.columns if col not in id_vars]
            df = df.melt(
                id_vars=id_vars,
                value_vars=value_vars,
                var_name=zone_dvec_id,
                value_name="value",
            )
            df["value"] = df["value"] / 5
            df["year"] = year

            processed_dataframes.append(df)

        if not processed_dataframes:
            print(f"No valid files processed for subfolder '{subfolder}'.")
            return None

        combined_df = pd.concat(processed_dataframes, axis=0)
        combined_df = combined_df.pivot_table(
            index=[zone_dvec_id, "p", "m"], columns="year", values="value"
        ).reset_index()
        combined_df.columns.name = None
        combined_df = combined_df.rename(columns={zone_dvec_id: zone_id})

        return combined_df


class TEConstraintProcessor(constraint.ConstraintProcessor):
    """
    This class extends ConstraintProcessor to specifically handle trip-end related
    constraints while reusing methods from the base class.
    """

    def __init__(self, config: inputs.DLitConfig) -> None:
        """
        Initializes the TripEndConstraintProcessor with trip-end specific configuration settings.

        Args:
            config (inputs.DLitConfig): The configuration object containing constraint details.
        """
        # Call the parent class's __init__ to inherit all other methods and attributes
        super().__init__(config)

        # Override the sector assignment if needed for trip-end specific logic
        # Here, you can define any additional initialization required
        self.config: inputs.DLitConfig = config
        self.sector: inputs.Sector = (
            config.tripend.sector
        )  # Modify if needed for trip-end-specific sector
        self.sector_info_map = inputs.SECTOR_INFO_MAP
        self.base_year_column = config.large_sites.base_year
        self.future_year_columns = [str(year) for year in config.split.future_years]
        # Validate sector
        if self.sector not in self.sector_info_map:
            raise ValueError(f"Unsupported sector: {self.sector}")

        # Retrieve sector-specific metadata from predefined SECTOR_INFO_MAP
        self.sector_info: Dict[str, Any] = self.sector_info_map[self.sector]

        # Initialize the BaseZoneHandler for geo_boundary and other zone info
        self.base_zone_handler = ls.BaseZoneHandler(
            config, geo_boundary_override=inputs.GeoBoundary.NORMITS
        )
        # Access zone information based on geo_boundary
        self.zone_info = self.base_zone_handler.zone_info
        # Example: If you want to update other sector-specific information
        self.sector_info["lookup_path"] = self.zone_info[
            "zone_to_lad_path"
        ]  # config.large_sites.summary_data.normits_to_lad_file
        self.sector_info["zone_id"] = self.zone_info[
            "group_by_column"
        ]  # "normits_v3.3_id"
        self.sector_info["sector_id"] = self.zone_info["lad_id_col"]  # "lad2013_id"
        self.sector_info["zone_to_sector_prop_col"] = self.zone_info[
            "zone_to_lad_prop"
        ]  # "normits_v3.3_to_lad2013"
        self.sector_info["sector_name"] = self.config.constraint.lad_name

    def load_zone_translation(self, model_zone) -> pd.DataFrame:
        """
        Loads the appropriate zone translation file based on the model_zone value.

        Args:
            model_zone (GeoBoundary): The model zone type to determine which translation file to load.

        Returns:
            pd.DataFrame: The translation file as a DataFrame, or None if no translation is needed.
        """

        if model_zone == inputs.GeoBoundary.NORMITS.value:
            # No translation file needed for normits
            return None
        elif model_zone == inputs.GeoBoundary.LSOA.value:
            return pd.read_csv(self.config.large_sites.lsoa_to_normits)
        # elif model_zone == inputs.GeoBoundary.MSOA.value:
        #    return pd.read_csv(self.config.tripend.normits_to_msoa)
        # elif model_zone == inputs.GeoBoundary.NOHAM.value:
        #    return pd.read_csv(self.config.tripend.normits_to_noham)
        # elif model_zone == inputs.GeoBoundary.NORMS.value:
        #    return pd.read_csv(self.config.tripend.normits_to_norms)
        else:
            # Optional: raise an exception or handle an unsupported model_zone
            raise ValueError(f"Unsupported model_zone: {model_zone}")

    def merge_zone_data(
        self,
        by_data: pd.DataFrame,
        new_data: pd.DataFrame,
        zone_col_in_by: list[str],  # Accepting a list of column names
    ) -> pd.DataFrame:
        """
        Merge new dwelling, population, and job data into the existing zonal data.

        Parameters
        ----------
        by_data : pd.DataFrame
            Existing zonal data to be updated.
        new_data : pd.DataFrame
            DataFrame containing new dwelling, population, and job data with zone columns.
        zone_col_in_by : list[str]
            List of column names representing zones in by_data.

        Returns
        -------
        pd.DataFrame
            Updated zonal data with merged dwelling, population, and job information.
        """
        # Merging by_data with new_data to create zonal_data
        base_year_column = self.base_year_column
        future_year_columns = self.future_year_columns

        # Perform the merge operation with multiple columns for zones
        zonal_data = by_data.merge(
            new_data,
            on=zone_col_in_by,  # Using the list of zone columns for merging
            how="left",
        )

        # Return the relevant columns (zone, base year, and future years)
        zonal_data = zonal_data[
            zone_col_in_by + [base_year_column] + future_year_columns
        ]
        return zonal_data

    def aggregate_to_sector_p_m(
        self,
        data: pd.DataFrame,
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

        Returns
        ------
        pd.DataFrame
            DataFrame containing aggregated region-level data with columns for the base year and future years.

        Raises
        ------
        ValueError
            If the total values before and after aggregation do not match, indicating a discrepancy in the aggregation process.
        """
        base_year_column = self.base_year_column
        future_year_columns = self.future_year_columns
        lookup_path = self.sector_info["lookup_path"]
        zone_id = self.sector_info["zone_id"]
        sector_id = self.sector_info["sector_id"]
        zone_to_sector_prop_col = self.sector_info["zone_to_sector_prop_col"]

        lookup_df = pd.read_csv(lookup_path)

        # Construct val_cols with base_year_column only if it's in the DataFrame
        val_cols = future_year_columns.copy()
        if base_year_column in data.columns:
            val_cols = [base_year_column] + val_cols

        totals_before = {col: data[col].sum() for col in val_cols}

        merged_df = data.merge(
            lookup_df[[zone_id, sector_id, zone_to_sector_prop_col]],
            on=zone_id,
            how="left",
        )

        for col in val_cols:
            merged_df[col] = merged_df[col] * merged_df[zone_to_sector_prop_col]

        sector_agg = merged_df.groupby([sector_id, "p", "m"], as_index=False)[
            val_cols
        ].sum()

        totals_after = {col: sector_agg[col].sum() for col in val_cols}

        for col in val_cols:
            if not np.all(np.isclose(totals_before[col], totals_after[col])):
                raise ValueError(
                    f"Total of '{col}' changed after aggregation: before={totals_before[col]}, after={totals_after[col]}"
                )

        return sector_agg

    def add_names_to_te_data(
        self,
        data: pd.DataFrame,
        id_column: str,
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
            None

        # Read the name file
        name_df = pd.read_csv(name_file)
        sector = self.sector.value
        if use_sector_cols:
            # Merge
            data_with_name = data.merge(
                name_df[["zone_name", "descriptions"]],
                left_on=id_column,
                right_on="zone_name",
                how="left",
            )
            data_with_name = data_with_name.drop(columns=["zone_name"])
            data_with_name = data_with_name.rename(
                columns={"descriptions": f"{sector}nm"}
            )
            columns = list(data_with_name.columns)
            columns.remove(f"{sector}nm")
            columns.insert(1, f"{sector}nm")
            data_with_name = data_with_name[columns]

        else:
            data_with_name = data.copy()
            # Add sector names to zone data
            zone_id = self.sector_info["zone_id"]
            lookup_path = self.sector_info["lookup_path"]
            sector_id = self.sector_info["sector_id"]

            if zone_id in data_with_name.columns:
                zone_to_sector_df = pd.read_csv(
                    lookup_path, usecols=[zone_id, sector_id]
                )
                sector_name_df = name_df
                data_with_name = data_with_name.merge(
                    zone_to_sector_df,
                    # left_on="LAD13CD",
                    # right_on="lad2013_id",
                    on=zone_id,
                    how="left",
                )
                data_with_name = data_with_name.merge(
                    sector_name_df[["zone_name", "descriptions"]],
                    left_on=sector_id,
                    right_on="zone_name",
                    how="left",
                )
                data_with_name = data_with_name.drop(columns=["zone_name"])
                data_with_name = data_with_name.rename(
                    columns={"descriptions": f"{sector}nm"}
                )
                cols = list(data_with_name.columns)
                # Define the preferred order for the first three columns
                priority_cols = [zone_id, sector_id, f"{sector}nm"]
                for col in priority_cols:
                    if col in cols:
                        cols.remove(col)
                # Insert them at the beginning in the desired order
                cols = priority_cols + cols
                data_with_name = data_with_name[cols]

        LOG.info(f"Added sector name to dataset with shape {data_with_name.shape}")

        return data_with_name


# class ByTeTransformation:
#     def __init__(self, df: pd.DataFrame, base_year_column: str):
#         """
#         Initialize the class with the given DataFrame and base year.

#         Parameters
#         ----------
#         df : pd.DataFrame
#             Input DataFrame with mode and period columns.
#         base_year : int
#             The base year to rename "prod" and "attr".
#         """
#         self.df = df.copy()

#         # Exclude unwanted categories for mode and period
#         self.df = self.df[~self.df["mode"].isin([7])]  # Exclude mode category 7 for Air
#         self.df = self.df[
#             self.df["period"].isin([1, 2, 3, 4])
#         ]  # Keep only period categories 1, 2, 3, and 4
#         self.df["mode"] = self.df["mode"].replace(
#             {4: 3}
#         )  # Merge 4 into 3 for mode to combine car driver and car passenger
#         self.base_year_column = base_year_column

#     def groupby_sum(self, val: str):
#         """
#         Group the DataFrame by "normits_v3.3_id", "purpose", "mode", and "period",
#         summing the specified value column.

#         Parameters
#         ----------
#         val : str
#             The column to be summed, either "prod" or "attr".

#         Returns
#         -------
#         pd.DataFrame
#             Grouped DataFrame with summed values.
#         """
#         if val not in self.df.columns:
#             raise ValueError(f"Column '{val}' not found in DataFrame")

#         grouped_df = self.df.groupby(
#             ["normits_v3.3_id", "purpose", "mode"], as_index=False
#         )[val].sum()
#         # Divide the summed value by 5 to get average week day totals
#         grouped_df[val] = grouped_df[val] / 5
#         # Rename columns
#         grouped_df = grouped_df.rename(
#             columns={
#                 "purpose": "p",
#                 "mode": "m",
#                 val: self.base_year_column,  # Rename "prod" or "attr" to base year
#             }
#         )

#         return grouped_df


class ForecastComparator:
    def __init__(
        self,
        df1,
        df2,
        key_columns,
        year_columns,
        df1_label="target",
        df2_label="output",
        use_tolerance=False,
        tol=1e-6,
        output_path=None,
    ):
        """
        Initialize the comparator with two dataframes and comparison settings.

        :param df1: First DataFrame (e.g., sector_target_tot)
        :param df2: Second DataFrame (e.g., agg_zone_forecast)
        :param key_columns: Columns to join on (common identifiers)
        :param year_columns: List of year columns to compare
        :param df1_label: Label suffix for df1
        :param df2_label: Label suffix for df2
        :param use_tolerance: Whether to use np.isclose for comparison
        :param tol: Tolerance level for np.isclose
        """
        self.df1 = df1
        self.df2 = df2
        self.key_columns = key_columns
        self.year_columns = year_columns
        self.df1_label = df1_label
        self.df2_label = df2_label
        self.use_tolerance = use_tolerance
        self.tol = tol
        self.merged = None
        self.differences = []
        self.output_path = output_path if output_path else "output"

    def merge_data(self):
        self.merged = pd.merge(
            self.df1,
            self.df2,
            on=self.key_columns,
            suffixes=(f"_{self.df1_label}", f"_{self.df2_label}"),
            how="outer",
            indicator=True,
        )

    def compare(self):
        self.merge_data()
        self.differences.clear()
        for year in self.year_columns:
            col1 = f"{year}_{self.df1_label}"
            col2 = f"{year}_{self.df2_label}"
            if self.use_tolerance:
                mismatch = self.merged[
                    ~np.isclose(
                        self.merged[col1],
                        self.merged[col2],
                        atol=self.tol,
                        equal_nan=True,
                    )
                ]
            else:
                mismatch = self.merged[self.merged[col1] != self.merged[col2]]
            if not mismatch.empty:
                self.differences.append(
                    (year, mismatch[[*self.key_columns, col1, col2]])
                )

    def export(self):
        """
        Export the differences to a CSV file.

        :param output_path: Path to save the CSV file
        """
        if not self.differences:
            print("No differences found.")
            return

        # Create a DataFrame to hold all differences
        all_differences = pd.concat(
            [df for _, df in self.differences], ignore_index=True
        )
        output_file = self.output_path + f"/sector_differences.csv"

        utilities.write_to_csv(output_file, all_differences)

        print(f"Differences exported to {self.output_path}")

    def report(self):
        if self.differences:
            for year, df in self.differences:
                print(f"Mismatch found in year '{year}':")
                print(df)
            print(
                "Discrepancies found between input dataframes, but the process will continue."
            )
        else:
            print("All values match across future year columns.")

    def run_comparison(self):
        self.compare()
        self.export()
        self.report()


def summarize_year_sums(dfs_dict, year_columns):
    # dfs_dict: dict of DataFrames keyed by category keys
    # year_columns: list of columns (years) to sum across

    summed = pd.DataFrame()
    for key, df in dfs_dict.items():
        # Sum only the specified year columns
        year_sums = df[year_columns].sum()
        summed[key] = year_sums

    # Add a total column summing across all categories per year
    summed["total"] = summed.sum(axis=1)
    # Reset index to turn year index into a column, and put it first
    summed = summed.reset_index().rename(columns={"index": "year"})
    return summed


def run(config: inputs.DLitConfig):
    """Main function to run the Tripends processing."""

    if config.run_tripend is None:
        raise ValueError("Cannot run tripend module without parameters")

    LOG.info("Initializing Tripend Module")
    config.output_folder.mkdir(exist_ok=True)
    base_year = int(config.large_sites.base_year)
    base_year_column = str(base_year)
    future_years = config.split.future_years
    future_year_columns = [str(year) for year in future_years]
    # future_year_column = str(2024)
    cap_ratio = config.constraint.cap_ratio
    key_te_folder = config.output_folder / "M7_tripend"
    key_te_folder.mkdir(exist_ok=True)
    model_zone = config.large_sites.geo_boundary.value

    lookup_lad_region_file = config.constraint.lad_to_region_file
    lookup_lad_region = pd.read_csv(lookup_lad_region_file)
    LOG.info("Defining key lists")
    # Lists of categories and data types
    categories = ["hb"]  # ["hb", "nhb"],["hb"], ["nhb"]
    tes = ["prod", "attr"]  # trip end to be either production or attraction
    sources = ["dlog", "ntem"]  # dlog or ntem data
    sector_list = [
        "Cheshire West and Chester",
        "Barrow-in-Furness",
        "Dumfries and Galloway",
    ]  # ["Bury", "Manchester", "Oldham", "Rochdale", "Salford", "Stockport", "Tameside", "Trafford"]
    mode_list = [3]  # list of mode to be filtered
    LOG.info("Instantiating TEConstraintProcessor")
    te_cp = TEConstraintProcessor(config)
    zone_translation = te_cp.load_zone_translation(model_zone)
    sector_info = te_cp.sector_info
    zone_id = sector_info["zone_id"]
    sector_id = sector_info["sector_id"]
    sector = config.tripend.sector.value
    zone_index_columns_initial = [zone_id, "p", "m"]

    LOG.info(
        "Instantiating GrowthCalculator and ConstraintCalculation from constraint module"
    )
    growth_calculator = constraint.GrowthCalculator()
    calc = constraint.ConstraintCalculation()
    LOG.info("Loading and transforming trip end dvec h5 data")
    # Load datasets
    # Instantiate TEMOutputProcessor
    tem_tot = TEMOutputProcessor(
        config, inputs.GeoBoundary.NORMITS, config.tripend.dlog_te_tot_path
    )
    tem_lsgrth = TEMOutputProcessor(
        config, inputs.GeoBoundary.NORMITS, config.tripend.dlog_te_lsgrth_path
    )
    # Map short keys to subfolder names
    category_map = {
        "dlog_hb_prod": "hb_productions",
        "dlog_hb_attr": "hb_attractions",
        "dlog_nhb_prod": "nhb_productions",
        "dlog_nhb_attr": "nhb_attractions",
    }

    dlog_te_tot_dfs = {
        key: tem_tot.process_output(
            sub_folder, [base_year_column] + future_year_columns
        )
        for key, sub_folder in category_map.items()
    }
    summary_totals = summarize_year_sums(
        dlog_te_tot_dfs, [base_year_column] + future_year_columns
    )

    dlog_te_lsgrth_dfs = {
        key: tem_lsgrth.process_output(sub_folder, future_year_columns)
        for key, sub_folder in category_map.items()
    }
    summary_ls_growth = summarize_year_sums(dlog_te_lsgrth_dfs, future_year_columns)

    print("Summary of totals:\n", summary_totals)
    print("\nSummary of growths:\n", summary_ls_growth)

    # Export individual total DataFrames
    for key, df in dlog_te_tot_dfs.items():
        file_path = os.path.join(key_te_folder, f"{key}_total.csv")
        utilities.write_to_csv(file_path, df)
        print(f"Saved {file_path}")
    for key, df in dlog_te_lsgrth_dfs.items():
        file_path = os.path.join(key_te_folder, f"{key}_lsgrowth.csv")
        utilities.write_to_csv(file_path, df)
    # Export key dataFrames to CSV files
    utilities.write_to_csv(key_te_folder / "summary_totals.csv", summary_totals)
    utilities.write_to_csv(key_te_folder / "summary_growth.csv", summary_ls_growth)

    ntem_zone_te = {
        "ntem_hb_prod": pd.read_csv(config.tripend.ntem_zone_hb_prod)[
            [zone_id, "p", "m", base_year_column] + future_year_columns
        ],
        "ntem_hb_attr": pd.read_csv(config.tripend.ntem_zone_hb_attr)[
            [zone_id, "p", "m", base_year_column] + future_year_columns
        ],
        "ntem_nhb_prod": pd.read_csv(config.tripend.ntem_zone_nhb_prod)[
            [zone_id, "p", "m", base_year_column] + future_year_columns
        ],
        "ntem_nhb_attr": pd.read_csv(config.tripend.ntem_zone_nhb_attr)[
            [zone_id, "p", "m", base_year_column] + future_year_columns
        ],
    }

    LOG.info("Aggregating zonal data to sector level")

    zone_data_sets = {}
    for source in sources:
        for category in categories:
            for te in tes:
                key = f"{source}_{category}_{te}"

                if source == "dlog":
                    # Access dictionary variable, not a formatted string
                    zone_data_sets[key] = dlog_te_tot_dfs[key]
                else:
                    zone_data_sets[key] = ntem_zone_te[key]

    sector_data_sets = {}
    for key, df in zone_data_sets.items():
        sector_data_sets[key] = te_cp.aggregate_to_sector_p_m(
            df,
        )
    # # Check the shape of the sector dataframe
    # print(sector_data_sets["dlog_nhb_prod"])
    # print(sector_data_sets["dlog_nhb_attr"])

    LOG.info("Inserting name of sector to the data")
    # Define dataset mappings dynamically by looping through pop and emp keys
    datasets_process = []
    # Define geographies dynamically
    geographies = [("zone", zone_data_sets), (sector, sector_data_sets)]

    # Create datasets_process dynamically for zone and Sector data
    for geo, data_dict in geographies:
        for source in sources:
            for category in categories:
                for te in tes:
                    key = f"{source}_{category}_{te}"
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
                            zone_id if geo == "zone" else sector_id,
                            True,
                            False if geo == "zone" else True,
                        )
                    )

    # Initialize results dictionary to store processed data

    results = {}
    # Process each dataset with add_names_to_data
    for (
        data_name,
        data,
        id_col,
        use_sector_name,
        use_sector_cols,
    ) in datasets_process:
        processed_data = te_cp.add_names_to_te_data(
            data,
            id_col,
            use_sector_name=use_sector_name,
            use_sector_cols=use_sector_cols,
        )
        # Add processed data to results, using data_name as the key
        results[data_name] = {
            "YearTotal": processed_data,
            "AbsoluteGrowth": growth_calculator.calculate_growth(
                processed_data,
                base_year_column,
                future_year_columns,
                growth_type="absolute",
            ),
            "GrowthRatio": growth_calculator.calculate_growth(
                processed_data,
                base_year_column,
                future_year_columns,
                growth_type="ratio",
            ).fillna(1),
            "GrowthRate": growth_calculator.calculate_growth(
                processed_data,
                base_year_column,
                future_year_columns,
                growth_type="rate",
            ).fillna(0),
            "AnnualGrowthRate": growth_calculator.calculate_annual_growth_rate(
                processed_data, future_year_columns
            ).fillna(0),
        }

    # Export datasets for visualization
    export_for_viz = config.tripend.export_for_viz
    if export_for_viz:
        LOG.info("Exporting datasets for LAD and Aggregated LAD")
        # Define the list of locations to filter
        # sector_list = ["Bury", "Manchester", "Oldham", "Rochdale",
        #             "Salford", "Stockport", "Tameside", "Trafford"]

        # List of subkeys in the required order
        subkeys = [
            "YearTotal",
            "AbsoluteGrowth",
            "GrowthRatio",
            "GrowthRate",
            "AnnualGrowthRate",
        ]
        output_path = key_te_folder / f"output_for_viz"
        output_path.mkdir(exist_ok=True)
        for geography, _ in geographies:  # Ignore geo_data
            for subkey in subkeys:
                for category in categories:
                    for te in tes:
                        id = f"{category}_{te}"
                        # Define Excel file name
                        file_name = f"{geography}_{subkey}_{id}.xlsx"
                        file_path = f"{output_path}/{file_name}"

                        outputs = {}

                        for source in sources:
                            key = f"{geography}_{source}_{id}"
                            if key in results:
                                df = results[key].get(subkey)
                                if df is not None and f"{sector}nm" in df.columns:
                                    # Check if sector_list is provided (non-empty)
                                    if sector_list and geography == "zone":
                                        # Filter DataFrame to include only rows where sector name is in sector_list when sector_list is provided AND geography is "zone"
                                        df_filtered = df[
                                            df[f"{sector}nm"].isin(sector_list)
                                        ]
                                    else:
                                        # Use the original DataFrame if sector_list is not provided
                                        df_filtered = df

                                    if (
                                        not df_filtered.empty
                                    ):  # Ensure it's not empty after filtering
                                        sheet_name = f"{geography}_{source}_{id}"[
                                            :31
                                        ]  # Excel sheet name limit
                                        outputs[sheet_name] = df_filtered

                        if outputs:
                            # Check for empty DataFrames before writing
                            empty_sheets = [
                                sheet for sheet, df in outputs.items() if df.empty
                            ]
                            if empty_sheets:
                                LOG.warning(
                                    f"Skipping empty sheets in {file_path}: {empty_sheets}"
                                )

                            try:
                                utilities.write_to_excel(file_path, outputs)
                                LOG.info(f"Successfully wrote {file_path}")
                            except Exception as e:
                                LOG.error(f"Failed to write {file_path}: {e}")

    LOG.info(f"Constraining dlog data with ntem growth")
    te_output = {}
    te_ls_output = {}
    for category in categories:
        for te in tes:
            id = f"{category}_{te}"

            LOG.info(f"Working out the background growth at sector level for {id}")

            sector_target_tot = growth_calculator.target_yeartot(
                results[f"{sector}_dlog_{id}"][
                    "YearTotal"
                ],  # base year total from dlog
                results[f"{sector}_ntem_{id}"][
                    "YearTotal"
                ],  # fugure year target derived from ntem growth
                base_year_column,
                future_year_columns,
            )
            zone_target_tot = growth_calculator.target_yeartot(
                results[f"zone_dlog_{id}"]["YearTotal"],  # base year total from dlog
                results[f"zone_ntem_{id}"][
                    "YearTotal"
                ],  # fugure year target derived from ntem growth
                base_year_column,
                future_year_columns,
            )

            sector_target_growth = growth_calculator.target_growth(
                results[f"{sector}_dlog_{id}"][
                    "YearTotal"
                ],  # base year total from dlog
                results[f"{sector}_ntem_{id}"][
                    "YearTotal"
                ],  # fugure year growth from ntem
                base_year_column,
                future_year_columns,
            )
            # print(sector_target_growth)
            sector_estimated_growth = results[f"{sector}_dlog_{id}"]["AbsoluteGrowth"]

            sector_index_column_count = sector_target_growth.columns.get_loc(
                base_year_column
            )
            sector_index_columns = list(
                sector_target_growth.columns[:sector_index_column_count]
            )
            # print(sector_index_columns)

            sector_ratio = calc.calculate_ratio(
                cap_ratio,
                sector_target_growth,
                sector_estimated_growth,
                future_year_columns,
                sector_index_columns,
                1e-5,
            )
            # Apply conditional transformation
            # Merge the two dataframes on sector_index_columns
            merged = sector_ratio.merge(
                sector_target_growth,
                on=sector_index_columns,
                suffixes=("_ratio", "_target"),
            )

            # Apply conditional transformation for each future year column
            for col in future_year_columns:
                merged[col + "_ratio"] = np.where(
                    (merged[col + "_ratio"] > 1)
                    & (
                        merged[col + "_target"] > 0
                    ),  # set ratio to 1 when target growth is larger than estimated growth when both are positive
                    1,
                    merged[col + "_ratio"],
                )

            # Reconstruct adjusted sector_ratio DataFrame
            adjusted_cols = sector_index_columns + [
                col + "_ratio" for col in future_year_columns
            ]
            sector_ratio_adjusted = merged[adjusted_cols].copy()

            # Rename columns back to original
            sector_ratio_adjusted.rename(
                columns={col + "_ratio": col for col in future_year_columns},
                inplace=True,
            )

            # sector_ratio = sector_ratio.set_index(sector_index_columns)
            # sector_target_growth = sector_target_growth.set_index(sector_index_columns)
            # # Apply conditional transformation
            # for col in future_year_columns:
            #     sector_ratio[col] = np.where(
            #         ((sector_ratio[col] > 1) & (sector_target_growth[col] > 0)),
            #         1,  # set gap to 1 to target growth is larger than estimated growth when both are positive
            #         sector_ratio[col],  # Keep original value otherwise
            #     )
            # print("sector_ratio:", sector_ratio)
            # Dlog estimated growth at lower geographical level
            zone_estimated_growth = results[f"zone_dlog_{id}"]["AbsoluteGrowth"]

            # Relative NTEM related growth at lower geographical level
            zone_target_growth = growth_calculator.target_growth(
                results[f"zone_dlog_{id}"]["YearTotal"],
                results[f"zone_ntem_{id}"]["YearTotal"],
                base_year_column,
                future_year_columns,
            )

            # print(zone_target_growth)
            # Create a list of columns for index
            zone_index_column_count = zone_target_growth.columns.get_loc(
                base_year_column
            )
            zone_index_columns = list(
                zone_target_growth.columns[:zone_index_column_count]
            )
            # print(zone_index_columns)
            zone_base_year = zone_target_growth[zone_index_columns + [base_year_column]]
            # Adjust zone_etmt growth using scaler to make sure the sector level total estimated growth won't exceed 95% of target growth
            zone_etmt_growth = zone_estimated_growth[
                zone_index_columns + future_year_columns
            ]
            # Set zonal estimtaed growth to zero if negative whilst the corresponding target growth is positive
            zone_etmt_growth = zone_etmt_growth.set_index(zone_index_columns)
            zone_target_growth = zone_target_growth.set_index(zone_index_columns)
            zone_etmt_growth[future_year_columns] = np.where(
                (zone_etmt_growth[future_year_columns] < 0)
                & (zone_target_growth[future_year_columns] >= 0),
                0,  # Set negative estimated growth to zero when target growth is positive
                zone_etmt_growth[future_year_columns],  # Keep original value otherwise
            )
            zone_etmt_growth = zone_etmt_growth.reset_index()
            zone_target_growth = zone_target_growth.reset_index()

            # Get zone scaler from sector ratio
            zone_scaler = zone_etmt_growth[zone_index_columns].copy()
            zone_scaler = zone_scaler.merge(
                sector_ratio_adjusted, on=sector_index_columns, how="left"
            )
            zone_scaler = zone_scaler[zone_index_columns + future_year_columns]
            zone_scaled_etmt_growth = calc.calculate_product(
                zone_etmt_growth,
                zone_scaler,
                future_year_columns,
                zone_index_columns,
            )

            agg_zone_scaled_etmt_growth = zone_scaled_etmt_growth.groupby(
                sector_index_columns
            )[future_year_columns].sum()
            sector_scaled_etmt_growth = sector_target_growth[
                sector_index_columns
            ].copy()
            sector_scaled_etmt_growth = sector_scaled_etmt_growth.merge(
                agg_zone_scaled_etmt_growth, on=sector_index_columns, how="left"
            )

            sector_bg_growth = calc.calculate_gap(
                sector_target_growth,
                sector_scaled_etmt_growth,
                future_year_columns,
                sector_index_columns,
            )

            # print(sector_bg_growth)

            LOG.info(
                f"Calculating weight to distribute background growth for each zone for {id}"
            )

            # calculate gap of growth between target (trend-based forecast) and estimated (from dlog) for each zone
            zone_gap_growth = calc.calculate_gap(
                zone_target_growth,
                zone_scaled_etmt_growth,
                future_year_columns,
                zone_index_columns,
            )
            zone_etmt_growth = zone_etmt_growth.rename(
                columns={col: f"{col}_etmt" for col in future_year_columns}
            )
            zone_gap_growth = zone_gap_growth.merge(
                zone_etmt_growth, on=zone_index_columns, how="left"
            )
            # Apply conditional transformation
            for col in future_year_columns:
                zone_gap_growth[col] = np.where(
                    (zone_gap_growth[col] < 0)
                    & (~np.isclose(zone_gap_growth[f"{col}_etmt"], 0, atol=1e-5)),
                    0,  # Set gap to zero when estimated growth isn't (almost) zero and gap is negative
                    zone_gap_growth[col],  # Keep original value otherwise
                )
            # zone_gap_growth["zone_count"] = zone_gap_growth.groupby(
            #     zone_index_columns[1:]
            # )[zone_id].transform("count")
            # Calculate weight to be used to distribute sector level background growth
            zone_weight = calc.calculate_zone_weights(
                zone_gap_growth,
                future_year_columns,
                zone_index_columns,
                sector_index_columns,
            )
            # Get lower geographical level background growth
            zone_bg_growth = zone_target_growth[zone_index_columns].copy()
            zone_bg_growth = zone_bg_growth.merge(
                sector_bg_growth, on=sector_index_columns, how="left"
            )
            zone_bg_growth = calc.calculate_product(
                zone_bg_growth,
                zone_weight,
                future_year_columns,
                zone_index_columns,
            )
            agg_zone_bg_growth = zone_bg_growth.groupby(sector_index_columns)[
                future_year_columns
            ].sum()
            agg_zone_bg_growth = agg_zone_bg_growth.reset_index()

            # Generating final adjusted data
            zone_adjusted_growth = calc.calculate_sum(
                zone_bg_growth,
                zone_scaled_etmt_growth,
                future_year_columns,
                zone_index_columns,
            )
            zone_adjusted_growth = zone_adjusted_growth.merge(
                zone_base_year, on=zone_index_columns, how="left"
            )
            agg_zone_adj_growth = zone_adjusted_growth.groupby(sector_index_columns)[
                future_year_columns
            ].sum()
            agg_zone_adj_growth = agg_zone_adj_growth.reset_index()

            # Create target year total
            zone_forecast = growth_calculator.yearly_totals_from_base(
                zone_adjusted_growth, base_year_column, future_year_columns
            )
            agg_zone_forecast = zone_forecast.groupby(sector_index_columns)[
                future_year_columns
            ].sum()
            agg_zone_forecast = agg_zone_forecast.reset_index()

            # Filter your dataframes before comparison for car mode only "m" == 3
            filtered_sector_target = sector_target_tot[sector_target_tot["m"] == 3]
            filtered_sector_output = agg_zone_forecast[agg_zone_forecast["m"] == 3]
            # Check sector level output against target values
            comparator = ForecastComparator(
                df1=filtered_sector_target,
                df2=filtered_sector_output,
                key_columns=sector_index_columns,
                year_columns=future_year_columns,
                df1_label="target",
                df2_label="output",
                use_tolerance=True,
                tol=1e-6,
            )

            comparator.run_comparison()

            # Calculate final adjustment factor between forecast (adjusted future year total) and estimated total to scale te growth related to large sites
            zone_adjustment_factor = calc.calculate_ratio(
                cap_ratio=1,  # no scaling
                target_data=zone_forecast,  # final future year total combined ntem and dlog
                estimated_data=results[f"zone_dlog_{id}"][
                    "YearTotal"
                ],  # original estimated future year total from dlog
                build_out_columns=future_year_columns,
                sector_index_columns=zone_index_columns,
                tolerance=1e-5,
            )
            # Check for any negative values in the specified future year columns
            negative_values_df = zone_adjustment_factor[
                zone_adjustment_factor[future_year_columns].lt(0).any(axis=1)
            ]

            if not negative_values_df.empty:
                print("Negative values found in dataframe 'zone_adjustment_factor':")
                print(negative_values_df)
            else:
                print("No negative values found in 'zone_adjustment_factor'.")

            # Get trip end growth related to large sites
            zone_ls_te_grth = dlog_te_lsgrth_dfs[f"dlog_{id}"]
            # get LAD and Region
            sector_ls_te_grth = te_cp.aggregate_to_sector_p_m(zone_ls_te_grth)

            zone_ls_te_grth = te_cp.add_names_to_te_data(
                zone_ls_te_grth, zone_id, use_sector_name=True, use_sector_cols=False
            )
            sector_ls_te_grth = te_cp.add_names_to_te_data(
                sector_ls_te_grth, sector_id, use_sector_name=True, use_sector_cols=True
            )

            zone_ls_te_grth_scaled = calc.calculate_product(
                zone_ls_te_grth,  # large site te growth
                zone_adjustment_factor,  # adjustment factor
                future_year_columns,
                zone_index_columns_initial,
            )
            zone_ls_te_grth_scaled[future_year_columns] = zone_ls_te_grth_scaled[
                future_year_columns
            ].where(zone_ls_te_grth_scaled[future_year_columns].abs() >= 1e-5, 0)

            LOG.info("Adding regions into final output dataframe")
            zone_forecast = zone_forecast.merge(
                lookup_lad_region[[sector_id, "ntem_region_id"]],
                on=sector_id,
                how="left",
            )
            # Get subset for negative trip end values
            negative_records = zone_forecast[
                (zone_forecast[future_year_columns] < 0).any(axis=1)
            ]
            region_forecast = zone_forecast.groupby(["ntem_region_id", "p", "m"])[
                [base_year_column] + future_year_columns
            ].sum()
            region_forecast = region_forecast.reset_index()

            te_output[id] = zone_forecast

            te_ls_output[id] = zone_ls_te_grth

            LOG.info("Exporting key output data")
            inter_output_path = key_te_folder / f"output_intermediate"
            inter_output_path.mkdir(exist_ok=True)
            # Files to be exported
            # sector_list = ["Eilean Siar", "Moray", "Aberdeen City"]

            # Combine data and file names into a single dictionary
            data_files_and_names = {
                "sector_target_growth": {
                    "data": sector_target_growth,
                    "file": f"{sector}_target_growth_{id}.csv",
                },
                "sector_estimated_growth": {
                    "data": sector_estimated_growth,
                    "file": f"{sector}_estimated_growth_{id}.csv",
                },
                "sector_ratio": {
                    "data": sector_ratio,
                    "file": f"{sector}_ratio_{id}.csv",
                },
                "sector_bg_growth": {
                    "data": sector_bg_growth,
                    "file": f"{sector}_background_growth_{id}.csv",
                },
                "zone_target_growth": {
                    "data": zone_target_growth,
                    "file": f"zone_target_growth_{id}.csv",
                },
                "zone_gap_growth": {
                    "data": zone_gap_growth,
                    "file": f"zone_gap_growth_{id}.csv",
                },
                "zone_weight": {
                    "data": zone_weight,
                    "file": f"zone_weight_{id}.csv",
                },
                "zone_bg_growth": {
                    "data": zone_bg_growth,
                    "file": f"zone_background_growth_{id}.csv",
                },
                "agg_zone_bg_growth": {
                    "data": agg_zone_bg_growth,
                    "file": f"agg_zone_background_growth_{id}.csv",
                },
                "zone_estimated_growth": {
                    "data": zone_estimated_growth,
                    "file": f"zone_estimated_growth_{id}.csv",
                },
                "zone_scaler": {"data": zone_scaler, "file": f"zone_scaler_{id}.csv"},
                "zone_scaled_etmt_growth": {
                    "data": zone_scaled_etmt_growth,
                    "file": f"zone_scaled_estimated_growth_{id}.csv",
                },
                "zone_adjusted_growth": {
                    "data": zone_adjusted_growth,
                    "file": f"zone_adjusted_growth_{id}.csv",
                },
                "agg_zone_adj_growth": {
                    "data": agg_zone_adj_growth,
                    "file": f"agg_zone_adjusted_growth_{id}.csv",
                },
                "sector_target_tot": {
                    "data": sector_target_tot,
                    "file": f"{sector}_target_tot_{id}.csv",
                },
                "zone_target_tot": {
                    "data": zone_target_tot,
                    "file": f"zone_target_tot_{id}.csv",
                },
                "zone_ls_te_grth": {
                    "data": zone_ls_te_grth,
                    "file": f"zone_largesite_te_growth_{id}.csv",
                },
                "sector_ls_te_grth": {
                    "data": sector_ls_te_grth,
                    "file": f"{sector}_largesite_te_growth_{id}.csv",
                },
            }

            # Check if sector_list is provided (not empty)
            if mode_list:
                # Filter dataframes based on sector_list
                for key, value in data_files_and_names.items():
                    filtered_data = value["data"][value["data"]["m"].isin(mode_list)]
                    utilities.write_to_csv(
                        inter_output_path / value["file"], filtered_data
                    )
            else:
                # Export without filtering if sector_list is empty
                for key, value in data_files_and_names.items():
                    utilities.write_to_csv(
                        inter_output_path / value["file"], value["data"]
                    )

            # Combine data and file names into a single dictionary
            key_outputs_and_names = {
                "zone_fy_tot": {
                    "data": zone_forecast,
                    "file": f"normits_tripend_fy_{id}.csv",
                },
                "zone_fy_tot_negative": {
                    "data": negative_records,
                    "file": f"normits_tripend_fy_{id}_negative.csv",
                },
                "agg_zone_future_year_tot": {
                    "data": agg_zone_forecast,
                    "file": f"normits_agg_tripend_fy_{id}.csv",
                },
                "region_forecast": {
                    "data": region_forecast,
                    "file": f"normits_region_tripend_fy_{id}.csv",
                },
                "zone_adjustment_factor": {
                    "data": zone_adjustment_factor,
                    "file": f"normits_zone_ajfactor_{id}.csv",
                },
                "zone_fy_ls_grth": {
                    "data": zone_ls_te_grth,
                    "file": f"normits_largesite_tripend_fy_{id}.csv",
                },
            }

            # Check if sector_list is provided (not empty)
            if mode_list:
                # Filter dataframes based on sector_list
                for key, value in key_outputs_and_names.items():
                    filtered_data = value["data"][value["data"]["m"].isin(mode_list)]
                    utilities.write_to_csv(key_te_folder / value["file"], filtered_data)
            else:
                # Export without filtering if sector_list is empty
                for key, value in key_outputs_and_names.items():
                    utilities.write_to_csv(key_te_folder / value["file"], value["data"])
    LOG.info("Finally scale attraction according to the production total")
    # Scale attraction according to the production total
    for cat in categories:
        prod_df = te_output[f"{cat}_prod"]
        attr_df = te_output[f"{cat}_attr"]
        # ls_prod_df = te_ls_output[f"{cat}_prod"]
        ls_attr_df = te_ls_output[f"{cat}_attr"]

        # Group by (p, m) and sum across future years
        prod_totals = prod_df.groupby(["p", "m"])[future_year_columns].sum()
        attr_totals = attr_df.groupby(["p", "m"])[future_year_columns].sum()

        # Compute scaling factor
        scaling_factor = (
            prod_totals.divide(attr_totals).replace([np.inf, -np.inf], np.nan).fillna(1)
        )
        print("Scaling factor:\n", scaling_factor)
        # Apply scaling
        attr_scaled = attr_df.copy()
        ls_attr_scaled = ls_attr_df.copy()
        # Scale the attraction values by the scaling factor
        for year in future_year_columns:
            attr_scaled[year] = attr_scaled.apply(
                lambda row: row[year] * scaling_factor.loc[(row["p"], row["m"]), year],
                axis=1,
            )
            ls_attr_scaled[year] = ls_attr_scaled.apply(
                lambda row: row[year] * scaling_factor.loc[(row["p"], row["m"]), year],
                axis=1,
            )
        # Make sure the attraction values of larges sites are not bigger than those of total
        ls_attr_scaled = ls_attr_scaled.set_index(zone_index_columns_initial)
        attr_scaled = attr_scaled.set_index(zone_index_columns_initial)
        ls_attr_scaled_aj = ls_attr_scaled.copy()

        # Apply the condition safely with proper parentheses
        condition = (
            ls_attr_scaled[future_year_columns] > attr_scaled[future_year_columns]
        ) & (ls_attr_scaled[future_year_columns] > 0)

        ls_attr_scaled_aj[future_year_columns] = ls_attr_scaled[
            future_year_columns
        ].where(
            condition,
            attr_scaled[future_year_columns],
        )
        # Reset index to original structure if needed
        attr_scaled = attr_scaled.reset_index()
        ls_attr_scaled = ls_attr_scaled.reset_index()
        ls_attr_scaled_aj = ls_attr_scaled_aj.reset_index()
        # Filter for m == 3
        filtered_attr_scaled = attr_scaled[attr_scaled["m"].isin(mode_list)]
        filtered_ls_attr_scaled = ls_attr_scaled[ls_attr_scaled["m"].isin(mode_list)]

        # Write to CSV
        LOG.info(f"Writing scaled tripends for {cat} category")
        utilities.write_to_csv(
            key_te_folder / f"normits_tripend_fy_{cat}_attr_scaled.csv",
            filtered_attr_scaled,
        )
        utilities.write_to_csv(
            key_te_folder / f"normits_largesite_tripend_fy_{cat}_attr_scaled.csv",
            filtered_ls_attr_scaled,
        )

    LOG.info("Data processing completed")
