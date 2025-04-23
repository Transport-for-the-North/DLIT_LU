import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, List
from pathlib import Path

# Local imports
from dlit_lu import inputs, dev_pattern, constraint, utilities
from caf.base.data_structures import DVector
import caf.base as cb

LOG = logging.getLogger(__name__)


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
        self.base_year_column = config.dev_pattern.base_year
        self.future_year_columns = [str(year) for year in config.tripend.future_years]
        # Validate sector
        if self.sector not in self.sector_info_map:
            raise ValueError(f"Unsupported sector: {self.sector}")

        # Retrieve sector-specific metadata from predefined SECTOR_INFO_MAP
        self.sector_info: Dict[str, Any] = self.sector_info_map[self.sector]

        # Initialize the BaseZoneHandler for geo_boundary and other zone info
        self.base_zone_handler = dev_pattern.BaseZoneHandler(config)
        # Access zone information based on geo_boundary
        self.zone_info = self.base_zone_handler.zone_info
        # Example: If you want to update other sector-specific information
        self.sector_info["lookup_path"] = self.zone_info[
            "zone_to_lad_path"
        ]  # config.dev_pattern.summary_data.normits_to_lad_file
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
        # elif model_zone == inputs.GeoBoundary.LSOA.value:
        #    return pd.read_csv(self.config.tripend.normits_to_lsoa)
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

        val_cols = [base_year_column] + future_year_columns
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


class ByTeTransformation:
    def __init__(self, df: pd.DataFrame, base_year_column: str):
        """
        Initialize the class with the given DataFrame and base year.

        Parameters
        ----------
        df : pd.DataFrame
            Input DataFrame with mode and period columns.
        base_year : int
            The base year to rename "prod" and "attr".
        """
        self.df = df.copy()

        # Exclude unwanted categories for mode and period
        self.df = self.df[~self.df["mode"].isin([7])]  # Exclude mode category 7 for Air
        self.df = self.df[
            self.df["period"].isin([1, 2, 3, 4])
        ]  # Keep only period categories 1, 2, 3, and 4
        self.df["mode"] = self.df["mode"].replace(
            {4: 3}
        )  # Merge 4 into 3 for mode to combine car driver and car passenger
        self.base_year_column = base_year_column

    def groupby_sum(self, val: str):
        """
        Group the DataFrame by "normits_v3.3_id", "purpose", "mode", and "period",
        summing the specified value column.

        Parameters
        ----------
        val : str
            The column to be summed, either "prod" or "attr".

        Returns
        -------
        pd.DataFrame
            Grouped DataFrame with summed values.
        """
        if val not in self.df.columns:
            raise ValueError(f"Column '{val}' not found in DataFrame")

        grouped_df = self.df.groupby(
            ["normits_v3.3_id", "purpose", "mode"], as_index=False
        )[val].sum()
        # Divide the summed value by 5 to get average week day totals
        grouped_df[val] = grouped_df[val] / 5
        # Rename columns
        grouped_df = grouped_df.rename(
            columns={
                "purpose": "p",
                "mode": "m",
                val: self.base_year_column,  # Rename "prod" or "attr" to base year
            }
        )

        return grouped_df


def run(config: inputs.DLitConfig):
    """Main function to run the Tripends processing."""

    if config.run_tripend is None:
        raise ValueError("Cannot run tripend module without parameters")

    LOG.info("Initializing Tripend Module")
    config.output_folder.mkdir(exist_ok=True)
    base_year = int(config.dev_pattern.base_year)
    base_year_column = str(base_year)
    future_years = config.tripend.future_years
    future_year_columns = [str(year) for year in future_years]
    future_year_column = str(2024)
    cap_ratio = config.constraint.cap_ratio
    key_te_folder = config.output_folder / "07_tripend"
    key_te_folder.mkdir(exist_ok=True)
    model_zone = config.dev_pattern.geo_boundary.value

    LOG.info("Instantiating TEConstraintProcessor")
    te_cp = TEConstraintProcessor(config)
    zone_translation = te_cp.load_zone_translation(model_zone)
    sector_info = te_cp.sector_info
    zone_id = sector_info["zone_id"]
    sector_id = sector_info["sector_id"]
    sector = config.tripend.sector.value

    LOG.info(
        "Instantiating GrowthCalculator and ConstraintCalculation from constraint module"
    )
    growth_calculator = constraint.GrowthCalculator()
    calc = constraint.ConstraintCalculation()
    LOG.info("Loading and transforming base year trip end data")
    # Load datasets
    by_datasets = {
        "by_hb": pd.read_csv(config.tripend.by_fr_hb),
        "by_hb_to": pd.read_csv(config.tripend.by_to_hb),
        "by_nhb": pd.read_csv(config.tripend.by_nhb),
    }

    # Process each dataset
    by_zone_te = {}
    summary_by_zone_te = []  # Initialize summary table outside the loop
    for name, df in by_datasets.items():
        total_prod_before = df[
            "prod"
        ].sum()  # Divide by 5 to get average week day totals
        total_attr_before = df[
            "attr"
        ].sum()  # Divide by 5 to get average week day totals
        transformer = ByTeTransformation(df, base_year_column)

        # Store transformed DataFrame
        by_zone_te[name] = {
            "total_prod_before": total_prod_before,
            "total_attr_before": total_attr_before,
            "prod": transformer.groupby_sum("prod"),
            "attr": transformer.groupby_sum("attr"),
        }

    # Summary table to check total before and after transformation
    for name in by_zone_te.keys():
        # Get total sum from aggregated "prod" and "attr"
        total_prod = by_zone_te[name]["prod"][base_year_column].sum()
        total_attr = by_zone_te[name]["attr"][base_year_column].sum()

        # Store summary data
        summary_by_zone_te.append(
            {
                "dataset": name,
                "total_prod_beforetrans": by_zone_te[name]["total_prod_before"],
                "total_attr_beforetrans": by_zone_te[name]["total_attr_before"],
                "total_prod": total_prod,
                "total_attr": total_attr,
            }
        )

    # Convert summary data to DataFrame
    summary_by_df = pd.DataFrame(summary_by_zone_te)
    utilities.write_to_csv(key_te_folder / "by_te_summary.csv", summary_by_df)
    utilities.write_to_csv(
        key_te_folder / "by_hb_production.csv", by_zone_te["by_hb"]["prod"]
    )
    utilities.write_to_csv(
        key_te_folder / "by_hb_attraction.csv", by_zone_te["by_hb"]["attr"]
    )
    # Check the shape of the by_zone_te
    print(by_zone_te["by_hb"]["prod"].shape)
    print(by_zone_te["by_hb"]["attr"].shape)

    # # Rename key "by_hb_fr" to "by_hb"
    # if "by_hb_fr" in by_zone_te:
    #     by_zone_te["by_hb"] = by_zone_te.pop("by_hb_fr")

    # Get the index columns for the zone data
    zone_index_column_count = by_zone_te["by_hb"]["prod"].columns.get_loc(
        base_year_column
    )

    zone_index_columns = list(
        by_zone_te["by_hb"]["prod"].columns[:zone_index_column_count]
    )

    LOG.info("Generating or Loading Dlog trip end data")

    # Load data into a dictionary

    # Load datasets
    dlog_datasets = {
        "fy_hb": pd.read_csv(config.tripend.fy_fr_hb),
        "fy_hb_to": pd.read_csv(config.tripend.fy_to_hb),
        "fy_nhb": pd.read_csv(config.tripend.fy_nhb),
    }

    dlog_grth_datasets = {
        "fy_grth_hb": pd.read_csv(config.tripend.fy_grth_fr_hb),
        "fy_grth_hb_to": pd.read_csv(config.tripend.fy_grth_to_hb),
        "fy_grth_nhb": pd.read_csv(config.tripend.fy_grth_nhb),
    }

    # Process each dataset
    dlog_zone_te = {}
    summary_dlog_zone_te = []  # Initialize summary table outside the loop
    for name, df in dlog_datasets.items():
        total_prod_before = df[
            "prod"
        ].sum()  # Divide by 5 to get average week day totals
        total_attr_before = df[
            "attr"
        ].sum()  # Divide by 5 to get average week day totals
        transformer = ByTeTransformation(df, future_year_column)

        # Store transformed DataFrame
        dlog_zone_te[name] = {
            "total_prod_before": total_prod_before,
            "total_attr_before": total_attr_before,
            "prod": transformer.groupby_sum("prod"),
            "attr": transformer.groupby_sum("attr"),
        }

    # Summary table to check total before and after transformation
    for name in dlog_zone_te.keys():
        # Get total sum from aggregated "prod" and "attr"
        total_prod = dlog_zone_te[name]["prod"][future_year_column].sum()
        total_attr = dlog_zone_te[name]["attr"][future_year_column].sum()

        # Store summary data
        summary_dlog_zone_te.append(
            {
                "dataset": name,
                "total_prod_beforetrans": dlog_zone_te[name]["total_prod_before"],
                "total_attr_beforetrans": dlog_zone_te[name]["total_attr_before"],
                "total_prod": total_prod,
                "total_attr": total_attr,
            }
        )

    # Convert summary data to DataFrame
    summary_dlog_df = pd.DataFrame(summary_dlog_zone_te)
    utilities.write_to_csv(key_te_folder / "dlog_te_summary.csv", summary_dlog_df)
    utilities.write_to_csv(
        key_te_folder / "dlog_hb_production.csv", dlog_zone_te["fy_hb"]["prod"]
    )
    utilities.write_to_csv(
        key_te_folder / "dlog_hb_attraction.csv", dlog_zone_te["fy_hb"]["attr"]
    )
    # Check the shape of the by_zone_te
    print(dlog_zone_te["fy_hb"]["prod"].shape)
    print(dlog_zone_te["fy_hb"]["attr"].shape)

    # Process each dataset
    dlog_zone_te_grth = {}
    summary_dlog_zone_te_grth = []  # Initialize summary table outside the loop
    for name, df in dlog_grth_datasets.items():
        total_prod_before = df[
            "prod"
        ].sum()  # Divide by 5 to get average week day totals
        total_attr_before = df[
            "attr"
        ].sum()  # Divide by 5 to get average week day totals
        transformer = ByTeTransformation(df, future_year_column)

        transformer = ByTeTransformation(df, future_year_column)

        # Store transformed DataFrame
        dlog_zone_te_grth[name] = {
            "total_prod_before": total_prod_before,
            "total_attr_before": total_attr_before,
            "prod": transformer.groupby_sum("prod"),
            "attr": transformer.groupby_sum("attr"),
        }
    # Summary table to check total before and after transformation
    for name in dlog_zone_te_grth.keys():
        # Get total sum from aggregated "prod" and "attr"
        total_prod = dlog_zone_te_grth[name]["prod"][future_year_column].sum()
        total_attr = dlog_zone_te_grth[name]["attr"][future_year_column].sum()

        # Store summary data
        summary_dlog_zone_te_grth.append(
            {
                "dataset": name,
                "total_prod_beforetrans": dlog_zone_te_grth[name]["total_prod_before"],
                "total_attr_beforetrans": dlog_zone_te_grth[name]["total_attr_before"],
                "total_prod": total_prod,
                "total_attr": total_attr,
            }
        )

    # Convert summary data to DataFrame
    summary_dlog_grth_df = pd.DataFrame(summary_dlog_zone_te_grth)
    utilities.write_to_csv(
        key_te_folder / "dlog_te_grth_summary.csv", summary_dlog_grth_df
    )
    utilities.write_to_csv(
        key_te_folder / "dlog_hb_production_grth.csv",
        dlog_zone_te_grth["fy_grth_hb"]["prod"],
    )
    utilities.write_to_csv(
        key_te_folder / "dlog_hb_attraction_grth.csv",
        dlog_zone_te_grth["fy_grth_hb"]["attr"],
    )

    # dlog_zone_te = {
    #     key: df.rename(columns={"zone": "normits_v3.3_id"})
    #     for key, df in {
    #         "dlog_hb_prod": pd.read_csv(config.tripend.dlog_hb_prod),
    #         "dlog_hb_attr": pd.read_csv(config.tripend.dlog_hb_attr),
    #         "dlog_nhb_prod": pd.read_csv(config.tripend.dlog_nhb_prod),
    #         "dlog_nhb_attr": pd.read_csv(config.tripend.dlog_nhb_attr),
    #     }.items()
    # }
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

    # Lists of categories and data types
    categories = ["hb"]  # ["hb", "nhb"]
    te = ["prod", "attr"]
    sources = ["dlog", "ntem"]

    LOG.info("Combining Dlog fy data (or fy growth) with base year data")
    # Initialize an empty dictionary to hold the processed data
    dlog_zone_te_processed = {}

    for category in categories:
        for data_type in te:
            # Merge zone data for each category and data type
            merged_data = te_cp.merge_zone_data(
                by_zone_te[f"by_{category}"][data_type],
                # dlog_zone_te_grth[f"fy_grth_{category}"][data_type],
                dlog_zone_te[f"fy_{category}"][data_type],
                # dlog_zone_te[f"dlog_{category}_{data_type}"],
                zone_index_columns,
            )
            # Fill NaN values with 0 in merged_data before further processing
            merged_data = merged_data.fillna(0)

            # # Apply yearly totals for the merged data
            # processed_data_type = growth_calculator.yearly_totals_from_base(
            #     merged_data,
            #     base_year_column,
            #     future_year_columns,
            # )

            # # Fill NaN values with 0 in processed_data_type after yearly totals
            # processed_data_type = processed_data_type.fillna(0)

            # # Store the processed data in the dictionary
            # dlog_zone_te_processed[f"dlog_{category}_{data_type}"] = processed_data_type
            # Store the processed data in the dictionary
            dlog_zone_te_processed[f"dlog_{category}_{data_type}"] = merged_data

    LOG.info("Aggregating zonal data to sector level")
    zone_data_sets = {}
    for source in sources:
        for category in categories:
            for data_type in te:
                key = f"{source}_{category}_{data_type}"

                if source == "dlog":
                    # Access dictionary variable, not a formatted string
                    zone_data_sets[key] = dlog_zone_te_processed[key]
                else:
                    zone_data_sets[key] = ntem_zone_te[key]

    # Check the shape of the by_zone_te
    print(zone_data_sets["dlog_hb_prod"])
    print(zone_data_sets["dlog_hb_attr"])
    print(zone_data_sets["ntem_hb_prod"])
    print(zone_data_sets["ntem_hb_attr"])

    sector_data_sets = {}
    for key, df in zone_data_sets.items():
        sector_data_sets[key] = te_cp.aggregate_to_sector_p_m(
            df,
        )
    # Check the shape of the sector dataframe
    print(sector_data_sets["dlog_hb_prod"])
    print(sector_data_sets["dlog_hb_attr"])

    LOG.info("Inserting name of sector to the data")
    # Define dataset mappings dynamically by looping through pop and emp keys
    datasets_process = []
    # Define geographies dynamically
    geographies = [("zone", zone_data_sets), (sector, sector_data_sets)]

    # Create datasets_process dynamically for zone and Sector data
    for geo, data_dict in geographies:
        for source in sources:
            for category in categories:
                for data_type in te:
                    key = f"{source}_{category}_{data_type}"
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
            ),
            "GrowthRate": growth_calculator.calculate_growth(
                processed_data,
                base_year_column,
                future_year_columns,
                growth_type="rate",
            ),
            "AnnualGrowthRate": growth_calculator.calculate_annual_growth_rate(
                processed_data, future_year_columns
            ),
        }

    # Export datasets for visualization
    export_for_viz = config.tripend.export_for_viz
    if export_for_viz:
        LOG.info("Export datasets for LAD and Aggregated LAD")
        # Define the list of locations to filter
        # sector_list = ["Bury", "Manchester", "Oldham", "Rochdale",
        #             "Salford", "Stockport", "Tameside", "Trafford"]
        sector_list = ["Bury", "Manchester"]
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
                    for data_type in te:
                        id = f"{category}_{data_type}"
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
                                    if sector_list:
                                        # Filter DataFrame to include only rows where sector name is in sector_list
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

    LOG.info(f"Constraining dlog data with ntem data")
    for category in categories:
        for data_type in te:
            id = f"{category}_{data_type}"

            LOG.info(f"Working out the background growth at sector level for {id}")

            sector_target_tot = growth_calculator.target_yeartot(
                results[f"{sector}_dlog_{id}"]["YearTotal"],
                results[f"{sector}_ntem_{id}"]["YearTotal"],
                base_year_column,
                future_year_columns,
            )
            zone_target_tot = growth_calculator.target_yeartot(
                results[f"zone_dlog_{id}"]["YearTotal"],
                results[f"zone_ntem_{id}"]["YearTotal"],
                base_year_column,
                future_year_columns,
            )

            sector_target_growth = growth_calculator.target_growth(
                results[f"{sector}_dlog_{id}"]["YearTotal"],
                results[f"{sector}_ntem_{id}"]["YearTotal"],
                base_year_column,
                future_year_columns,
            )
            print(sector_target_growth)
            sector_estimated_growth = results[f"{sector}_dlog_{id}"]["AbsoluteGrowth"]

            sector_index_column_count = sector_target_growth.columns.get_loc(
                base_year_column
            )
            sector_index_columns = list(
                sector_target_growth.columns[:sector_index_column_count]
            )
            print(sector_index_columns)

            sector_ratio = calc.calculate_ratio(
                cap_ratio,
                sector_target_growth,
                sector_estimated_growth,
                future_year_columns,
                sector_index_columns,
            )
            # Apply conditional transformation
            for col in future_year_columns:
                sector_ratio[col] = np.where(
                    (sector_ratio[col] > 1),
                    1,  # set gap to zero to avoid additional background growth when estimated growth is not zero and the gap is negative (estimated exceeds target)
                    sector_ratio[col],  # Keep original value otherwise
                )
            print("sector_ratio:", sector_ratio)
            # Dlog estimated growth at lower geographical level
            zone_estimated_growth = results[f"zone_dlog_{id}"]["AbsoluteGrowth"]

            # Relative NTEM related growth at lower geographical level
            zone_target_growth = growth_calculator.target_growth(
                results[f"zone_dlog_{id}"]["YearTotal"],
                results[f"zone_ntem_{id}"]["YearTotal"],
                base_year_column,
                future_year_columns,
            )
            print(zone_target_growth)
            # Create a list of columns for index
            zone_index_column_count = zone_target_growth.columns.get_loc(
                base_year_column
            )
            zone_index_columns = list(
                zone_target_growth.columns[:zone_index_column_count]
            )
            print(zone_index_columns)
            zone_base_year = zone_target_growth[zone_index_columns + [base_year_column]]
            # Adjust zone_etmt growth using scaler to make sure the sector level total estimated growth won't exceed 95% of target growth
            zone_etmt_growth = zone_estimated_growth[
                zone_index_columns + future_year_columns
            ]

            # Get zone scaler from sector gratio
            zone_scaler = zone_etmt_growth[zone_index_columns].copy()
            zone_scaler = zone_scaler.merge(
                sector_ratio, on=sector_index_columns, how="left"
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
                f"Calculating weight to distribute background growth for each ZONE for {id}"
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
                    (zone_gap_growth[col] < 0) & (zone_gap_growth[f"{col}_etmt"] != 0),
                    0,  # set gap to zero to avoid additional background growth when estimated growth is not zero and the gap is negative (estimated exceeds target)
                    zone_gap_growth[col],  # Keep original value otherwise
                )

            # calculate weight to be used to distribute sector level background growth
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
            agg_zone_adj_growth = zone_adjusted_growth.groupby(f"{sector}nm")[
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

            LOG.info("Exporting key output data")
            # Files to be exported
            sector_list = ["Bury", "Manchester"]

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
                "zone_forecast": {
                    "data": zone_forecast,
                    "file": f"zone_forecast_{id}.csv",
                },
                "agg_zone_forecast": {
                    "data": agg_zone_forecast,
                    "file": f"agg_zone_forecast_{id}.csv",
                },
                "sector_target_tot": {
                    "data": sector_target_tot,
                    "file": f"{sector}_target_tot_{id}.csv",
                },
                "zone_target_tot": {
                    "data": zone_target_tot,
                    "file": f"zone_target_tot_{id}.csv",
                },
            }

            # Check if sector_list is provided (not empty)
            if sector_list:
                # Filter dataframes based on sector_list
                for key, value in data_files_and_names.items():
                    filtered_data = value["data"][
                        value["data"][f"{sector}nm"].isin(sector_list)
                    ]
                    utilities.write_to_csv(key_te_folder / value["file"], filtered_data)
            else:
                # Export without filtering if sector_list is empty
                for key, value in data_files_and_names.items():
                    utilities.write_to_csv(key_te_folder / value["file"], value["data"])

    LOG.info("Data processing completed")
