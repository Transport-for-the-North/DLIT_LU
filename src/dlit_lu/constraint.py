import pandas as pd
import logging
from typing import Dict, Any
import numpy as np

# Local imports
from dlit_lu import inputs, utilities
from dlit_lu.viz import GrowthRateVisualizer

LOG = logging.getLogger(__name__)

class ConstraintProcessor():
    """
    A class to process constraints for geographical aggregations.

    Attributes:
        sector (inputs.Sector): The sector type defined in the configuration.
        config (inputs.DLitConfig): The configuration settings for constraints.
        sector_info_map (Dict[inputs.Sector, Dict[str, Any]]): Mapping of sector types to their corresponding metadata.
        sector_info (Dict[str, Any]): The sector metadata for the selected sector.
    """
    def __init__(self, config: inputs.DLitConfig) -> None:
        """
        Initializes the ConstraintProcessor with configuration settings.

        Args:
            config (inputs.DLitConfig): The configuration object containing constraint details.
        """
        self.sector: inputs.Sector = config.constraint.sector
        self.config: inputs.DLitConfig = config
        self.sector_info_map: Dict[inputs.Sector, Dict[str, Any]] = {
            inputs.Sector.REGION: {
                "lookup_path": self.config.constraint.lad_to_region_file,
                "zone_id": "lad2013_id",
                "base_year_column": self.config.constraint.base_year,
                "sector_id": "ntem_region_id",  
                "zone_to_sector_prop_col": "lad2013_to_ntem_region",
                "sector_name": self.config.constraint.region_name
            }
        }
        # print(f"Sector: {self.sector}")
        # print(f"Available Keys: {self.sector_info_map.keys()}")
        self.sector_info: Dict[str, Any] = self.sector_info_map[self.sector]

    def sector_agg(self,
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
        lookup_path = self.sector_info["lookup_path"]  
        zone_id = self.sector_info["zone_id"]
        base_year_column = self.sector_info["base_year_column"]
        sector_id = self.sector_info["sector_id"]
        zone_to_sector_prop_col = self.sector_info["zone_to_sector_prop_col"]

        sector_data_annual = self.aggregate_to_sector(
            data,
            lookup_path,
            zone_id,
            base_year_column,
            future_year_columns,
            sector_id,
            zone_to_sector_prop_col
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
        zone_to_sector_prop_col: str
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

        merged_df = data.merge(lookup_df[[zone_id, sector_id, zone_to_sector_prop_col]], on=zone_id, how='left')
        
        for col in val_cols:
            merged_df[col] = merged_df[col] * merged_df[zone_to_sector_prop_col]

        sector_agg = merged_df.groupby(sector_id, as_index=False)[val_cols].sum()
        
        totals_after = {col: sector_agg[col].sum() for col in val_cols}

        for col in val_cols:
            if not np.all(pd.np.isclose(totals_before[col], totals_after[col])):
                raise ValueError(f"Total of '{col}' changed after aggregation: before={totals_before[col]}, after={totals_after[col]}")

        return sector_agg

    def add_names_to_data(
        self, 
        data: pd.DataFrame,
        id_column: str,
        name_column: str,
        use_sector_name: bool = False,
        use_sector_cols: bool = False
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
            data_with_name = data.merge(name_df, left_on=id_column, right_on='zone_id', how='left')
            data_with_name = data_with_name.drop(columns=['zone_id']) 
            data_with_name = data_with_name.rename(columns={id_column: 'REGIONCD', 'zone_name': 'REGIONNM'})
            columns = list(data_with_name.columns)
            columns.remove('REGIONNM')
            columns.insert(1, 'REGIONNM')  
            data_with_name = data_with_name[columns]
        else:
            data_with_name = data.merge(name_df, left_on=id_column, right_on='zone_name', how='left')
            data_with_name = data_with_name.drop(columns=['zone_id', 'zone_name'])
            data_with_name = data_with_name.rename(columns={id_column: 'LAD13CD', 'descriptions': 'LADNM'})
            columns = list(data_with_name.columns)
            columns.remove('LADNM')
            columns.insert(1, 'LADNM')
            data_with_name = data_with_name[columns]

            # Add region names to LAD data
            if 'LAD13CD' in data_with_name.columns:
                lad_to_region_df = pd.read_csv(self.config.dev_pattern.summary_data.lad_to_region_file)
                region_name_df = pd.read_csv(self.config.constraint.region_name)
                data_with_name = data_with_name.merge(lad_to_region_df[['lad2013_id', 'ntem_region_id']], 
                                                  left_on='LAD13CD', right_on='lad2013_id', how='left')
                data_with_name = data_with_name.merge(region_name_df[['zone_id', 'zone_name']], 
                                                  left_on='ntem_region_id', right_on='zone_id', how='left')
                data_with_name = data_with_name.rename(columns={'zone_name': 'REGIONNM'})
                data_with_name = data_with_name.drop(columns=['lad2013_id', 'ntem_region_id', 'zone_id'])
                cols = list(data_with_name.columns)
                cols.insert(cols.index('LADNM') + 1, cols.pop(cols.index('REGIONNM')))
                data_with_name = data_with_name[cols]

        LOG.info(f"Added {name_column} to dataset with shape {data_with_name.shape}")

        return data_with_name


class GrowthCalculator:

    def calculate_growth(self, data, base_year_int, build_out_columns, growth_type='absolute') -> pd.DataFrame:
        """
        General method to calculate absolute growth, growth ratio, or growth rate for each year.

        Parameters
        ----------
        data : pd.DataFrame
            DataFrame containing year columns.
        base_year_int : int
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
        valid_growth_types = {'absolute', 'ratio', 'rate'}
        if growth_type not in valid_growth_types:
            raise ValueError(
                f"Invalid growth_type '{growth_type}'. Choose from {valid_growth_types}."
            )

        base_year = str(base_year_int)
        growth_funcs = {
            'absolute': lambda future_year: data[future_year] - data[base_year],
            'ratio': lambda future_year: data[future_year] / data[base_year],
            'rate': lambda future_year: (data[future_year] / data[base_year]) - 1
        }

        growth = data.copy()
        # growth = growth.reset_index(drop=True)
        # Dynamically get the index columns count before the "source" column
        index_column_count = growth.columns.get_loc(base_year)
        # Determine the columns to be used for the index based on the index_column_count
        index_columns = list(growth.columns[:index_column_count])

        # # Set specified columns as index for both DataFrames
        # growth_indexed = growth.set_index(index_columns)
        for future_year in build_out_columns:
            growth[future_year] = growth_funcs[growth_type](future_year)

        return growth[index_columns + build_out_columns]

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
        growth_rate = data.copy()
        year_columns = sorted([int(year) for year in year_columns])
        cagr_columns = []

        for i in range(1, len(year_columns)):
            start_year = year_columns[i - 1]
            end_year = year_columns[i]
            years = end_year - start_year

            def calculate_row_growth(row):
                start_value = row[str(start_year)]
                end_value = row[str(end_year)]
                return None if start_value == 0 else ((end_value / start_value) ** (1 / years) - 1) * 100

            cagr_column_name = str(end_year)
            growth_rate[cagr_column_name] = data.apply(calculate_row_growth, axis=1)
            cagr_columns.append(cagr_column_name)

        result = growth_rate.iloc[:, [0, 1, 2, 3] + [growth_rate.columns.get_loc(col) for col in cagr_columns]]
        return result    



    def target_growth(self, data_base, data_source, base_year_int, build_out_columns):
        """
        Calculate target growth using original data.

        Parameters
        ----------
        data_base : pd.DataFrame
            DataFrame containing the base year total and dlog future year annual total.
        data_source : pd.DataFrame
            DataFrame containing the source annual total.
        base_year_int : int
            The base year for calculations.
        build_out_columns : list
            List of future year columns to calculate target growth.
        index_column_count : int
            The number of columns to be used for the index (e.g., 2 or 3).
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
        if list(data_base[index_columns].columns) != list(data_source[index_columns].columns):
            raise ValueError(f"Mismatch in column names specified for index between data_base and data_source.")

        # Set specified columns as index for both DataFrames
        data_base_indexed = data_base.copy()
        data_base_indexed = data_base_indexed.set_index(index_columns)
        data_source_indexed = data_source.copy()
        data_source_indexed = data_source_indexed.set_index(index_columns)

        # Sort both by index to avoid row-order mismatches
        data_base_indexed = data_base_indexed.sort_index()
        data_source_indexed = data_source_indexed.sort_index()

        # Perform intersection of indices (rows that exist in both data_base and data_source)
        common_index = data_base_indexed.index.intersection(data_source_indexed.index)

        # Filter both dataframes to keep only the rows that exist in both
        data_base_indexed = data_base_indexed.loc[common_index]
        data_source_indexed = data_source_indexed.loc[common_index]

        # Extract base year data (keeping specified columns as index + base year values)
        base_year = str(base_year_int)
        target_growth = data_base_indexed[[base_year]].copy()

        # Calculate growth rates using _calculate_growth
        growth_result = self.calculate_growth(data_source_indexed, base_year_int, build_out_columns, growth_type='rate')

        # Multiply base year values by (1 + growth rate) to get future values
        for year in build_out_columns:
            target_growth[year] = target_growth[base_year] * growth_result[year]

        # Reset index so output matches original data format
        target_growth = target_growth.reset_index()

        return target_growth
    

    def target_yeartot(self, data_base, data_source, base_year_int, build_out_columns):
        """
        Calculate target growth using original data.

        Parameters
        ----------
        data_base : pd.DataFrame
            DataFrame containing the base year data and dlog annual total.
        data_source : pd.DataFrame
            DataFrame containing the source annual total.
        base_year_int : int
            The base year for calculations.
        build_out_columns : list
            List of future year columns to calculate target growth.
        index_column_count : int
            The number of columns to be used for the index (e.g., 2 or 3).
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
        if list(data_base[index_columns].columns) != list(data_source[index_columns].columns):
            raise ValueError(f"Mismatch in column names specified for index between data_base and data_source.")

        # Set specified columns as index for both DataFrames
        data_base_indexed = data_base.copy()
        data_base_indexed = data_base_indexed.set_index(index_columns)
        data_source_indexed = data_source.copy()
        data_source_indexed = data_source_indexed.set_index(index_columns)

        # Sort both by index to avoid row-order mismatches
        data_base_indexed = data_base_indexed.sort_index()
        data_source_indexed = data_source_indexed.sort_index()

        # Perform intersection of indices (rows that exist in both data_base and data_source)
        common_index = data_base_indexed.index.intersection(data_source_indexed.index)

        # Filter both dataframes to keep only the rows that exist in both
        data_base_indexed = data_base_indexed.loc[common_index]
        data_source_indexed = data_source_indexed.loc[common_index]

        # Extract base year data (keeping specified columns as index + base year values)
        base_year = str(base_year_int)
        target = data_base_indexed[[base_year]].copy()

        # Calculate growth rates using _calculate_growth
        growth_result = self.calculate_growth(data_source_indexed, base_year_int, build_out_columns, growth_type='ratio')

        # Multiply base year values by (1 + growth rate) to get future values
        for year in build_out_columns:
            target[year] = target[base_year] * growth_result[year]

        # Reset index so output matches original data format
        target = target.reset_index()

        return target
    
    def cumulative_yearly_totals(
        self,
        data: pd.DataFrame,
        base_year_column : str,
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

        return updated_data
        
def run(config: inputs.DLitConfig):

    if config.constraint is None:
        raise ValueError("Cannot run constraint module without any constraint parameters")

    LOG.info("Initialising Constraint Module")

    config.output_folder.mkdir(exist_ok=True)

    key_constraint_path = config.output_folder / f"06_constraint"
    key_constraint_path.mkdir(exist_ok=True)

    LOG.info("Loading Key Inputs")

    sector = config.constraint.sector.value
    # Load data into a dictionary
    lad_data = {
        "ddg_pop": pd.read_csv(config.constraint.ddg_pop),
        "ddg_emp": pd.read_csv(config.constraint.ddg_emp),
        "dlog_pop": pd.read_csv(config.constraint.dlog_pop),
        "dlog_emp": pd.read_csv(config.constraint.dlog_emp),
        "ntem_pop" : pd.read_csv(config.constraint.ntem_pop),
        "ntem_emp" : pd.read_csv(config.constraint.ntem_emp)
    }

    # Process data
    year_columns = [col for col in lad_data["ntem_pop"].columns if col.isdigit()]
    base_year_column = config.constraint.base_year
    base_year_int = int(base_year_column)
    build_out_columns = np.arange(base_year_int + 1, 2062, 1).tolist()
    build_out_columns = [str(year) for year in build_out_columns]

    ddg_col = 'LAD13CD'

    # Process data: Rename columns, filter, and drop "LON" rows
    for key in ["ddg_pop", "ddg_emp"]:
        df = lad_data[key]
        df.drop(columns=[col for col in df.columns if col not in [ddg_col] + year_columns], inplace=True)
        df.rename(columns={ddg_col: "lad2013_id"}, inplace=True)
        df.drop(df[df["lad2013_id"].str.startswith("LON")].index, inplace=True)
        # Sort the DataFrame by 'lad2013_id' (you can replace 'lad2013_id' with 'ddg_col' if needed)
        df.sort_values(by="lad2013_id", ascending=True, inplace=True)
        
        lad_data[key] = df  # Save back to the dictionary


    # Instantiate the ConstraintProcessor using the provided config
    processor = ConstraintProcessor(config)
    growth_calculator = GrowthCalculator()  

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
        "ddg_pop": processor.sector_agg(lad_data["ddg_pop"], base_year_column, build_out_columns),
        "ddg_emp": processor.sector_agg(lad_data["ddg_emp"], base_year_column, build_out_columns),
        "dlog_pop": processor.sector_agg(lad_data["dlog_pop"], base_year_column, build_out_columns),
        "dlog_emp": processor.sector_agg(lad_data["dlog_emp"], base_year_column, build_out_columns),
        "ntem_pop": processor.sector_agg(lad_data["ntem_pop"], base_year_column, build_out_columns),
        "ntem_emp": processor.sector_agg(lad_data["ntem_emp"], base_year_column, build_out_columns),
    }
    LOG.info("Process to get datasets for LAD and Region completed")
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
                data_dict[key]['source'] = source  
                # Find the index of the base_year_column
                base_year_index = data_dict[key].columns.get_loc(base_year_column)
                
                # Insert "source" column before base_year_column
                data_dict[key].insert(base_year_index, 'source', data_dict[key].pop('source'))

                datasets_process.append((f"{geo}_{key}", data_dict[key], 
                                        zone_id if geo == "lad" else sector_id, 
                                        name_column if geo == "lad" else name_column_sector, 
                                        False if geo == "lad" else True, 
                                        False if geo == "lad" else True))


    # Initialize results dictionary to store processed data

    results = {}
    # Process each dataset with add_names_to_data
    for data_name, data, id_col, name_col, use_sector_name, use_sector_cols in datasets_process:
        processed_data = processor.add_names_to_data(
            data, id_col, name_col, use_sector_name=use_sector_name, use_sector_cols=use_sector_cols
        )
        # Add processed data to results, using data_name as the key
        results[data_name] = {
            "YearTotal": processed_data,
            "AbsoluteGrowth": growth_calculator.calculate_growth(processed_data, base_year_int, build_out_columns, growth_type='absolute'),
            "GrowthRatio": growth_calculator.calculate_growth(processed_data, base_year_int, build_out_columns, growth_type='ratio'),
            "GrowthRate": growth_calculator.calculate_growth(processed_data, base_year_int, build_out_columns, growth_type='rate'),
            "AnnualGrowthRate": growth_calculator.calculate_annual_growth_rate(processed_data, year_columns)
        }
        # processed_datasets.append(processed_data)

    # data_names = [data_name for data_name, _, _, _, _, _ in datasets_process]
    # print(data_names)

    LOG.info("Export datasets for LAD and Region")
    # List of subkeys in the required order
    subkeys = ['YearTotal', 'AbsoluteGrowth', 'GrowthRatio', 'GrowthRate', 'AnnualGrowthRate']
    output_path = key_constraint_path / f"output_for_viz"
    output_path.mkdir(exist_ok=True)    
    for geography, _ in geographies:  # Ignore geo_data
        for subkey in subkeys:
            for id in ids:
                # Define Excel file name
                file_name = f"{geography}_{subkey}_{id}.xlsx"
                file_path = f'{output_path}/{file_name}'

                outputs = {}

                for source in sources:
                    key = f"{geography}_{source}_{id}"
                    if key in results:
                        df = results[key].get(subkey)
                        if df is not None:
                            sheet_name = f"{geography}_{source}_{id}"[:31]  # Excel sheet name limit
                            outputs[sheet_name] = df

                if outputs:
                    # Check for empty DataFrames before writing
                    empty_sheets = [sheet for sheet, df in outputs.items() if df.empty]
                    if empty_sheets:
                        LOG.warning(f"Skipping empty sheets in {file_path}: {empty_sheets}")

                    try:
                        utilities.write_to_excel(file_path, outputs)
                        LOG.info(f"Successfully wrote {file_path}")
                    except Exception as e:
                        LOG.error(f"Failed to write {file_path}: {e}")

    LOG.info(f"Constraining dlog data with DDG data")
    for id in ids:
        LOG.info(f"Constraining dlog data with DDG data for {id}")
        
        sector_target_growth = growth_calculator.target_growth(
            results[f"{sector}_dlog_{id}"]["YearTotal"], 
            results[f"{sector}_ddg_{id}"]["YearTotal"], 
            base_year_int, 
            build_out_columns,
        )
        
        sector_estimated_growth = results[f"{sector}_dlog_{id}"]["AbsoluteGrowth"]

        sector_index_column_count = sector_target_growth.columns.get_loc(base_year_column)
        sector_index_columns = list(sector_target_growth.columns[:sector_index_column_count])
        sector_bg_growth = sector_target_growth[sector_index_columns].copy()
        for col in build_out_columns:
            sector_bg_growth[col] = sector_target_growth[col] - sector_estimated_growth[col]
      
        zone_target_growth = growth_calculator.target_growth(
            results[f"lad_dlog_{id}"]["YearTotal"], 
            results[f"lad_ddg_{id}"]["YearTotal"], 
            base_year_int, 
            build_out_columns,
        )
        # Create a list of columns to select (zone_index_columns and base_year_column)
        # Calculate weight to distribute sector level background into zone level
        zone_index_column_count = zone_target_growth.columns.get_loc(base_year_column)
        zone_index_columns = list(zone_target_growth.columns[:zone_index_column_count])
        agg_zone_target_growth = zone_target_growth.groupby('REGIONNM')[build_out_columns].sum()
        agg_zone_target_growth.columns = [f"{col}_agg" for col in build_out_columns]
        zone_weight = zone_target_growth.merge(agg_zone_target_growth, on='REGIONNM')
        
        for col in build_out_columns:
            zone_weight[f"{col}_weight"] = (
                zone_weight[col] / zone_weight[f"{col}_agg"]
            )
        
        zone_weight = zone_weight[zone_index_columns + [f"{col}_weight" for col in build_out_columns]]
        
        agg_zone_weight = zone_weight.groupby('REGIONNM')[[f"{col}_weight" for col in build_out_columns]].sum()
        print("agg zone weight", agg_zone_weight)

        # Get lower geographical level background growth    
        zone_bg_growth = zone_weight.merge(sector_bg_growth, on='REGIONNM')
        for col in build_out_columns:
            zone_bg_growth[col] = zone_bg_growth[col] * zone_bg_growth[f"{col}_weight"]
        zone_bg_growth = zone_bg_growth[zone_index_columns + build_out_columns]
   
        agg_zone_bg_growth = zone_bg_growth.groupby('REGIONNM')[build_out_columns].sum()

        # Dlog estimated growth at lower geographical level
        zone_estimated_growth = results[f"lad_dlog_{id}"]["AbsoluteGrowth"]

        # Create a list of columns to select (zone_index_columns and base_year_column)
        columns_to_select = zone_index_columns + [base_year_column]
        zone_adjusted_growth = zone_target_growth[columns_to_select]

        # Create a list of columns to select (zone_index_columns and base_year_column)
        zone_estimated_growth = zone_estimated_growth.sort_values(by=zone_index_columns).reset_index(drop=True)
        zone_bg_growth = zone_bg_growth.sort_values(by=zone_index_columns).reset_index(drop=True)
        zone_adjusted_growth = zone_adjusted_growth.sort_values(by=zone_index_columns).reset_index(drop=True)

        for col in build_out_columns:
            zone_adjusted_growth[col] = (
                zone_estimated_growth[col].values + zone_bg_growth[col].values
            )
        agg_zone_adj_growth = zone_adjusted_growth.groupby('REGIONNM')[build_out_columns].sum()
        # Create target year total        
        zone_forecast = growth_calculator.yearly_totals_from_base(zone_adjusted_growth, base_year_column, build_out_columns)
        # zone_forecast_tot = zone_target_growth[columns_to_select].add(zone_forecast, fill_value=0)
 
        agg_zone_forecast = zone_forecast.groupby('REGIONNM')[build_out_columns].sum()
        agg_zone_forecast = agg_zone_forecast.reset_index()
    
        sector_target_tot = growth_calculator.target_yeartot(
            results[f"{sector}_dlog_{id}"]["YearTotal"], 
            results[f"{sector}_ddg_{id}"]["YearTotal"], 
            base_year_int, 
            build_out_columns,
        )
        # Files to be exported
        sector_target_growth_file = f"{sector}_target_growth_{id}.csv"
        sector_estimated_growth_file = f"{sector}_estimated_growth_{id}.csv"
        sector_bg_growth_file = f"{sector}_background_growth_{id}.csv"
        zone_target_growth_file = f"lad_target_growth_{id}.csv"
        zone_bg_growth_file = f"lad_background_growth_{id}.csv"
        agg_zone_bg_growth_file = f"agg_lad_background_growth_{id}.csv"
        zone_estimated_growth_file = f"lad_estimated_growth_{id}.csv"
        zone_adjusted_growth_file = f"lad_adjusted_growth_{id}.csv"
        agg_zone_adj_growth_file = f"agg_lad_adjusted_growth_{id}.csv"
        zone_forecast_file = f"lad_forecast_{id}.csv"
        agg_zone_forecast_file = f"agg_lad_forecast_{id}.csv"
        sector_target_tot_file = f"{sector}_target_tot_{id}.csv"

        utilities.write_to_csv(key_constraint_path / sector_target_growth_file, sector_target_growth)
        utilities.write_to_csv(key_constraint_path / sector_estimated_growth_file, sector_estimated_growth)
        utilities.write_to_csv(key_constraint_path / sector_bg_growth_file, sector_bg_growth)
        utilities.write_to_csv(key_constraint_path / zone_target_growth_file, zone_target_growth)
        utilities.write_to_csv(key_constraint_path / zone_bg_growth_file, zone_bg_growth)
        utilities.write_to_csv(key_constraint_path / agg_zone_bg_growth_file, agg_zone_bg_growth)
        utilities.write_to_csv(key_constraint_path / zone_estimated_growth_file, zone_estimated_growth)
        utilities.write_to_csv(key_constraint_path / zone_adjusted_growth_file, zone_adjusted_growth)
        utilities.write_to_csv(key_constraint_path / agg_zone_adj_growth_file, agg_zone_adj_growth)
        utilities.write_to_csv(key_constraint_path / zone_forecast_file, zone_forecast)
        utilities.write_to_csv(key_constraint_path / agg_zone_forecast_file, agg_zone_forecast)
        utilities.write_to_csv(key_constraint_path / sector_target_tot_file, sector_target_tot)

    # # Generate visualizations
    # # visualizer = GrowthRateVisualizer(output_dir=key_constraint_path / 'visualizations')
    # # visualizer.generate_visualizations(combined_datasets["GrowthRate"])

    LOG.info("Data processing, aggregation, and visualization completed")
