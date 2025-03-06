import pandas as pd
import logging
from typing import Dict, Any
import numpy as np

# Local imports
from dlit_lu import inputs, utilities
from dlit_lu.viz import GrowthRateVisualizer

LOG = logging.getLogger(__name__)

class ConstraintProcessor():

    def __init__(self, config: inputs.DLitConfig):
        self.sector: inputs.Sector = config.constraint.sector
        self.config: inputs.DLitConfig = config
        self.sector_info_map: Dict[inputs.Sector, Dict[str, Any]] = {
            inputs.Sector.REGION: {
                "lookup_path": self.config.constraint.lad_to_region_file,
                "lad_id": "lad2013_id",
                "base_year_column": self.config.constraint.base_year,
                "region_id": "ntem_region_id",  
                "lad_to_region_prop_col": "lad2013_to_ntem_region"
            }
        }

        self.sector_info: Dict[str, Any] = self.sector_info_map[self.sector]

    def region(self,
           data: pd.DataFrame,
           base_year_column: str,
           future_year_columns: list, 
    ) -> pd.DataFrame:
        """
        Aggregate LAD data to region.

        Parameters
        ----------
        data : pd.DataFrame
            Input data containing LAD-level information.
        Returns
        -------
        pd.DataFrame
            Region data.
        """
        lookup_path = self.sector_info["lookup_path"]  
        lad_id = self.sector_info["lad_id"]
        base_year_column = self.sector_info["base_year_column"]
        region_id = self.sector_info["region_id"]
        lad_to_region_prop_col = self.sector_info["lad_to_region_prop_col"]

        region_data_annual = self.aggregate_to_region(
            data,
            lookup_path,
            lad_id,
            base_year_column,
            future_year_columns,
            region_id,
            lad_to_region_prop_col
        )

        region_data = region_data_annual

        return region_data

    def aggregate_to_region(self, 
                            data, 
                            lookup_path, 
                            lad_id, 
                            base_year_column, 
                            future_year_columns, 
                            region_id, 
                            lad_to_region_prop_col):
        """
        Aggregate data from Local Authority District (LAD) level to region level.

        This function aggregates data from the LAD level to the region level using a lookup table
        that maps LADs to regions. It adjusts the values based on a proportional column and ensures
        that the total values before and after aggregation remain consistent.

        Parameters
        ----------
        data : pd.DataFrame
            DataFrame containing LAD-level data with columns for the base year and future years.
        lookup_path : str
            Path to the CSV file containing the lookup table that maps LADs to regions.
        lad_id : str
            Column name in the data and lookup table representing the LAD identifier.
        base_year_column : str
            Column name in the data representing the base year.
        future_year_columns : list
            List of column names in the data representing future years.
        region_id : str
            Column name in the lookup table representing the region identifier.
        lad_to_region_prop_col : str
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

        merged_df = data.merge(lookup_df[[lad_id, region_id, lad_to_region_prop_col]], on=lad_id, how='left')
        
        for col in val_cols:
            merged_df[col] = merged_df[col] * merged_df[lad_to_region_prop_col]

        region_agg = merged_df.groupby(region_id, as_index=False)[val_cols].sum()
        
        totals_after = {col: region_agg[col].sum() for col in val_cols}

        for col in val_cols:
            if not np.all(pd.np.isclose(totals_before[col], totals_after[col])):
                raise ValueError(f"Total of '{col}' changed after aggregation: before={totals_before[col]}, after={totals_after[col]}")

        return region_agg

    def add_names_to_data(self, 
                      data, 
                      id_column, 
                      name_column, 
                      use_region_name=False,
                      use_region_cols=False):
    
        # Determine which name file to use
        if use_region_name:
            name_file = self.config.constraint.region_name
        else:
            name_file = self.config.constraint.lad_name

        # Read the name file
        name_df = pd.read_csv(name_file)

        if use_region_cols:
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
    
    def combine_datasets(self, growth_rate_results):
        """
        Split datasets into DDG and DLOG and prepare them for saving.

        Parameters
        ----------
        growth_rate_results : list of pd.DataFrame
         List of processed datasets with growth rates calculated.

        Returns
        -------
        dict
            Dictionary containing datasets split into DDG and DLOG.
        """
        # Define the source names based on the order of datasets
        source_names = ['DDG', 'DDG', 'DLOG', 'DLOG', 'DDG', 'DDG', 'DLOG', 'DLOG']
    
        # Add Source column to each dataset and reorder columns
        for i, data in enumerate(growth_rate_results):
            data['Source'] = source_names[i]
            # Reorder columns to place 'Source' after 'LADNM' or 'REGIONNM'
            if 'LADNM' in data.columns:
                cols = list(data.columns)
                cols.insert(cols.index('LADNM') + 1, cols.pop(cols.index('Source')))
                data = data[cols]
            elif 'REGIONNM' in data.columns:
                cols = list(data.columns)
                cols.insert(cols.index('REGIONNM') + 1, cols.pop(cols.index('Source')))
                data = data[cols]
            growth_rate_results[i] = data

        # Split datasets into DDG and DLOG
        ddg_pop = growth_rate_results[0]
        dlog_pop = growth_rate_results[2]
        ddg_emp = growth_rate_results[1]
        dlog_emp = growth_rate_results[3]
        region_ddg_pop = growth_rate_results[4]
        region_dlog_pop = growth_rate_results[6]
        region_ddg_emp = growth_rate_results[5]
        region_dlog_emp = growth_rate_results[7]

        return {
            'LAD_Population': {'DDG': ddg_pop, 'DLOG': dlog_pop},
            'LAD_Employment': {'DDG': ddg_emp, 'DLOG': dlog_emp},
            'Region_Population': {'DDG': region_ddg_pop, 'DLOG': region_dlog_pop},
            'Region_Employment': {'DDG': region_ddg_emp, 'DLOG': region_dlog_emp}
        }
    
class GrowthCalculator:
    
    def __init__(self):
        pass
        
    def calculate_absolute_growth(self, data, base_year_int, build_out_columns):
        """
        Calculate absolute growth for each year.

        Parameters
        ----------
        data : pd.DataFrame
            DataFrame containing year columns.
        year_columns : list
            List of year columns to calculate absolute growth.

        Returns
        -------
        pd.DataFrame
            DataFrame with absolute growth for each year.
        """
        abs_growth = data.copy()
        base_year = str(base_year_int)
        
        for future_year in build_out_columns:
            abs_growth[f'AbsGrowth_{future_year}'] = data[future_year] - data[base_year]
        
        result_columns = list(data.columns[:3]) + [f'AbsGrowth_{year}' for year in build_out_columns]
        
        return abs_growth[result_columns]
        
    def calculate_growth_ratio(self, data, base_year_int, build_out_columns):
        """
        Calculate growth ratio for each year.

        Parameters
        ----------
        data : pd.DataFrame
            DataFrame containing year columns.
        base_year_int : int
            The base year for calculations.
        build_out_columns : list
            List of future year columns to calculate growth ratio.

        Returns
        -------
        pd.DataFrame
            DataFrame with growth ratio for each year.
        """
        growth_ratio = data.copy()
        base_year = str(base_year_int)
        
        for future_year in build_out_columns:
            growth_ratio[f'GrowthRatio_{future_year}'] = data[future_year] / data[base_year]
        
        result_columns = list(data.columns[:3]) + [f'GrowthRatio_{year}' for year in build_out_columns]
        
        return growth_ratio[result_columns]
    
    def calculate_target_growth(self, original_data, base_year_int, build_out_columns):
        """
        Calculate target growth using original data.

        Parameters
        ----------
        original_data : pd.DataFrame
            DataFrame containing original year columns.
        base_year_int : int
            The base year for calculations.
        build_out_columns : list
            List of future year columns to calculate target growth.

        Returns
        -------
        pd.DataFrame
            DataFrame with target growth for each year.
        """
        target_growth = original_data.copy()
        base_year = str(base_year_int)
        
        for future_year in build_out_columns:
            target_growth[f'TargetGrowth_{future_year}'] = (original_data[future_year] / original_data[base_year] - 1) * original_data[base_year]
        
        result_columns = list(original_data.columns[:3]) + [f'TargetGrowth_{year}' for year in build_out_columns]
        
        return target_growth[result_columns]

    def calculate_annual_growth_rate(self, data, year_columns):
        """
        Calculate the Compound Annual Growth Rate (CAGR) for each period between consecutive years.

        This method calculates the CAGR for each period between consecutive years in the provided
        year columns. The CAGR is expressed as a percentage and is added as a new column for each
        end year in the DataFrame.

        Parameters
        ----------
        data : pd.DataFrame
            DataFrame containing data with columns for each year to calculate the growth rate.
        year_columns : list
            List of column names representing the years for which to calculate the CAGR.

        Returns
        -------
        pd.DataFrame
            DataFrame with CAGR columns of each period
            between consecutive years.

        Notes
        -----
        - The CAGR is calculated using the formula:
        CAGR = ((end_value / start_value) ** (1 / years) - 1) * 100
        - If the start value for a period is zero, the CAGR for that period is set to None.
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
                if start_value == 0:
                    return None  
                return ((end_value / start_value) ** (1 / years) - 1) * 100

            cagr_column_name = f'CAGR_{end_year}'
            growth_rate[cagr_column_name] = data.apply(calculate_row_growth, axis=1)
            cagr_columns.append(cagr_column_name)

        result = growth_rate.iloc[:, [0, 1, 2] + [growth_rate.columns.get_loc(col) for col in cagr_columns]]

        return result

    def target(self, target_data, original_data, base_year_int, build_out_columns):
        """
        Calculate the sum of target growth and original data base year value.

        Parameters
        ----------
        target_growth_data : pd.DataFrame
            DataFrame containing target growth results.
        original_data : pd.DataFrame
            DataFrame containing original year columns.
        base_year_int : int
            The base year for calculations.
        build_out_columns : list
            List of future year columns to calculate the sum.

        Returns
        -------
        pd.DataFrame
            DataFrame with the sum of target growth and base year value for each year.
        """
        target_with_base = target_data.copy()
        base_year = str(base_year_int)
        
        for future_year in build_out_columns:
            target_with_base[f'TargetWithBase_{future_year}'] = target_data[f'TargetGrowth_{future_year}'] + original_data[base_year]
        
        result_columns = list(original_data.columns[:3]) + [f'TargetWithBase_{year}' for year in build_out_columns]
        
        return target_with_base[result_columns]

    # def calculate_growth_gap(self, ddg_target_growth, dlog_abs_growth, build_out_columns):
    #     """
    #     Calculate the growth gap between DDG target growth and DLOG absolute growth.

    #     Parameters
    #     ----------
    #     ddg_target_growth : pd.DataFrame
    #         DataFrame containing target growth values from the DDG sheet.
    #     dlog_abs_growth : pd.DataFrame
    #         DataFrame containing absolute growth values from the DLOG sheet.
    #     build_out_columns : list
    #         List of future year columns to calculate the growth gap.

    #     Returns
    #     -------
    #     pd.DataFrame
    #         DataFrame with the growth gap for each year.
    #     """
    #     growth_gap = ddg_target_growth.copy()
        
    #     for future_year in build_out_columns:
    #         growth_gap[f'GrowthGap_{future_year}'] = ddg_target_growth[f'TargetGrowth_{future_year}'] - dlog_abs_growth[f'AbsGrowth_{future_year}']
        
    #     result_columns = list(ddg_target_growth.columns[:3]) + [f'GrowthGap_{year}' for year in build_out_columns]
        
    #     return growth_gap[result_columns]
    
        
def run(config: inputs.DLitConfig):

    if config.dev_pattern is None:
        raise ValueError("Cannot run development pattern without any dev_pattern parameters")

    LOG.info("Initialising GrowthRate Module")

    config.output_folder.mkdir(exist_ok=True)

    # Load data
    ddg_pop = pd.read_csv(config.constraint.ddg_pop)
    ddg_emp = pd.read_csv(config.constraint.ddg_emp)
    dlog_population = pd.read_csv(config.constraint.dlog_population)
    dlog_employment = pd.read_csv(config.constraint.dlog_employment)

    key_constraint_path = config.output_folder / f"06_constraint"
    key_constraint_path.mkdir(exist_ok=True)

    # Process data
    year_columns = [col for col in dlog_population.columns if col.isdigit()]
    base_year_column = config.constraint.base_year
    base_year_int = int(base_year_column)
    build_out_columns = np.arange(base_year_int + 1, 2067, 1).tolist()
    build_out_columns = [str(year) for year in build_out_columns]

    ddg_col = 'LAD13CD'
    ddg_pop = ddg_pop[[ddg_col] + year_columns]
    ddg_emp = ddg_emp[[ddg_col] + year_columns]

    ddg_pop = ddg_pop[~ddg_pop[ddg_col].str.startswith('LON')]
    ddg_emp = ddg_emp[~ddg_emp[ddg_col].str.startswith('LON')]

    ddg_pop = ddg_pop.rename(columns={'LAD13CD': 'lad2013_id'})
    ddg_emp = ddg_emp.rename(columns={'LAD13CD': 'lad2013_id'})

    lad_id = "lad2013_id"
    name_column = "descriptions"
    region_id = "ntem_region_id"
    name_column_region = "zone_id"

    processor = ConstraintProcessor(config)
    growth_calculator = GrowthCalculator()

    # Aggregate data
    region_dlog_pop = processor.region(
        dlog_population, 
        base_year_column, 
        build_out_columns)
    
    region_ddg_pop = processor.region(
        ddg_pop, 
        base_year_column, 
        build_out_columns)
    
    region_ddg_emp = processor.region(
        ddg_emp, 
        base_year_column, 
        build_out_columns)
    
    region_dlog_emp = processor.region(
        dlog_employment,
        base_year_column, 
        build_out_columns)

    # Add names
    datasets = [
        (ddg_pop, lad_id, name_column, False, False),
        (ddg_emp, lad_id, name_column, False, False),
        (dlog_population, lad_id, name_column, False, False),
        (dlog_employment, lad_id, name_column, False, False),
        (region_ddg_pop, region_id, name_column_region, True, True),
        (region_ddg_emp, region_id, name_column_region, True, True),
        (region_dlog_pop, region_id, name_column_region, True, True),
        (region_dlog_emp, region_id, name_column_region, True, True)
    ]
    
    processed_datasets = []

    for data, id_col, name_col, use_region_name, use_region_cols in datasets:
        processed_data = processor.add_names_to_data(
            data, id_col, name_col, use_region_name=use_region_name, use_region_cols=use_region_cols
        )
        processed_datasets.append(processed_data)

    # Calculate growth metrics
    abs_growth_results = []
    growth_ratio_results = []
    target_growth_results = []
    growth_rate_results = []
    target = []

    for data in processed_datasets:
        # Calculate absolute growth
        abs_growth_result = growth_calculator.calculate_absolute_growth(data, base_year_int, build_out_columns)
        abs_growth_results.append(abs_growth_result)

        # Calculate growth ratio
        growth_ratio_result = growth_calculator.calculate_growth_ratio(data, base_year_int, build_out_columns)
        growth_ratio_results.append(growth_ratio_result)

        # Calculate annual growth rate
        growth_rate_result = growth_calculator.calculate_annual_growth_rate(data, year_columns)
        growth_rate_results.append(growth_rate_result)

        # Calculate target growth
        target_growth_result = growth_calculator.calculate_target_growth(data, base_year_int, build_out_columns)
        target_growth_results.append(target_growth_result)

        # Calculate target with base
        target_with_base_result = growth_calculator.target(target_growth_result, data, base_year_int, build_out_columns)
        target.append(target_with_base_result)

    # Split and prepare datasets
    prepared_datasets_gr = processor.combine_datasets(growth_rate_results)
    prepared_datasets_tg = processor.combine_datasets(target_growth_results)
    prepared_datasets_ab = processor.combine_datasets(abs_growth_results)
    prepared_datasets_gra = processor.combine_datasets(growth_ratio_results)
    prepared_datasets_t = processor.combine_datasets(target)


    for category, datasets in prepared_datasets_gr.items():
        excel_output_path = key_constraint_path / f'{category}_GrowthRate.xlsx'
        utilities.write_to_excel(excel_output_path, datasets)

    for category, datasets in prepared_datasets_tg.items():
        excel_output_path = key_constraint_path / f'{category}_TargetGrowth.xlsx'
        utilities.write_to_excel(excel_output_path, datasets)

    for category, datasets in prepared_datasets_ab.items():
        excel_output_path = key_constraint_path / f'{category}_AbsoluteGrowth.xlsx'
        utilities.write_to_excel(excel_output_path, datasets)

    for category, datasets in prepared_datasets_gra.items():
        excel_output_path = key_constraint_path / f'{category}_GrowthRatio.xlsx'
        utilities.write_to_excel(excel_output_path, datasets)

    for category, datasets in prepared_datasets_t.items():
        excel_output_path = key_constraint_path / f'{category}_Target.xlsx'
        utilities.write_to_excel(excel_output_path, datasets)


    # Visualize the data
    visualizer = GrowthRateVisualizer(output_dir=key_constraint_path / 'visualizations')
    visualizer.generate_visualizations(prepared_datasets_gr)

    LOG.info("Data processing, aggregation, and visualization completed")