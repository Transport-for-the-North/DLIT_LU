import pandas as pd
import pathlib
import logging
import os

# Local imports
from dlit_lu import inputs, utilities
import numpy as np

from dlit_lu.viz import GrowthRateVisualizer

LOG = logging.getLogger(__name__)

class ConstraintProcessor():

    def __init__(self, config: inputs.DLitConfig):
        self.config: inputs.DLitConfig = config
            
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
        lookup_path = self.config.dev_pattern.summary_data.lad_to_region_file
        lad_id = "lad2013_id"
        base_year_column = "2023"
        region_id = "ntem_region_id"
        lad_to_region_prop_col = "lad2013_to_ntem_region"

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
    
def calculate_growth_rate(data, year_columns):
    """
    Calculate the annual growth rate for the given DataFrame.

    Args:
        data (DataFrame): The DataFrame containing year columns with data to calculate growth rate.
        year_columns (list): List of year columns to calculate growth rate.

    Returns:
        DataFrame: A DataFrame with only the LAD column and CAGR values.
    """
    growth_rate = data.copy()
    year_columns = sorted([int(year) for year in year_columns])

    cagr_columns = []

    # Calculate CAGR for each period
    for i in range(1, len(year_columns)):
        start_year = year_columns[i - 1]
        end_year = year_columns[i]
        years = end_year - start_year

        # Calculate CAGR for the period and store it in the end year's column
        def calculate_row_growth(row):
            start_value = row[str(start_year)]
            end_value = row[str(end_year)]
            if start_value == 0:
                return None  
            return ((end_value / start_value) ** (1 / years) - 1) * 100

        cagr_column_name = f'CAGR_{end_year}'
        growth_rate[cagr_column_name] = data.apply(calculate_row_growth, axis=1)
        cagr_columns.append(cagr_column_name)

    # Select only the first columns and existing CAGR columns
    result = growth_rate.iloc[:, [0, 1, 2] + [growth_rate.columns.get_loc(col) for col in cagr_columns]]

    return result

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
    ddg_col = 'LAD13CD'
    ddg_pop = ddg_pop[[ddg_col] + year_columns]
    ddg_emp = ddg_emp[[ddg_col] + year_columns]

    ddg_pop = ddg_pop[~ddg_pop[ddg_col].str.startswith('LON')]
    ddg_emp = ddg_emp[~ddg_emp[ddg_col].str.startswith('LON')]

    ddg_pop = ddg_pop.rename(columns={'LAD13CD': 'lad2013_id'})
    ddg_emp = ddg_emp.rename(columns={'LAD13CD': 'lad2013_id'})

    processor = ConstraintProcessor(config)

    lad_id = "lad2013_id"
    name_column = "descriptions"
    base_year_column = "2023"
    base_year_int = int(base_year_column)
    region_id = "ntem_region_id"
    name_column_region = "zone_id"

    build_out_columns = np.arange(base_year_int + 1, 2067, 1).tolist()
    build_out_columns = [str(year) for year in build_out_columns]

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

    # Calculate growth rates
    growth_rate_results = []
    for data in processed_datasets:
        growth_rate_result = calculate_growth_rate(data, year_columns)
        growth_rate_results.append(growth_rate_result)

    # Split and prepare datasets
    prepared_datasets = processor.combine_datasets(growth_rate_results)

    for category, datasets in prepared_datasets.items():
        excel_output_path = key_constraint_path / f'{category}.xlsx'
        utilities.write_to_excel(excel_output_path, datasets)

    # Visualize the data
    visualizer = GrowthRateVisualizer(output_dir=key_constraint_path / 'visualizations')
    visualizer.generate_visualizations(prepared_datasets)

    LOG.info("Data processing, aggregation, and visualization completed")