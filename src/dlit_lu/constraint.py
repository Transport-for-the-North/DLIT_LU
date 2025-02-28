# load ddg_data, dlog (lad)():
# aggregate(): from lad to region - ddg and dlog.  Should have region and lad data. 8 Datasets - 4 in lad for ddg and dlog and 4 region ddg and region dlog
# assign zone id to name(): 
# def calculate growth rate(): 
# merge ddg and dlog(): pop and emp , 4 dataframes 
# visualise(): pop and emp 4 visuals 
# save in 06>visuals , 06>output 

# lad_population and lad_job call these 

import pandas as pd
import pathlib
import logging
import os

# Local imports
from dlit_lu import inputs, utilities
import numpy as np


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

        # Convert zone_id to string to match lad2013_id
        #name_df['zone_id'] = name_df['zone_id'].astype(str)

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

        LOG.info(f"Added {name_column} to dataset with shape {data_with_name.shape}")

        return data_with_name
    
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

    # List to keep track of created CAGR columns
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
                return None  # Skip calculation and return None for zero start value
            return ((end_value / start_value) ** (1 / years) - 1) * 100

        cagr_column_name = f'CAGR_{end_year}'
        growth_rate[cagr_column_name] = data.apply(calculate_row_growth, axis=1)
        cagr_columns.append(cagr_column_name)

    # Select only the first column and existing CAGR columns
    result = growth_rate.iloc[:, [0,1] + [growth_rate.columns.get_loc(col) for col in cagr_columns]]

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

    # Save results
    filenames = [
        "ddg_pop.csv", 
        "ddg_emp.csv", 
        "dlog_population.csv",
        "dlog_employment.csv",
        "region_ddg_pop.csv", 
        "region_ddg_emp.csv", 
        "region_dlog_pop.csv", 
        "region_dlog_emp.csv"
    ]

    for filename, data in zip(filenames, growth_rate_results):
        utilities.write_to_csv(key_constraint_path / filename, data)

    LOG.info("Data processing and aggregation completed")

# def merge():
#     return

# def visualise():
#     return
# def run(config:inputs.ConstraintsConfig):
#     return



# def merge_datasets(dataset1, dataset2, key_column1, key_column2, suffix1='_1', suffix2='_2', keep_key='left'):
#     """
#     Merge two datasets on specified key columns with suffixes and keep only one key column.

#     Args:
#         dataset1 (DataFrame): The first DataFrame to merge.
#         dataset2 (DataFrame): The second DataFrame to merge.
#         key_column1 (str): The key column name in the first DataFrame.
#         key_column2 (str): The key column name in the second DataFrame.
#         suffix1 (str): Suffix to apply to overlapping columns from the first DataFrame.
#         suffix2 (str): Suffix to apply to overlapping columns from the second DataFrame.
#         keep_key (str): Specify which key column to keep ('left' or 'right').

#     Returns:
#         DataFrame: A merged DataFrame with only one key column.
#     """
#     merged_data = dataset1.merge(
#         dataset2,
#         left_on=key_column1,
#         right_on=key_column2,
#         suffixes=(suffix1, suffix2),
#         how='outer'
#     )
    
#     if keep_key == 'left':
#         if key_column2 in merged_data.columns:
#             merged_data.drop(columns=[key_column2], inplace=True)
#     elif keep_key == 'right':
#         if key_column1 in merged_data.columns:
#             merged_data.drop(columns=[key_column1], inplace=True)
#     else:
#         raise ValueError("Invalid value for 'keep_key'. Use 'left' or 'right'.")

#     return merged_data

# def visualize_results(data, output_file):
#     """
#     Visualize the results in an interactive HTML file.

#     Args:
#         data (DataFrame): The DataFrame containing growth rate data.
#         output_file (str): The path to save the HTML visualization.

#     Returns:
#         None
#     """
#     fig = go.Figure()
#     for zone in data['LAD13CD'].unique():
#         zone_data = data[data['LAD13CD'] == zone]
#         for col in zone_data.columns:
#             if col.startswith('growth_'):
#                 year = col.split('_')[1]
#                 trace = go.Scatter(
#                     x=[year], y=zone_data[col], mode='lines+markers',
#                     name=f"{zone} - {year}", hovertemplate='%{y:.2f}%'
#                 )
#                 fig.add_trace(trace)
#     fig.update_layout(
#         title="Growth Rate Visualization",
#         xaxis_title="Year",
#         yaxis_title="Growth Rate (%)",
#         yaxis_tickformat=",d"
#     )
#     fig.write_html(output_file)
#     print(f"Visualization saved as: {output_file}")


# # if __name__ == "__main__":
# #     # Specify input file paths
# #     lad_population_file_path = r"I:\NorMITs Demand\import\edge_replicant\edge_inputs\Drivers\Jan 24 central DDG drivers\DD_Jan24_Central_Emp_LA.csv"
# #     #  = "path/to/lad_population.csv"

# #         # Load data
# #     lad_population = pd.read_csv(lad_population_file_path)
# #     # lad_population = pd.read_csv(lad_population_file_path)

# #     # Determine year columns from lad_population
# #     year_columns = [col for col in lad_population.columns if col.isdigit()]

# #     # Calculate growth rates
# #     # ddg_pop_growth = calculate_growth_rate(ddg_pop_lad, year_columns)
# #     lad_population_growth = calculate_growth_rate(lad_population, year_columns)



# #     # ddg_pop_growth.to_csv("ddg_pop_growth_rates.csv", index=False)

# #     out = "I:\\Data\\D-Log\\DLIT\\Outputs\\test10\\05_normits"
# #     lad_population_growth.to_csv(os.path.join(out, "ddg_population_growth_rates.csv"), index=False)

#     ######## Growth Rate ######

#     LOG.info("Calculating Growth Rate and Visualising results")

#     # Determine year columns from lad_population or lad_job
#     year_columns = [col for col in lad_population.columns if col.isdigit()]

#     # Subset DDG datasets to match LAD datasets
#     ddg_pop_lad = ddg_pop_lad[['LAD13CD'] + year_columns]
#     ddg_emp_lad = ddg_emp_lad[['LAD13CD'] + year_columns]

#     #ddg_pop_lad.to_csv(os.path.join(key_output_path, "ddg_pop_lad.csv"))

#     # Calculate growth rates
#     ddg_pop_growth = calculate_growth_rate(ddg_pop_lad, year_columns)
#     lad_population_growth = calculate_growth_rate(lad_population, year_columns)
#     lad_job_growth = calculate_growth_rate(lad_job, year_columns)
#     ddg_emp_growth = calculate_growth_rate(ddg_emp_lad, year_columns)

#     merged_growth_data_pop = merge_datasets(lad_population_growth, ddg_pop_growth,key_column1='lad2013_id', key_column2='LAD13CD',suffix1='_lad', suffix2='_ddg',keep_key='right' ) 
#     #merged_growth_data_pop.to_csv(os.path.join(key_output_path, "merged_growth_data_pop.csv"))
#     merged_growth_data_job = merge_datasets(lad_job_growth, ddg_emp_growth,key_column1='lad2013_id', key_column2='LAD13CD',suffix1='_lad', suffix2='_ddg',keep_key='right')

#     #os.makedirs(output_directory, exist_ok=True)
#     visualize_results(merged_growth_data_pop, os.path.join(key_output_path, "growth_rate_visualization_pop.html"))
#     visualize_results(merged_growth_data_job, os.path.join(key_output_path, "growth_rate_visualization_job.html"))