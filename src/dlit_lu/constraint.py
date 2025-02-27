# load ddg_data, dlog (lad)():
# aggregate(): from lad to region - ddg and dlog.  Should have region and lad data. 8 Datasets - 4 in lad for ddg and dlog and 4 region ddg and region dlog
# assign zone id to name(): 
# def calculate growth rate(): 
# merge ddg and dlog(): pop and emp , 4 dataframes 
# visualise(): pop and emp 4 visuals 
# save in 06>visuals , 06>output 



# lad_population and lad_job call these 

import pandas as pd
import plotly.graph_objects as go
import os

import logging
import pathlib
from typing import Optional, Dict, Any

# third party imports
import pandas as pd
import geopandas as gpd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from scipy.spatial import cKDTree
import os

# local imports
from dlit_lu import inputs
from dlit_lu import land_use as lu

LOG = logging.getLogger(__name__)

class GrowthRate:
    def __init__(self, config: inputs.DLitConfig):
        
        self.constraints_config = config.constraint
        LOG.info("Initialising Constraints Module")

    def load_data(self, config: inputs.SummaryInputs):
        LOG.info("Loading DDG Employment data...")
        ddg_pop = pd.read_csv(self.constraints_config.ddg_pop)
        ddg_emp = pd.read_csv(self.constraints_config.ddg_emp)
        dlog_population = pd.read_csv(self.constraints_config.dlog_population)
        dlog_employment = pd.read_csv(self.constraints_config.dlog_employment)
        lad_to_region_file = pd.read_csv(config.lad_to_region_file)
        
        return ddg_pop, ddg_emp, dlog_population, dlog_employment, lad_to_region_file

    def _lad_to_region(        
            self,
            data: pd.DataFrame,
            lookup_path: pathlib.Path,
            lad_id: str,
            base_year_column: str,
            future_year_columns: list,
            region_id: str,
            lad_to_region_prop_col: str,
    ) -> pd.DataFrame:
        """
        Aggregate LAD data to region level using a lookup file with proportional mapping.

        Parameters
        ----------
        data : pd.DataFrame
            DataFrame containing LAD-level data.
        lookup_path : Path
            Path to the lookup CSV file containing LAD-to-region mapping and proportion columns.
        lad_id : str
            Column name in data representing the LAD identifier.
        future_year_columns : list
            List of columns in data with future year values to be aggregated.
        region_id : str
            Column name in lookup representing the region identifier.
        lad_to_region_prop_col : str
            Column in lookup representing the proportion of LAD value allocated to each region.

        Returns
        -------
        pd.DataFrame
            Aggregated region-level DataFrame with the summed values.
        """
        lookup_df = pd.read_csv(lookup_path)

        # Check initial totals for all val_cols
        val_cols = [base_year_column] + future_year_columns
        totals_before = {col: data[col].sum() for col in val_cols}

        # Merge data with lookup to assign regions and proportions
        merged_df = data.merge(
            lookup_df[[lad_id, region_id, lad_to_region_prop_col]],
            on=lad_id,
            how='left'
        )

        # Apply proportions to each value column
        for col in val_cols:
            merged_df[col] = merged_df[col] * merged_df[lad_to_region_prop_col]

        # Group by region and sum the values
        region_agg = merged_df.groupby(region_id, as_index=False)[val_cols].sum()

        # Check totals after aggregation
        totals_after = {col: region_agg[col].sum() for col in val_cols}

        for col in val_cols:
            if not np.isclose(totals_before[col], totals_after[col]):
                raise ValueError(
                    f"Total of '{col}' changed after aggregation: before={totals_before[col]}, after={totals_after[col]}"
                )

        return region_agg

    def aggregate(self, ddg_pop, ddg_emp, dlog_population, dlog_employment, lad_to_region_file):
        
        year_columns = [col for col in dlog_population.columns if col.isdigit()]

        # Subset DDG datasets to match LAD datasets (using LAD13CD and year columns)
        ddg_pop = ddg_pop[['LAD13CD'] + year_columns]
        ddg_emp = ddg_emp[['LAD13CD'] + year_columns]

        LOG.info(f"ddg_pop and ddg_emp subsets to LAD13CD and years: {year_columns}")

       
        lad_id = "lad2013_id"
        region_id = "ntem_region_id"
        lad_to_region_prop_col = "lad2013_to_ntem_region"

        # Aggregating from LAD to Region for DDG datasets
        region_ddg_pop = self._lad_to_region(
            ddg_pop, 
            lad_to_region_file, 
            lad_id, 
            "2023",  
            year_columns,  
            region_id,  
            lad_to_region_prop_col  
        )

        region_ddg_emp = self._lad_to_region(
            ddg_emp, 
            lad_to_region_file, 
            lad_id, 
            "2023",  
            year_columns,  
            region_id,  
            lad_to_region_prop_col 
        )

        LOG.info(f"Aggregated DDG population and employment data to region format.")

        # Aggregating from LAD to Region for DLOG datasets (population and employment)

        region_dlog_pop = self._lad_to_region(
            dlog_population, 
            lad_to_region_file, 
            lad_id, 
            "2023",  
            year_columns,  
            region_id,  
            lad_to_region_prop_col
        )

        region_dlog_emp = self._lad_to_region(
            dlog_employment, 
            lad_to_region_file, 
            lad_id, 
            "2023", 
            year_columns,  
            region_id,  
            lad_to_region_prop_col 
        )

        LOG.info(f"Aggregated DLOG population and employment data to region format.")

        return region_ddg_pop, region_ddg_emp, region_dlog_pop, region_dlog_emp

# def id_to_name():
#     return

# def calculate_growth_rate(data, year_columns):
#     return

# def merge():
#     return

# def visualise():
#     return
# def run(config:inputs.ConstraintsConfig):
#     return

# def calculate_growth_rate(data, year_columns):
#     """
#     Calculate the annual growth rate for the given DataFrame.

#     Args:
#         data (DataFrame): The DataFrame containing year columns with data to calculate growth rate.
#         year_columns (list): List of year columns to calculate growth rate.

#     Returns:
#         DataFrame: A DataFrame with only the LAD column and CAGR values.
#     """
#     growth_rate = data.copy()
#     year_columns = sorted([int(year) for year in year_columns])
    
#     # List to keep track of created CAGR columns
#     cagr_columns = []
    
#     # Calculate CAGR for each period
#     for i in range(1, len(year_columns)):
#         start_year = year_columns[i - 1]
#         end_year = year_columns[i]
#         years = end_year - start_year
        
#         # Calculate CAGR for the period and store it in the end year's column
#         def calculate_row_growth(row):
#             start_value = row[str(start_year)]
#             end_value = row[str(end_year)]
#             if start_value == 0:
#                 return None  # Skip calculation and return None for zero start value
#             return ((end_value / start_value) ** (1 / years) - 1) * 100
        
#         cagr_column_name = f'cagr_{end_year}'
#         growth_rate[cagr_column_name] = data.apply(calculate_row_growth, axis=1)
#         cagr_columns.append(cagr_column_name)
    
#     # Select only the first column and existing CAGR columns
#     result = growth_rate.iloc[:, [0] + [growth_rate.columns.get_loc(col) for col in cagr_columns]]
    
#     return result

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