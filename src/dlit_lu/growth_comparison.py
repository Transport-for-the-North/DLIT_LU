# load ddg_data ():
# def calculate growth rate():
# merge ddg and dlog 
# visualise 

# lad_population and lad_job call these 

import pandas as pd
import plotly.graph_objects as go
import os

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
        
        cagr_column_name = f'cagr_{end_year}'
        growth_rate[cagr_column_name] = data.apply(calculate_row_growth, axis=1)
        cagr_columns.append(cagr_column_name)
    
    # Select only the first column and existing CAGR columns
    result = growth_rate.iloc[:, [0] + [growth_rate.columns.get_loc(col) for col in cagr_columns]]
    
    return result

def merge_datasets(dataset1, dataset2, key_column1, key_column2, suffix1='_1', suffix2='_2', keep_key='left'):
    """
    Merge two datasets on specified key columns with suffixes and keep only one key column.

    Args:
        dataset1 (DataFrame): The first DataFrame to merge.
        dataset2 (DataFrame): The second DataFrame to merge.
        key_column1 (str): The key column name in the first DataFrame.
        key_column2 (str): The key column name in the second DataFrame.
        suffix1 (str): Suffix to apply to overlapping columns from the first DataFrame.
        suffix2 (str): Suffix to apply to overlapping columns from the second DataFrame.
        keep_key (str): Specify which key column to keep ('left' or 'right').

    Returns:
        DataFrame: A merged DataFrame with only one key column.
    """
    merged_data = dataset1.merge(
        dataset2,
        left_on=key_column1,
        right_on=key_column2,
        suffixes=(suffix1, suffix2),
        how='outer'
    )
    
    if keep_key == 'left':
        if key_column2 in merged_data.columns:
            merged_data.drop(columns=[key_column2], inplace=True)
    elif keep_key == 'right':
        if key_column1 in merged_data.columns:
            merged_data.drop(columns=[key_column1], inplace=True)
    else:
        raise ValueError("Invalid value for 'keep_key'. Use 'left' or 'right'.")

    return merged_data

def visualize_results(data, output_file):
    """
    Visualize the results in an interactive HTML file.

    Args:
        data (DataFrame): The DataFrame containing growth rate data.
        output_file (str): The path to save the HTML visualization.

    Returns:
        None
    """
    fig = go.Figure()
    for zone in data['LAD13CD'].unique():
        zone_data = data[data['LAD13CD'] == zone]
        for col in zone_data.columns:
            if col.startswith('growth_'):
                year = col.split('_')[1]
                trace = go.Scatter(
                    x=[year], y=zone_data[col], mode='lines+markers',
                    name=f"{zone} - {year}", hovertemplate='%{y:.2f}%'
                )
                fig.add_trace(trace)
    fig.update_layout(
        title="Growth Rate Visualization",
        xaxis_title="Year",
        yaxis_title="Growth Rate (%)",
        yaxis_tickformat=",d"
    )
    fig.write_html(output_file)
    print(f"Visualization saved as: {output_file}")


# if __name__ == "__main__":
#     # Specify input file paths
#     lad_population_file_path = r"I:\NorMITs Demand\import\edge_replicant\edge_inputs\Drivers\Jan 24 central DDG drivers\DD_Jan24_Central_Emp_LA.csv"
#     #  = "path/to/lad_population.csv"

#         # Load data
#     lad_population = pd.read_csv(lad_population_file_path)
#     # lad_population = pd.read_csv(lad_population_file_path)

#     # Determine year columns from lad_population
#     year_columns = [col for col in lad_population.columns if col.isdigit()]

#     # Calculate growth rates
#     # ddg_pop_growth = calculate_growth_rate(ddg_pop_lad, year_columns)
#     lad_population_growth = calculate_growth_rate(lad_population, year_columns)



#     # ddg_pop_growth.to_csv("ddg_pop_growth_rates.csv", index=False)

#     out = "I:\\Data\\D-Log\\DLIT\\Outputs\\test10\\05_normits"
#     lad_population_growth.to_csv(os.path.join(out, "ddg_population_growth_rates.csv"), index=False)