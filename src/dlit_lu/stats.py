"""determination of cut-off values (thresholds) 
"""

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.stats import zscore

def plot_distribution(df, columns):
    """
    Visualize the distribution of the density variables.
    
    Parameters:
    df: pandas.DataFrame
        The input dataframe.
    columns: list of str
        List of column names to visualize.
    """
    for col in columns:
        plt.figure(figsize=(8, 6))
        sns.histplot(df[col], kde=True, bins=30, color='skyblue')
        plt.title(f'Distribution of {col}')
        plt.xlabel(col)
        plt.ylabel('Frequency')
        plt.show()

def basic_statistics(df, columns):
    """
    Compute basic statistics (mean, median, std) for the given columns.
    
    Parameters:
    df: pandas.DataFrame
        The input dataframe.
    columns: list of str
        List of column names to compute statistics for.
    
    Returns:
    stats: pandas.DataFrame
        Dataframe with mean, median, and std for each column.
    """
    stats = pd.DataFrame(index=columns)
    stats['Mean'] = df[columns].mean()
    stats['Median'] = df[columns].median()
    stats['Standard Deviation'] = df[columns].std()
    return stats

def percentiles_or_quantiles(df, columns, percentile=25):
    """
    Identify zones in the lower percentile range (default: 25th percentile).
    
    Parameters:
    df: pandas.DataFrame
        The input dataframe.
    columns: list of str
        List of column names to calculate percentiles.
    percentile: int, optional
        Percentile to identify low-density zones, default is 25.
    
    Returns:
    low_density_zones: dict
        Dictionary with column names as keys and list of low-density zones as values.
    """
    low_density_zones = {}
    for col in columns:
        cutoff = np.percentile(df[col], percentile)
        low_density_zones[col] = df[df[col] < cutoff].index.tolist()
    
    return low_density_zones

def z_score_method(df: pd.DataFrame, columns: list, threshold=-1):
    """
    Identify zones based on Z-score threshold for low-density values.
    
    Parameters:
    df: pandas.DataFrame
        The input dataframe.
    columns: list of str
        List of column names to calculate Z-scores for.
    threshold: float, optional
        The Z-score threshold for identifying low-density zones, default is -1.
    
    Returns:
    low_density_zones: dict
        Dictionary with column names as keys and list of low-density zones as values.
    """
    low_density_zones = {}
    for col in columns:
        z_scores = zscore(df[col].dropna())
        low_density_zones[col] = df.index[z_scores < threshold].tolist()
    
    return low_density_zones

# # Example usage:
# columns_to_explore = [
#     "Total new dwelling",
#     "Existing density of hh",
#     "Existing density of emp",
#     "Growth over existing zonal total dwelling",
#     "Distance to the existing centroids of population"
# ]

# # Assuming `df` is your dataframe
# plot_distribution(df, columns_to_explore)
# stats = basic_statistics(df, columns_to_explore)
# print(stats)

# low_density_percentiles = percentiles_or_quantiles(df, columns_to_explore, percentile=25)
# print("Low Density Zones based on Percentile:")
# print(low_density_percentiles)

# low_density_z_scores = z_score_method(df, columns_to_explore, threshold=-1)
# print("Low Density Zones based on Z-score:")
# print(low_density_z_scores)
