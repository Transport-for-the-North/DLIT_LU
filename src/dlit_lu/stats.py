"""determination of cut-off values (thresholds) 
"""

import pandas as pd
import numpy as np
import pathlib
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.stats import zscore

def plot_distribution(df:pd.DataFrame, columns: list, save_path: pathlib.Path):
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
        
        # Calculate key statistics
        mean = df[col].mean()
        percentile_25 = np.percentile(df[col], 25)
        percentile_75 = np.percentile(df[col], 75)
        
        # Add vertical lines for mean, 25th, and 75th percentiles
        plt.axvline(mean, color='red', linestyle='--', label=f'Mean: {mean:.2f}')
        plt.axvline(percentile_25, color='green', linestyle='--', label=f'25th Percentile: {percentile_25:.2f}')
        plt.axvline(percentile_75, color='orange', linestyle='--', label=f'75th Percentile: {percentile_75:.2f}')
        
        plt.title(f'Distribution of {col}')
        plt.xlabel(col)
        plt.ylabel('Frequency')
        plt.legend()
        
        # If save_path is provided, save the figure to that directory
        if save_path:
            plt.savefig(f"{save_path}/{col}_distribution.png", dpi=300)
        plt.close()

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
    # Calculate mean, median, and std
    stats['Mean'] = df[columns].mean()
    stats['Median'] = df[columns].median()
    stats['Standard Deviation'] = df[columns].std()
    
    # Calculate percentiles
    stats['25th Percentile'] = df[columns].apply(lambda col: np.percentile(col, 25))
    stats['75th Percentile'] = df[columns].apply(lambda col: np.percentile(col, 75))
    
    # Calculate Z-scores
    z_scores = df[columns].apply(lambda col: zscore(col.dropna()))
    stats['Z-Score Mean'] = z_scores.mean()
    stats['Z-Score Std'] = z_scores.std()
    
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


