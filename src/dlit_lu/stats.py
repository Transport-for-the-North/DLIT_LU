"""determination of cut-off values (thresholds) 
"""

import pandas as pd
import numpy as np
import pathlib
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.stats import zscore
from typing import Tuple, List
import jenkspy


def plot_distribution(df: pd.DataFrame, columns: list, save_path: pathlib.Path, category: str = 'Res'):
    """
    Visualize the distribution of the density variables.

    Parameters:
    df: pandas.DataFrame
        The input dataframe.
    columns: list of str
        List of column names to visualize.
    category: str, optional (default is 'General')
        Category of the data to differentiate between residential and employment.
    """
    for col in columns:
        plt.figure(figsize=(8, 6))
        sns.histplot(df[col], kde=True, bins=100, color='skyblue')
        
        # Calculate key statistics
        mean = df[col].mean()
        percentile_25 = np.percentile(df[col], 25)
        percentile_75 = np.percentile(df[col], 75)
        
        # Add vertical lines for mean, 25th, and 75th percentiles
        plt.axvline(mean, color='red', linestyle='--', label=f'Mean: {mean:.4f}')
        plt.axvline(percentile_25, color='green', linestyle='--', label=f'25th Percentile: {percentile_25:.4f}')
        plt.axvline(percentile_75, color='orange', linestyle='--', label=f'75th Percentile: {percentile_75:.4f}')
        
        plt.title(f'Distribution of {col} - {category}')
        plt.xlabel(col)
        plt.ylabel('Frequency')
        plt.legend()
        # Adjust x-ticks to show finer intervals
        x_min, x_max = df[col].min(), df[col].max()
        plt.xticks(np.linspace(x_min, x_max, num=15))  # Adjust 'num' for more or fewer ticks
        plt.xticks(rotation=45)  # Rotate labels if they overlap    
        # If save_path is provided, save the figure to that directory with category in filename
        if save_path:
            save_filename = f"{category}_{col}_distribution.png"
            plt.savefig(save_path / save_filename, dpi=300)
        plt.close()


def plot_boxplots(df: pd.DataFrame, columns: list, save_path: pathlib.Path, category: str = 'Res'):
    """
    Generate box plots with key statistics for given density variables.

    Parameters:
    df: pandas.DataFrame
        The input dataframe.
    columns: list of str
        List of column names to visualize.
    save_path: pathlib.Path
        The directory to save the plots.
    category: str, optional (default is 'Res')
        Category of the data to differentiate between residential and employment.
    """
    for col in columns:
        plt.figure(figsize=(8, 6))
        
        # Compute key statistics
        mean = df[col].mean()
        median = df[col].median()
        percentile_10 = np.percentile(df[col], 10)
        percentile_90 = np.percentile(df[col], 90)

        # Create the box plot
        sns.boxplot(y=df[col], color='lightblue')
        
        # Add horizontal lines for key statistics
        plt.axhline(mean, color='red', linestyle='--', label=f'Mean: {mean:.4f}')
        plt.axhline(median, color='purple', linestyle='-', label=f'Median (50%): {median:.4f}')
        plt.axhline(percentile_10, color='blue', linestyle='--', label=f'10%: {percentile_10:.4f}')
        plt.axhline(percentile_90, color='brown', linestyle='--', label=f'90%: {percentile_90:.4f}')
        
        plt.legend(loc='upper right', fontsize=9)
        plt.title(f'Box Plot of {col} - {category}')
        plt.ylabel(col)
        
        # Save the plot if save_path is provided
        if save_path:
            save_filename = f"{category}_{col}_boxplot.png"
            plt.savefig(save_path / save_filename, dpi=300)
        
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
    stats['Max'] = df[columns].max()
    stats['Min'] = df[columns].min()
    stats['Mean'] = df[columns].mean()
    stats['Median'] = df[columns].median()
    stats['Standard Deviation'] = df[columns].std()
    
    # Calculate percentiles
    stats['25th Percentile'] = df[columns].apply(lambda col: np.percentile(col, 25))
    stats['75th Percentile'] = df[columns].apply(lambda col: np.percentile(col, 75))
   
    return stats



def compute_z_scores(df: pd.DataFrame, columns: list, ) -> pd.DataFrame:
    """
    Compute Z-score, Robust Z-score, and Modified Z-score for given columns.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame containing numeric columns.
    columns : list[str]
        List of column names for which to compute the scores.

    Returns
    -------
    pd.DataFrame
        Original DataFrame with additional columns for computed scores.
    """

    df = df.copy()  # Avoid modifying the original DataFrame
    
    for col in columns:
        # Compute mean and standard deviation for Z-score
        mean_col = df[col].mean()
        std_col = df[col].std()

        # Compute Median, MAD, and IQR for robust statistics
        median_col = df[col].median()
        mad_col = np.median(np.abs(df[col] - median_col))  # Median Absolute Deviation (MAD)
        q1 = np.percentile(df[col], 25)
        q3 = np.percentile(df[col], 75)
        iqr = q3 - q1  # Interquartile Range

        # Compute Z-score
        df[f'{col}_zscore'] = (df[col] - mean_col) / std_col

        # Compute Robust Z-score (based on Median & IQR)
        df[f'{col}_robust_zscore'] = (df[col] - median_col) / iqr

        # Compute Modified Z-score (based on MAD)
        df[f'{col}_modified_zscore'] = 0.6745 * (df[col] - median_col) / mad_col

    return df

def plot_zscore_distributions(df: pd.DataFrame, columns: list[str], save_path: pathlib.Path = None, category: str = 'Res'):
    """
    Plot the distribution of Z-score, Robust Z-score, and Modified Z-score for selected columns.
    Ensures uniform y-axis scaling and includes vertical lines at the 25th and 75th percentiles.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing the Z-score, Robust Z-score, and Modified Z-score.
    columns : list[str]
        List of column names for which the distributions should be plotted.
    save_path : pathlib.Path, optional
        Directory path where plots should be saved (default is None, meaning plots won't be saved).
    category : str, optional
        Category name to include in the saved filename (default is 'Res').
    """

    num_cols = len(columns)
    fig, axes = plt.subplots(num_cols, 3, figsize=(15, 5 * num_cols), sharey=True)  # Ensure same y-scale

    # Determine the maximum y-axis limit across all histograms
    max_y = 0
    for col in columns:
        for suffix in ['','_zscore', '_robust_zscore', '_modified_zscore']:
            counts, _ = np.histogram(df[f'{col}{suffix}'], bins=30)
            max_y = max(max_y, max(counts))

    for i, col in enumerate(columns):
        for j, suffix in enumerate(['zscore', 'robust_zscore', 'modified_zscore']):
            ax = axes[i, j]
            data = df[f'{col}_{suffix}']

            sns.histplot(data, bins=30, kde=True, ax=ax)
            ax.set_title(f"{suffix.replace('_', ' ').title()} Distribution: {col}")
            ax.axvline(x=0, color='red', linestyle='--', label="Mean/Median")  # Mean/Median line

            # Compute and plot 25th & 75th percentile lines
            p25, p75 = np.percentile(data, [25, 75])
            ax.axvline(x=p25, color='grey', linestyle='dotted', alpha=0.7, label="25th Percentile")
            ax.axvline(x=p75, color='grey', linestyle='dotted', alpha=0.7, label="75th Percentile")

            ax.set_ylim(0, max_y * 1)  # Set the same y-axis scale across plots

    plt.tight_layout()

    # Save the figure if save_path is provided
    if save_path:
        save_path.mkdir(parents=True, exist_ok=True)  # Ensure the directory exists
        save_filename = f"{category}_zscore_distributions.png"
        plt.savefig(save_path / save_filename, dpi=300, bbox_inches='tight')
        print(f"Plot saved at: {save_path / save_filename}")

    plt.close()


