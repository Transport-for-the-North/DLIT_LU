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

def distribution_method_data(
    df: pd.DataFrame, 
    attribute: str, 
    method: str, 
    num_categories: int
) -> Tuple[pd.DataFrame, List[Tuple[float, float]]]:
    """
    Applies the specified classification method to a given attribute and divides the data into specified categories.

    Parameters:
    df: pandas.DataFrame
        The input dataframe.
    attribute: str
        The column name on which classification is performed.
    method: str
        The classification method to apply. Options include:
        - "equal_interval"
        - "equal_count"
        - "natural_breaks"
        - "logarithmic_scale"
        - "standard_deviation"
    num_categories: int
        The number of categories to divide the data into.

    Returns:
    pd.DataFrame
        The input dataframe with an additional 'category' column for the assigned category.
    list of Tuple[float, float]
        The bounds of each category as a list of tuples.
    """
    
    # Define the classification methods
    methods = {
        "equal_interval": lambda x: np.linspace(
            x.min(), x.max(), num_categories + 1
        ),
        "equal_count": lambda x: np.quantile(
            x, np.linspace(0, 1, num_categories + 1)
        ),
        "natural_breaks": lambda x: jenkspy.jenks_breaks(
            x, n_classes=num_categories
        ),
        "logarithmic_scale": lambda x: np.linspace(
            np.log1p(x).min(), np.log1p(x).max(), num_categories + 1
        ),
        "standard_deviation": lambda x: [
            x.mean() + (i - num_categories // 2) * x.std()
            for i in range(num_categories + 1)
        ],
    }

    # Ensure the method is valid
    if method not in methods:
        raise ValueError(f"Invalid method: {method}")

    # Apply the chosen classification method to the attribute
    bounds = methods[method](df[attribute])

    # Assign categories based on the calculated bounds
    df["category"] = pd.cut(
        df[attribute], bins=bounds, labels=False, right=True
    )

    # Return the updated dataframe and the category bounds
    return df, [(bounds[i], bounds[i + 1]) for i in range(num_categories)]


def plot_distribution_by_method(
    df: pd.DataFrame, 
    columns: list, 
    save_path: pathlib.Path, 
    category: str = 'Res', 
    methods: List[str] = ["equal_interval", "equal_count", "natural_breaks", "logarithmic_scale", "standard_deviation"],
    num_categories: int = 10
):
    """
    Visualize the distribution of the density variables for different classification methods.

    Parameters:
    df: pandas.DataFrame
        The input dataframe.
    columns: list of str
        List of column names to visualize.
    category: str, optional (default is 'General')
        Category of the data to differentiate between residential and employment.
    methods: list of str, optional (default includes all methods)
        List of classification methods to apply.
    num_categories: int, optional (default is 6)
        The number of categories to divide the data into.
    """
    for method in methods:
        for col in columns:
            # Apply the chosen distribution method to the column
            df, bounds = distribution_method_data(df, col, method, num_categories)
            category_counts = df["category"].value_counts().sort_index()
            category_labels = [
                f"Cat {i + 1}: {bounds[i][0]:.2f} - {bounds[i][1]:.2f}"
                for i in range(len(category_counts))
            ]            
            # Plot the distribution after classification
            plt.figure(figsize=(10, 6))
            plt.bar(category_labels, category_counts.values, color="skyblue")  # Use category_counts.values instead of "counts"

            # Calculate key statistics
            mean = df[col].mean()
            percentile_25 = np.percentile(df[col], 25)
            percentile_75 = np.percentile(df[col], 75)

            # Add vertical lines for mean, 25th, and 75th percentiles
            plt.axvline(mean, color='red', linestyle='--', label=f'Mean: {mean:.4f}')
            plt.axvline(percentile_25, color='green', linestyle='--', label=f'25th Percentile: {percentile_25:.4f}')
            plt.axvline(percentile_75, color='orange', linestyle='--', label=f'75th Percentile: {percentile_75:.4f}')

            plt.title(f'Distribution of {col} - {category} ({method})')
            plt.xlabel(col)
            plt.ylabel('Frequency')

            # Add the category counts and labels as annotations
            for i, label in enumerate(category_labels):
                plt.text(
                    0.95, 0.90 - i * 0.05,  # Adjust x and y positions
                    f"{label}: {category_counts.iloc[i]}",
                    horizontalalignment='right', verticalalignment='top',
                    transform=plt.gca().transAxes, fontsize=10, color='black', weight='bold'
                )

            plt.legend()

            # # Adjust x-ticks to show finer intervals
            # x_min, x_max = df[col].min(), df[col].max()
            # plt.xticks(np.linspace(x_min, x_max, num=15))  # Adjust 'num' for more or fewer ticks
            plt.xticks(rotation=45, ha="right")  # Rotate x-axis labels for better readability 

            # If save_path is provided, save the figure to that directory with method and category in filename
            if save_path:
                save_filename = f"{category}_{col}_{method}_distribution.png"
                plt.savefig(save_path / save_filename, dpi=300)
            plt.close()


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


def plot_zscore_distributions_2(df: pd.DataFrame, columns: list[str], save_path: pathlib.Path = None, category: str = 'Res'):
    """
    Plot the distribution of Z-score, Robust Z-score, and Modified Z-score for selected columns.

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
    fig, axes = plt.subplots(num_cols, 3, figsize=(15, 5 * num_cols))  # Create subplots

    for i, col in enumerate(columns):
        sns.histplot(df[f'{col}_zscore'], bins=30, kde=True, ax=axes[i, 0])
        axes[i, 0].set_title(f"Z-score Distribution: {col}")
        axes[i, 0].axvline(x=0, color='red', linestyle='--')  # Mean line

        sns.histplot(df[f'{col}_robust_zscore'], bins=30, kde=True, ax=axes[i, 1])
        axes[i, 1].set_title(f"Robust Z-score Distribution: {col}")
        axes[i, 1].axvline(x=0, color='red', linestyle='--')  # Median line

        sns.histplot(df[f'{col}_modified_zscore'], bins=30, kde=True, ax=axes[i, 2])
        axes[i, 2].set_title(f"Modified Z-score Distribution: {col}")
        axes[i, 2].axvline(x=0, color='red', linestyle='--')  # Median line

    plt.tight_layout()

    # Save the figure if save_path is provided
    if save_path:
        save_path.mkdir(parents=True, exist_ok=True)  # Ensure the directory exists
        save_filename = f"{category}_zscore_distributions.png"
        plt.savefig(save_path / save_filename, dpi=300, bbox_inches='tight')
        print(f"Plot saved at: {save_path / save_filename}")

    plt.show()



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


