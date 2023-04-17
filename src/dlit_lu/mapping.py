# -*- coding: utf-8 -*-
"""Functionality for creating static graphs and maps using matplotlib."""

##### IMPORTS #####
# Standard imports
from __future__ import annotations
import dataclasses
import logging
from typing import NamedTuple

# Third party imports
import geopandas as gpd
import mapclassify
from matplotlib import cm, patches, pyplot as plt
import numpy as np
import pandas as pd

# Local imports

##### CONSTANTS #####
LOG = logging.getLogger(__name__)

##### CLASSES #####
@dataclasses.dataclass
class CustomCmap:
    """Store information about a custom colour map."""

    bin_categories: pd.Series
    colours: pd.DataFrame
    legend_elements: list[patches.Patch]

    def __add__(self, other: CustomCmap | None) -> CustomCmap:
        """Return new CustomCmap with the attributes from `self` and `other` concatenated."""
        if not isinstance(other, CustomCmap):
            raise TypeError(f"other should be a CustomCmap not {type(other)}")

        return CustomCmap(
            pd.concat(
                [self.bin_categories, other.bin_categories], verify_integrity=True
            ),
            pd.concat([self.colours, other.colours], verify_integrity=True),
            self.legend_elements + other.legend_elements,
        )

    @classmethod
    def new_empty(cls) -> CustomCmap:
        """Create a new CustomCmap with no data."""
        return cls(
            bin_categories=pd.Series(dtype=int),
            colours=pd.DataFrame(dtype=float),
            legend_elements=[],
        )

    @property
    def empty(self) -> bool:
        """Whether or not the CustomCmap has data (False) or is empty (True)."""
        return self.bin_categories.empty


class Bounds(NamedTuple):
    """Coordinates for geospatial extent."""

    min_x: int
    min_y: int
    max_x: int
    max_y: int


##### FUNCTIONS #####
def mapclassify_natural(
    y: np.ndarray,
    k: int = 5,  # pylint: disable=invalid-name
    initial: int = 10,
) -> mapclassify.NaturalBreaks:
    """Try smaller values of k on error of NaturalBreaks.

    Parameters
    ----------
    y : np.ndarray
        (n,1), values to classify
    k : int, optional, default 5
        number of classes required
    initial : int, default 10
        Number of initial solutions generated with different centroids.
        Best of initial results is returned.

    Returns
    -------
    mapclassify.NaturalBreaks
    """
    while True:
        try:
            return mapclassify.NaturalBreaks(y, k, initial)
        except ValueError:
            if k <= 2:
                raise
            k -= 1


def colormap_classify(
    data: pd.Series,
    cmap_name: str,
    bins: list[int | float] | int = 5,
    label_fmt: str = "{:.0f}",
    nan_colour: tuple[float, float, float, float] | None = None,
) -> CustomCmap:
    """Classify `data` into colormap bins based on NaturalBreaks.

    Parameters
    ----------
    data : pd.Series
        Data to classify.
    cmap_name : str
        Name of colormap to use.
    bins : list[int | float] | int, default 5
        Number of bins to classify using NaturalBreaks,
        or pre-defined bin edges.
    label_fmt : str, default "{:.0f}"
        Numeric format to use for the legend.
    nan_colour : tuple[float, float, float, float], default red
        RGBA values (0 - 1) to use for coloring NaN values.

    Returns
    -------
    CustomCmap
        Color definitions for given `data`.
    """

    def make_label(lower: float, upper: float) -> str:
        if lower == -np.inf:
            return "< " + label_fmt.format(upper)
        if upper == np.inf:
            return "> " + label_fmt.format(lower)
        return label_fmt.format(lower) + " - " + label_fmt.format(upper)

    finite = data.dropna()
    if finite.empty:
        # Return empty colour map
        return CustomCmap(pd.Series(dtype=float), pd.DataFrame(dtype=float), [])

    if isinstance(bins, int):
        mc_bins = mapclassify_natural(finite, bins)
    else:
        mc_bins = mapclassify.UserDefined(finite, bins)

    bin_categories = pd.Series(mc_bins.yb, index=finite.index)

    cmap = cm.get_cmap(cmap_name, mc_bins.k)
    # Cmap produces incorrect results if given floats instead of int so
    # bin_categories can't contain Nans until after colours are calculated
    colours = pd.DataFrame(
        cmap(bin_categories.astype(int)),
        index=bin_categories.index,
        columns=iter("RGBA"),
    )

    bin_categories = bin_categories.reindex_like(data)
    colours = colours.reindex(bin_categories.index)

    if nan_colour is None:
        colours.loc[bin_categories.isna(), :] = np.nan
    else:
        colours.loc[bin_categories.isna(), :] = nan_colour

    min_bin = np.min(finite)
    if min_bin > mc_bins.bins[0]:
        if mc_bins.bins[0] > 0:
            min_bin = 0
        else:
            min_bin = -np.inf

    bins = [min_bin, *mc_bins.bins]
    labels = [make_label(l, u) for l, u in zip(bins[:-1], bins[1:])]
    legend = [
        patches.Patch(fc=c, label=l, ls="")
        for c, l in zip(cmap(range(mc_bins.k)), labels)
    ]

    if nan_colour is not None:
        legend.append(patches.Patch(fc=nan_colour, label="Missing Values", ls=""))

    return CustomCmap(bin_categories, colours, legend)


def heatmap_figure(
    geodata: gpd.GeoDataFrame,
    column_name: str,
    title: str,
    bins: list[int | float] | int = 5,
    legend_label_fmt: str = "{:.1%}",
    legend_title: str | None = None,
    zoomed_bounds: Bounds | None = None,
    footnote: str | None = None,
) -> plt.Figure:
    """Create a heatmap of `geodata`.

    Parameters
    ----------
    geodata : gpd.GeoDataFrame
        Data for plotting.
    column_name : str
        Name of column in `geodata` to use for heatmap.
    title : str
        Plot title.
    bins : list[int | float] | int, default 5
        Number of bins to classify using NaturalBreaks,
        or pre-defined bin edges.
    legend_label_fmt : str, default "{:.1%}"
        Numeric format for the legend.
    legend_title : str, optional
        Title to use for the legend.
    zoomed_bounds : Bounds, optional
        Area to zoom into on a sub-plot, if not given no
        sub-plot created.
    footnote : str | None, optional
        Footnote to add to the bottom of the plot.

    Returns
    -------
    plt.Figure
        Matplotlib figure of heatmap.

    Raises
    ------
    ValueError
        If an empty list of bins is given or there are
        no values to plot.
    TypeError
        If the wrong type is given for bins.
    """
    ncols = 1 if zoomed_bounds is None else 2

    fig, axes = plt.subplots(
        1, ncols, figsize=(10, 15), frameon=False, constrained_layout=True
    )
    if ncols == 1:
        axes = [axes]

    fig.suptitle(title, fontsize="xx-large", backgroundcolor="white")
    for ax in axes:
        ax.set_aspect("equal")
        ax.set_xticklabels([])
        ax.set_yticklabels([])
        ax.tick_params(length=0)
        ax.set_axis_off()

    if isinstance(bins, int):
        positive_bins, negative_bins = bins, bins

    elif isinstance(bins, tuple, list):
        if len(bins) == 0:
            raise ValueError("empty list of bins given")

        positive_bins = list(filter(lambda x: x >= 0, bins))
        negative_bins = list(filter(lambda x: x <= 0, bins))

        if len(negative_bins) == 0:
            negative_bins = [-1 * i for i in positive_bins]
        elif len(positive_bins) == 0:
            positive_bins = [abs(i) for i in negative_bins]

    else:
        raise TypeError(f"bins should be an int or list not: '{type(bins)}'")

    cmap = CustomCmap.new_empty()

    # Calculate, and apply, separate colormaps for positive and negative values
    negative_data = geodata.loc[geodata[column_name] < 0, column_name]
    if len(negative_data) > 0:
        cmap += colormap_classify(
            negative_data, "PuBu_r", negative_bins, label_fmt=legend_label_fmt
        )

    positive_data = geodata.loc[
        (geodata[column_name] >= 0) | (geodata[column_name].isna()), column_name
    ]
    if len(positive_data) > 0:
        cmap += colormap_classify(
            positive_data,
            "YlGn",
            positive_bins,
            label_fmt=legend_label_fmt,
            nan_colour=(1.0, 0.0, 0.0, 1.0),
        )

    if cmap.empty:
        raise ValueError("No values to plot")

    # Update colours index to be the same order as geodata
    cmap.colours = cmap.colours.reindex(geodata.index)

    for ax in axes:
        geodata.plot(ax=ax, color=cmap.colours.values, linewidth=0.1, edgecolor="black")

    axes[ncols - 1].legend(
        handles=cmap.legend_elements,
        title=legend_title,
        title_fontsize="large",
        fontsize="medium",
    )

    if ncols == 2 and zoomed_bounds is not None:
        axes[1].set_xlim(zoomed_bounds.min_x, zoomed_bounds.max_x)
        axes[1].set_ylim(zoomed_bounds.min_y, zoomed_bounds.max_y)

    if footnote is not None:
        axes[ncols - 1].annotate(
            footnote,
            xy=(0.9, 0.01),
            xycoords="figure fraction",
            bbox=dict(boxstyle="square", fc="white"),
        )

    return fig
