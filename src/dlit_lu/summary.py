# -*- coding: utf-8 -*-
"""Functionality for aggregating the land use outputs and creating summaries."""

##### IMPORTS #####
# Standard imports
import datetime as dt
import dataclasses
import logging
import pathlib

# Third party imports
import geopandas as gpd
from matplotlib import pyplot as plt
from matplotlib.backends import backend_pdf
import numpy as np
import pandas as pd

# Local imports
from dlit_lu import inputs, mapping

##### CONSTANTS #####
LOG = logging.getLogger(__name__)
LAND_USE_ZONING = "msoa"
PLOT_YEAR_GAP = 10


##### CLASSES #####
@dataclasses.dataclass
class SummaryLookup:
    """Data for translating to the summary zone system."""

    lookup: pd.DataFrame
    from_zone_column: str
    to_zone_column: str
    split_column: str


##### FUNCTIONS #####
def load_summary_lookup(
    parameters: inputs.SummaryInputs,
) -> tuple[SummaryLookup, gpd.GeoDataFrame]:
    """Load summary lookup and spatial data.

    Parameters
    ----------
    parameters : inputs.SummaryInputs
        Summary input parameters.

    Returns
    -------
    SummaryLookup,
        Data for translating to the summary zone system.
    gpd.GeoDataFrame
        Geospatial data for summary zone system.
    """

    from_zone = f"{LAND_USE_ZONING}_zone_id"
    to_zone = f"{parameters.summary_zone_name}_zone_id"
    split_column = f"{LAND_USE_ZONING}_to_{parameters.summary_zone_name}"
    lookup = pd.read_csv(
        parameters.lookup_file,
        usecols=[from_zone, to_zone, split_column],
        dtype={from_zone: str, to_zone: str, split_column: float},
    )
    summary_lookup = SummaryLookup(lookup, from_zone, to_zone, split_column)

    shapefile: gpd.GeoDataFrame = gpd.read_file(parameters.shapefile)[
        [parameters.shapefile_id_column, "geometry"]
    ]

    if parameters.geometry_simplify_tolerance is not None:
        LOG.info(
            "Simplifing geometries with tolerance of %s",
            parameters.geometry_simplify_tolerance,
        )
        shapefile.loc[:, "geometry"] = shapefile.simplify(
            parameters.geometry_simplify_tolerance, preserve_topology=False
        )

    shapefile = shapefile.set_index(parameters.shapefile_id_column)

    return summary_lookup, shapefile


def translate_zoning(data: pd.DataFrame, summary_lookup: SummaryLookup) -> pd.DataFrame:
    """Translate `data` to summary zone system.

    Parameters
    ----------
    data : pd.DataFrame
        Data at `LAND_USE_ZONING`.
    summary_lookup : SummaryLookup
        Lookup to perform translation.

    Returns
    -------
    pd.DataFrame
        Data at summary zone system.
    """
    index_columns = data.index.names

    merged = data.reset_index().merge(
        summary_lookup.lookup,
        on=summary_lookup.from_zone_column,
        how="left",
        indicator=True,
    )
    missing = merged["_merge"] != "both"
    merged.drop(columns="_merge", inplace=True)
    if missing.sum() > 0:
        value, count = np.unique(
            missing.replace({"left_only": "data_only", "right_only": "lookup_only"}),
            return_counts=True,
        )
        LOG.warning(
            "Lookup and data don't contain all the same zones: %s",
            ", ".join(f"{i}: {j:,}" for i, j in zip(value, count)),
        )

    for column in merged.columns:
        if column in summary_lookup.lookup.columns or column in index_columns:
            continue
        merged.loc[:, column] = merged[column] * merged[summary_lookup.split_column]

    merged.drop(
        columns=[summary_lookup.from_zone_column, summary_lookup.split_column],
        inplace=True,
    )

    if index_columns is None:
        index_columns = [summary_lookup.to_zone_column]
    elif summary_lookup.to_zone_column not in index_columns:
        index_columns = [summary_lookup.to_zone_column] + index_columns

    index_columns = [i for i in index_columns if i != summary_lookup.from_zone_column]

    return merged.groupby(index_columns).sum()


def summary_spreadsheet(summary: pd.DataFrame, excel_file: pathlib.Path) -> None:
    """Create spreadsheet with `summary` grouped by each index level.

    Parameters
    ----------
    summary : pd.DataFrame
        Data to write to spreadsheet.
    excel_file : pathlib.Path
        Path to create Excel file at.
    """
    with pd.ExcelWriter(  # pylint: disable=abstract-class-instantiated
        excel_file, engine="openpyxl"
    ) as excel:
        summary.to_excel(excel, sheet_name="All")

        for index in summary.index.names:
            grouped = summary.groupby(level=index).sum()
            grouped.to_excel(excel, sheet_name=index)

    LOG.info("Written summaries to %s", excel_file)


def _plot_all_columns(
    data: gpd.GeoDataFrame,
    output_file: pathlib.Path,
    title: str,
    footnote: str | None = None,
) -> None:
    """Create heatmaps for each column in `data`."""
    with backend_pdf.PdfPages(output_file) as pdf:
        for column in data.select_dtypes("number").columns:
            fig = mapping.heatmap_figure(
                data,
                column,
                title,
                bins=7,
                legend_title=f"Year {column}",
                legend_label_fmt="{:.2g}",
                footnote=footnote,
                zoomed_bounds=mapping.Bounds(290000, 345000, 555000, 660000),
            )
            pdf.savefig(fig)
            plt.close(fig)

    LOG.info("Written: %s", output_file)


def plot_summaries(
    summary: gpd.GeoDataFrame,
    zone_column: str,
    output_file: pathlib.Path,
    data_name: str,
) -> None:
    """Create summary heatmap for each index column.

    Parameters
    ----------
    summary : gpd.GeoDataFrame
        Geospatial data to plot.
    zone_column : str
        Name of column containing zone IDs.
    output_file : pathlib.Path
        Base file path to write outputs to, various outputs
        will be created using this as the base.
    data_name : str
        Name of the data type being plotted,
        used in figure titles.
    """
    # Plot summary for each combination of zone system and 1 index column,
    # then plot zone totals
    index_columns = [i for i in summary.index.names if i != zone_column]
    zone_name = zone_column.lower().replace("zone_id", "")
    zone_name = " ".join(zone_name.split("_")).upper()
    footnote = (
        f"Data from DLIT, plotted at {zone_name} zoning. "
        f"Produced on {dt.date.today():%Y-%m-%d}"
    )

    LOG.info("Creating %s total plot", zone_column)
    aggregation = dict.fromkeys(
        summary.columns, lambda x: np.nan if x.isna().all() else np.sum(x)
    )
    aggregation["geometry"] = "first"

    grouped = summary.groupby(zone_column).agg(aggregation)
    grouped = gpd.GeoDataFrame(grouped, geometry="geometry", crs=summary.crs)
    _plot_all_columns(
        grouped,
        output_file.with_name(output_file.stem + f"-{zone_column}.pdf"),
        f"Total {data_name.title()}",
        footnote,
    )

    for index in index_columns:
        grouped = summary.groupby([zone_column, index]).agg(aggregation)
        grouped = gpd.GeoDataFrame(grouped, geometry="geometry", crs=summary.crs)

        index_values = grouped.index.get_level_values(index).unique()
        if len(index_values) > 100:
            LOG.warning(
                "Index has %s unique values so not creating individual plots",
                len(index_values),
            )
            continue

        for value in index_values:
            LOG.info("Creating %s plot for value %s", index, value)
            _plot_all_columns(
                grouped.loc[:, value, :],
                output_file.with_name(
                    output_file.stem + f"-{zone_column}-{index}_{value}.pdf"
                ),
                f"{data_name.title()} {index.title()} {value}",
                footnote,
            )


def summarise_landuse(
    residential_msoa: pd.DataFrame,
    employment_msoa: pd.DataFrame,
    summary_params: inputs.SummaryInputs,
    output_folder: pathlib.Path,
) -> None:
    """Create summary spreadsheets and plots for the land use data."""
    output_folder.mkdir(exist_ok=True)
    msoa_data = {"residential": residential_msoa, "employment": employment_msoa}

    LOG.info("Summarising land use data to %s zoning", summary_params.summary_zone_name)
    summary_lookup, shapefile = load_summary_lookup(summary_params)

    for name, data in msoa_data.items():
        summary = translate_zoning(data, summary_lookup)
        excel_file = (
            output_folder / f"{name}_summary_{summary_params.summary_zone_name}.xlsx"
        )
        summary_spreadsheet(summary, excel_file)

        # Don't include any total columns in cummulative summary
        summary.drop(
            columns=[i for i in summary.columns if i.lower().startswith("total")],
            inplace=True,
        )
        excel_file = excel_file.with_name(
            f"{name}_cummulative_summary_{summary_params.summary_zone_name}.xlsx"
        )
        summary = summary.cumsum(axis=1)
        summary_spreadsheet(summary, excel_file)

        years: list[int] = []
        for column in summary.columns:
            try:
                years.append(int(column))
            except ValueError:
                pass

        plot_columns = {i for i in years if i % PLOT_YEAR_GAP == 0}
        plot_columns.add(min(years))
        plot_columns.add(max(years))
        plot_columns = [str(i) for i in sorted(plot_columns)]

        index_columns = summary.index.names
        # Add all combinations of zones and indices so plots contain all zones
        full_index = pd.MultiIndex.from_product(
            [shapefile.reset_index()[summary_params.shapefile_id_column].unique()]
            + [
                summary.index.get_level_values(i).unique().values
                for i in index_columns
                if i != summary_lookup.to_zone_column
            ],
            names=index_columns,
        )
        summary = summary.reindex(full_index)

        summary = summary.reset_index().merge(
            shapefile.reset_index(),
            left_on=summary_lookup.to_zone_column,
            right_on=summary_params.shapefile_id_column,
            how="left",
        )

        summary = gpd.GeoDataFrame(summary, crs=shapefile.crs)
        summary = summary.set_index(index_columns).loc[:, plot_columns + ["geometry"]]

        plots_folder = output_folder / f"{name} heatmaps"
        plots_folder.mkdir(exist_ok=True)
        plot_summaries(
            summary,
            summary_lookup.to_zone_column,
            plots_folder / f"{name}_summary_heatmaps.pdf",
            name,
        )
