# -*- coding: utf-8 -*-
"""Functionality for aggregating the land use outputs and creating summaries."""

##### IMPORTS #####
# Standard imports
import logging
import dataclasses
import pathlib

# Third party imports
import geopandas as gpd
import numpy as np
import pandas as pd

# Local imports
from dlit_lu import inputs

##### CONSTANTS #####
LOG = logging.getLogger(__name__)
LAND_USE_ZONING = "msoa"


##### CLASSES #####
@dataclasses.dataclass
class SummaryLookup:
    lookup: pd.DataFrame
    from_zone_column: str
    to_zone_column: str
    split_column: str


##### FUNCTIONS #####
def load_summary_lookup(
    parameters: inputs.SummaryInputs,
) -> tuple[SummaryLookup, gpd.GeoDataFrame]:
    from_zone = f"{LAND_USE_ZONING}_zone_id"
    to_zone = f"{parameters.summary_zone_name}_zone_id"
    split_column = f"{LAND_USE_ZONING}_to_{parameters.summary_zone_name}"
    lookup = pd.read_csv(
        parameters.lookup_file,
        usecols=[from_zone, to_zone, split_column],
        dtype={from_zone: str, to_zone: str, split_column: float},
    )
    summary_lookup = SummaryLookup(lookup, from_zone, to_zone, split_column)

    shapefile = gpd.read_file(parameters.shapefile)[
        [parameters.shapefile_id_column, "geometry"]
    ]
    shapefile = shapefile.set_index(parameters.shapefile_id_column)

    return summary_lookup, shapefile


def translate_zoning(
    data: pd.DataFrame,
    summary_lookup: SummaryLookup,
) -> pd.DataFrame:
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
    with pd.ExcelWriter(excel_file) as excel:
        summary.to_excel(excel, sheet_name="All")

        for index in summary.index.names:
            grouped = summary.groupby(level=index).sum()
            grouped.to_excel(excel, sheet_name=index)

    LOG.info("Written summaries to %s", excel_file)


def plot_summaries(summary: gpd.GeoDataFrame) -> ...:
    # Plot summary for each combination of zone system and 1 index column,
    # then plot zone totals
    raise NotImplementedError("WIP")


def summarise_landuse(
    residential_msoa: pd.DataFrame,
    employment_msoa: pd.DataFrame,
    summary_params: inputs.SummaryInputs,
    output_folder: pathlib.Path,
) -> None:
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
        summary = summary.cumprod(axis=1)
        summary_spreadsheet(summary, excel_file)

        continue
        # TODO(MB) Plot LADs on heatmap
        index_columns = summary.index.names
        summary = summary.reset_index().merge(
            shapefile,
            left_on=summary_lookup.from_zone_column,
            right_on=summary_params.shapefile_id_column,
        )
        summary = gpd.GeoDataFrame(summary, crs=shapefile.crs)
        summary = summary.set_index(index_columns)
        plot_summaries(summary)
