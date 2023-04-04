# -*- coding: utf-8 -*-
"""Script to analyse the TRICS sites lists."""

##### IMPORTS #####
# Standard imports
import calendar
import dataclasses
import datetime as dt
import logging
import pathlib
import re
from typing import Iterator

# Third party imports
import caf.toolkit
import numpy as np
import pandas as pd
import pydantic

# Local imports

##### CONSTANTS #####
LOG = logging.getLogger(__name__)
LOG_FILE = "TRICS_analysis.log"
EXPECTED_TRICS_LAND_USES = {
    1: "ACDEFGHIJKLMNORS",
    2: "ABCDEFG",
    3: "ABCDFGJKLMNOP",
    4: "ABCDF",
    5: "ABCDEFGHJKLMN",
    6: "ABCDFGHIJKL",
    7: "ABCGIJKLMOPQTUVX",
    8: "A",
    9: "ABCEG",
    10: "A",
    12: "AC",
    13: "ABC",
    14: "A",
    15: "ABCD",
    16: "ABC",
    17: "ABC",
}

##### CLASSES #####
class TRICSAnalysisParameters(caf.toolkit.BaseConfig):
    """Parameters for TRICS site lists analysis script."""

    trics_data_folder: pydantic.DirectoryPath  # pylint: disable=no-member
    output_folder: pydantic.DirectoryPath  # pylint: disable=no-member
    landuse_lookup: pydantic.FilePath  # pylint: disable=no-member
    sub_landuse_lookup: pydantic.FilePath  # pylint: disable=no-member


@dataclasses.dataclass
class _TRICSSummaries:
    """TRICS site summaries aggregated to various levels of land use."""

    all: pd.DataFrame
    land_use: pd.DataFrame
    sub_land_use: pd.DataFrame

    @property
    def names(self) -> list[str]:
        """Readable names of different attributes."""
        return ["All Sites", "Land Use", "Sub-Land Use"]

    def __iter__(self) -> Iterator[tuple[str, pd.DataFrame]]:
        values = [self.all, self.land_use, self.sub_land_use]
        yield from zip(self.names, values)


##### FUNCTIONS #####
def combine_trics_site_lists(folder: pathlib.Path) -> tuple[pd.DataFrame, pathlib.Path]:
    """Combine TRICS site lists CSVs by land uses into a single CSV.

    Parameters
    ----------
    folder : pathlib.Path
        Folder containing TRICS site CSVs.

    Returns
    -------
    pd.DataFrame
        All TRICS sites data.
    pathlib.Path
        Path to CSV containing all TRICS sites data.
    """
    pattern = r"TRICS_sites_(\d+)(\w).csv"
    regex = re.compile(pattern, re.I)

    missing_landuses = [
        f"{i}{j}" for i, lu in EXPECTED_TRICS_LAND_USES.items() for j in lu
    ]
    extra_landuses = []
    all_sites: list[pd.DataFrame] = []
    for file in folder.iterdir():
        match = regex.match(file.name)
        if match is None:
            LOG.warning("Found unexpected file: %s", file.name)
            continue

        landuse = int(match.group(1))
        sub_landuse = match.group(2).upper()
        lu_code = f"{landuse}{sub_landuse}"
        if lu_code in missing_landuses:
            missing_landuses.remove(lu_code)
        else:
            extra_landuses.append(lu_code)

        sites = pd.read_csv(file, encoding="cp1252")
        if sites.empty:
            LOG.warning("%s file is empty", file.name)
            continue

        sites.loc[:, "land_use"] = landuse
        sites.loc[:, "sub_land_use"] = sub_landuse
        all_sites.append(sites)

    all_sites: pd.DataFrame = pd.concat(all_sites)
    all_sites.loc[:, "Most Recent Survey"] = pd.to_datetime(
        all_sites["Most Recent Survey"]
    )
    all_sites.loc[:, "GFA Ratio"] = np.divide(
        all_sites["GFA"],
        all_sites["EMPLOY"],
        out=np.full_like(all_sites["GFA"], np.nan),
        where=(all_sites["GFA"] != 0) & (all_sites["EMPLOY"] != 0),
    )

    output_file = folder / "TRICS_sites.csv"
    all_sites.to_csv(output_file, index=False, encoding="utf-8")

    LOG.info(
        "Written: %s\nMissing land use codes: %s\nExtra land use codes: %s",
        output_file,
        ", ".join(missing_landuses),
        ", ".join(extra_landuses),
    )
    return all_sites, output_file


def load_landuse_lookups(
    landuse_path: pathlib.Path, sub_landuse_path: pathlib.Path
) -> tuple[dict[int, str], dict[str, str]]:
    """Load land sue lookup files as dictionaries.

    Parameters
    ----------
    landuse_path : pathlib.Path
        Path to land use lookup CSV with columns 'land_use' and 'name'.
    sub_landuse_path : pathlib.Path
        Path to sub land use lookup CSV with columns
        'land_use', 'sub_land_use' and 'name'.

    Returns
    -------
    dict[int, str]
        Lookup between land use code and name.
    dict[str, str]
        Lookup between sub land use code and name.
    """
    landuse_lookup = pd.read_csv(landuse_path)
    landuse_lookup = landuse_lookup.set_index("land_use")["name"].to_dict()

    sub_landuse_lookup = pd.read_csv(sub_landuse_path)
    sub_landuse_lookup.loc[:, "code"] = sub_landuse_lookup.apply(
        lambda row: f"{row['land_use']!s:0>2.2}{row['sub_land_use'].upper().strip()}",
        axis=1,
    )
    sub_landuse_lookup = sub_landuse_lookup.set_index("code")["name"].to_dict()

    return landuse_lookup, sub_landuse_lookup


def _summarise_site_data(data: pd.DataFrame) -> pd.DataFrame:
    """Calculate summary stats for some of the columns in `data`.

    Calculates count, mean, min, percentiles and max for columns:
    "GFA", "EMPLOY", "GFA Ratio", "Most Recent Survey". Calculates
    count of unique values for "Day of Week" column.
    """
    data_summary: list[pd.DataFrame] = []
    for column in ("GFA", "EMPLOY", "GFA Ratio", "Most Recent Survey"):
        values = data[column]
        percentiles = values.quantile([0.15, 0.5, 0.85]).to_dict()

        summary = {
            "Count": values.count(),
            "Mean": values.mean(),
            "Min": values.min(),
            **{f"{k:.0%}": v for k, v in percentiles.items()},
            "Max": values.max(),
        }

        if column == "Most Recent Survey":
            summary = {k: v.date() if k != "Count" else v for k, v in summary.items()}

        summary = pd.DataFrame(summary, index=[0])
        summary.columns = pd.MultiIndex.from_product([[column], summary.columns])
        data_summary.append(summary)

    days_count = dict(zip(*np.unique(data["Day of Week"], return_counts=True)))
    # Sort days into weekday order
    days_count = {k: days_count.get(k, 0) for k in calendar.day_name}

    data_summary.append(
        pd.DataFrame(
            {("Count of Day of Week", k): v for k, v in days_count.items()}, index=[0]
        )
    )

    summary_df = pd.concat(data_summary, axis=1)
    summary_df.insert(0, ("Total Rows", ""), len(data))
    return summary_df


def _write_summaries(summaries: _TRICSSummaries, excel_file: pathlib.Path) -> None:
    """Write summaries to separate sheets in `excel_file`."""
    with pd.ExcelWriter(excel_file) as excel:  # pylint: disable=E0110
        for name, data in summaries:
            data.to_excel(excel, sheet_name=name)
    LOG.info("Written: %s", excel_file)


def summarise_trics_sites(
    trics_sites: pd.DataFrame,
    landuse_lookup: dict[int, str],
    sub_landuse_lookup: dict[str, str],
    excel_file: pathlib.Path,
) -> _TRICSSummaries:
    """Calculate summaries for TRICS sites data.

    Summarise for different land use aggregations. Calculate
    count, mean, min, percentiles and max for columns: "GFA",
    "EMPLOY", "GFA Ratio", "Most Recent Survey". Calculates
    count of unique values for "Day of Week" column.

    Parameters
    ----------
    trics_sites : pd.DataFrame
        TRICS site data.
    landuse_lookup : dict[int, str]
        Lookup from land use code to name.
    sub_landuse_lookup : dict[str, str]
        Lookup from sub land use code to name.
    excel_file : pathlib.Path
        Path to Excel file to create with summaries.

    Returns
    -------
    _TRICSSummaries
        Summary dataframes.
    """
    LOG.info("Summarising TRICS sites")
    summaries = _TRICSSummaries(
        all=_summarise_site_data(trics_sites),
        land_use=trics_sites.groupby("land_use").apply(_summarise_site_data),
        sub_land_use=trics_sites.groupby(["land_use", "sub_land_use"]).apply(
            _summarise_site_data
        ),
    )

    summaries.all.index = ["All Sites"]

    landuse = summaries.land_use.index.get_level_values(0)
    names = pd.Series(landuse).replace(landuse_lookup)
    summaries.land_use.index = pd.MultiIndex.from_arrays(
        [landuse, names], names=["Land Use Code", "Land Use Name"]
    )

    landuse = summaries.sub_land_use.index.get_level_values(0)
    sub_landuse = summaries.sub_land_use.index.get_level_values(1)
    codes = pd.Series([f"{i!s:0>2.2}{j.upper()}" for i, j in zip(landuse, sub_landuse)])
    summaries.sub_land_use.index = pd.MultiIndex.from_arrays(
        [landuse, sub_landuse, codes.replace(sub_landuse_lookup)],
        names=["Land Use Code", "Sub-Land Use Code", "Sub-Land Use Name"],
    )

    _write_summaries(summaries, excel_file)
    return summaries


def _prepend_index_level(
    data: pd.DataFrame, index_value: str, level_name: str
) -> pd.DataFrame:
    """Add `index_value` as new level of index."""
    indices = [data.index.get_level_values(i) for i in range(data.index.nlevels)]
    data.index = pd.MultiIndex.from_arrays(
        [[index_value] * len(data)] + indices, names=[level_name, *data.index.names]
    )
    return data


def combine_summaries(
    summaries: dict[str, _TRICSSummaries], excel_file: pathlib.Path
) -> None:
    """Combine summaries from different sources.

    Uses dictionary key as new index value so
    indices are unique on new dataframe.

    Parameters
    ----------
    summaries : dict[str, _TRICSSummaries]
        Summaries with their names.
    excel_file : pathlib.Path
        Path to Excel file to create with the combined
        summaries.
    """
    all_: list[pd.DataFrame] = []
    land_use: list[pd.DataFrame] = []
    sub_land_use: list[pd.DataFrame] = []

    level_name = "Date Filter"
    for name, summary in summaries.items():
        all_.append(_prepend_index_level(summary.all.copy(), name, level_name))
        land_use.append(_prepend_index_level(summary.land_use.copy(), name, level_name))
        sub_land_use.append(
            _prepend_index_level(summary.sub_land_use.copy(), name, level_name)
        )

    summary = _TRICSSummaries(
        all=pd.concat(all_),
        land_use=pd.concat(land_use),
        sub_land_use=pd.concat(sub_land_use),
    )
    _write_summaries(summary, excel_file)


def main(parameters: TRICSAnalysisParameters, init_logging: bool = True) -> None:
    """Run analysis of TRICS site data and output summaries."""
    if init_logging:
        filehandler = logging.FileHandler(parameters.output_folder / LOG_FILE)
        filehandler.setFormatter(
            logging.Formatter("{asctime} [{levelname:^8.8}] {message}", style="{")
        )
        logging.basicConfig(
            handlers=[logging.StreamHandler(), filehandler],
            level=logging.DEBUG,
            format="[{levelname:^8.8}] {message}",
            style="{",
        )
    LOG.info("Running TRICS Analysis")

    trics_sites, _ = combine_trics_site_lists(parameters.trics_data_folder)

    landuse_lookup, sub_landuse_lookup = load_landuse_lookups(
        parameters.landuse_lookup, parameters.sub_landuse_lookup
    )

    summaries: dict[str, _TRICSSummaries] = {}
    summaries["All Dates"] = summarise_trics_sites(
        trics_sites,
        landuse_lookup,
        sub_landuse_lookup,
        parameters.output_folder / "TRICS_site_summaries-all.xlsx",
    )
    for date_filter in (2010, 2015, 2018):
        date_filter = dt.datetime(date_filter, 1, 1)
        summaries[f"From {date_filter:%Y}"] = summarise_trics_sites(
            trics_sites.loc[trics_sites["Most Recent Survey"] >= date_filter],
            landuse_lookup,
            sub_landuse_lookup,
            parameters.output_folder
            / f"TRICS_site_summaries-from_{date_filter:%Y}.xlsx",
        )

    combine_summaries(
        summaries, parameters.output_folder / "TRICS_site_summaries_by_date.xlsx"
    )


def _run() -> None:
    trics_data_folder = pathlib.Path(
        r"C:\Users\ukmjb018\OneDrive - WSP O365\WSP_Projects\TfN NorMITs Demand Partner 2022\D-Lit Land Use\Inputs\TRICS Sites"
    )

    parameters = TRICSAnalysisParameters(
        trics_data_folder=trics_data_folder,
        output_folder=trics_data_folder / "analysis",
        landuse_lookup=trics_data_folder / "TRICS_landuse_codes.csv",
        sub_landuse_lookup=trics_data_folder / "TRICS_sub_landuse_codes.csv",
    )
    main(parameters)


##### MAIN #####
if __name__ == "__main__":
    _run()
