# -*- coding: utf-8 -*-
"""Script to analyse the TRICS sites lists."""

##### IMPORTS #####
# Standard imports
import datetime as dt
import logging
import pathlib
import re
import numpy as np

# Third party imports
import pandas as pd

# Local imports

##### CONSTANTS #####
LOG = logging.getLogger(__name__)
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

##### FUNCTIONS #####
def combine_trics_site_lists(folder: pathlib.Path) -> tuple[pd.DataFrame, pathlib.Path]:
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

    data_summary: list[pd.DataFrame] = []
    for column in ("GFA", "EMPLOY", "GFA Ratio", "Most Recent Survey"):
        values = data[column]
        percentiles = values.quantile([0.1, 0.25, 0.5, 0.75, 0.9]).to_dict()

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

    days, count = np.unique(data["Day of Week"], return_counts=True)
    data_summary.append(
        pd.DataFrame(
            {("Count of Day of Week", k): v for k, v in zip(days, count)}, index=[0]
        )
    )

    summary_df = pd.concat(data_summary, axis=1)
    summary_df.insert(0, ("Total Rows", ""), len(data))
    return summary_df


def summarise_trics_sites(
    trics_sites: pd.DataFrame,
    landuse_lookup: dict[int, str],
    sub_landuse_lookup: dict[str, str],
    excel_file: pathlib.Path,
) -> None:
    LOG.info("Summarising TRICS sites")
    summaries = {
        "All Sites": _summarise_site_data(trics_sites),
        "Land Use": trics_sites.groupby("land_use").apply(_summarise_site_data),
        "Sub-Land Use": trics_sites.groupby(["land_use", "sub_land_use"]).apply(
            _summarise_site_data
        ),
    }

    summaries["All Sites"].index = ["All Sites"]

    landuse = summaries["Land Use"].index.get_level_values(0)
    names = pd.Series(landuse).replace(landuse_lookup)
    summaries["Land Use"].index = pd.MultiIndex.from_arrays(
        [landuse, names], names=["Land Use Code", "Land Use Name"]
    )

    landuse = summaries["Sub-Land Use"].index.get_level_values(0)
    sub_landuse = summaries["Sub-Land Use"].index.get_level_values(1)
    codes = pd.Series([f"{i!s:0>2.2}{j.upper()}" for i, j in zip(landuse, sub_landuse)])
    summaries["Sub-Land Use"].index = pd.MultiIndex.from_arrays(
        [landuse, sub_landuse, codes.replace(sub_landuse_lookup)],
        names=["Land Use Code", "Sub-Land Use Code", "Sub-Land Use Name"],
    )

    with pd.ExcelWriter(excel_file) as excel:
        for name, data in summaries.items():
            data.to_excel(excel, sheet_name=name)
    LOG.info("Written: %s", excel_file)


def main() -> None:
    logging.basicConfig(
        level=logging.DEBUG, format="[{levelname:^8.8}] {message}", style="{"
    )

    trics_folder = pathlib.Path(
        r"C:\Users\ukmjb018\OneDrive - WSP O365\WSP_Projects\TfN NorMITs Demand Partner 2022\D-Lit Land Use\Inputs\TRICS Sites"
    )
    output_folder = trics_folder / "analysis"
    output_folder.mkdir(exist_ok=True)
    trics_sites, _ = combine_trics_site_lists(trics_folder)
    date_filter = dt.datetime(2015, 1, 1)

    landuse_lookup, sub_landuse_lookup = load_landuse_lookups(
        trics_folder / "TRICS_landuse_codes.csv",
        trics_folder / "TRICS_sub_landuse_codes.csv",
    )

    summarise_trics_sites(
        trics_sites,
        landuse_lookup,
        sub_landuse_lookup,
        output_folder / "TRICS_site_summaries-all.xlsx",
    )
    summarise_trics_sites(
        trics_sites.loc[trics_sites["Most Recent Survey"] >= date_filter],
        landuse_lookup,
        sub_landuse_lookup,
        output_folder / "TRICS_site_summaries-after_2014.xlsx",
    )


##### MAIN #####
if __name__ == "__main__":
    main()
