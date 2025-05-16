"""Performs the conversion from D-Log formatting to the input to the Trip-Ends Module.
Conversion process involves disagregating by:
    land use code,
    aggregating by LSOA

    Residential:
        disagregate by dwelling type and convert to population
    Employment:
        convert to jobs and SIC codes

"""

# standard imports
import logging
import pathlib
import itertools

# third party imports
import pandas as pd
import geopandas as gpd
import numpy as np

# local imports
from dlit_lu import utilities, global_classes, parser, inputs, data_repair

# constants
LOG = logging.getLogger(__name__)


def run(input_data: global_classes.DLogData, config: inputs.DLitConfig):
    """runs process for converting DLOG to site and LSOA build out profiles

    disaggregaes mixed into employment and residential and land use codes
    applys dwelling types using land use split by LSOA
    rebases to LSOA build-out profiles

    Parameters
    ----------
    input_data : global_classes.DLogData
        data to perform conversion
    config : inputs.DLitConfig
        config file
    """
    if config.land_use is None:
        raise ValueError("cannot run land use without any land use parameters")

    LOG.info("Initialising Land Use Module")

    config.output_folder.mkdir(exist_ok=True)
    LUTI = config.land_use.luti
    lu_output_path = config.output_folder / "M2_land_use_outputs"
    lu_output_path.mkdir(exist_ok=True)

    LOG.info("Loading in key inputs for land use module")
    lsoa = parser.parse_zone(config.land_use.lsoa_shapefile_path)
    average_infill_values = inputs.InfillingAverages.load_yaml(
        config.output_folder / inputs.AVERAGE_INFILLING_VALUES_FILE
    )

    lsoa_hh_pop_column_names = [
        "lsoa2021_id",
        "accom_h",
        "household",
        "population",
        "pop_per_dwelling",
    ]

    by_lsoa_hh_pop = pd.read_csv(
        config.land_use.lsoa_dwelling_pop_path,
        names=lsoa_hh_pop_column_names,
        header=0,
        index_col=None,
    )

    LOG.info("Deriving key ratios from base year land use inputs")

    lsoa_dwelling_ratio = calc_lsoa_proportion(by_lsoa_hh_pop)

    LOG.info(
        "Defining key and common columns to be kept for both residential and commercial sites"
    )
    # key common columns
    common_key_columns = [
        "site_reference_id",
        "easting",
        "northing",
        "web_tag_certainty",
    ]
    res_key_columns = common_key_columns
    emp_key_columns = common_key_columns + ["land_use"]
    # range of years defined in D-log
    build_out_columns = np.arange(2000, 2067, 1).tolist()
    build_out_columns = [str(year) for year in build_out_columns]

    LOG.info("Disaggregating mixed into residential and employment")
    data = disagg_mixed(utilities.to_dict(input_data))

    LOG.info("Calulating build out profile for all years")

    data["residential"] = add_all_year_units(
        data["residential"],
        "res_distribution",
        "units_(dwellings)",
        build_out_columns,
        input_data.lookup.years,
    )
    data["employment"] = add_all_year_units(
        data["employment"],
        "emp_distribution",
        "units_(floorspace)",
        build_out_columns,
        input_data.lookup.years,
    )

    emp_redundant_columns = list(
        filter(
            lambda x: x.startswith("emp_year_"), data["employment"].columns.to_list()
        )
    )
    res_redundant_columns = list(
        filter(
            lambda x: x.startswith("res_year_"), data["residential"].columns.to_list()
        )
    )

    data["residential"].drop(columns=res_redundant_columns, inplace=True)
    data["employment"].drop(columns=emp_redundant_columns, inplace=True)

    LOG.info("Disaggregating employment proposed LUCs")
    construction_land_use_data = data.copy()
    construction_land_use_data["employment"] = disagg_expected_land_use_codes(
        construction_land_use_data["employment"],
        "proposed_land_use",
        build_out_columns,
        "expected_split",
        input_data.proposed_land_use_split,
        "ratio",
    )

    msg = "Demolitions calculated with dampener = %.2f"
    if config.land_use.demolition_dampener == 1:
        msg += ", i.e. no damping"
    elif config.land_use.demolition_dampener == 0:
        msg += ", i.e. no demolitions"
    LOG.info(msg, config.land_use.demolition_dampener)

    demolition_land_use_data: dict[str, pd.DataFrame] = {}
    for key, df in data.items():
        negative_sites = (df.loc[:, build_out_columns] < 0).any(axis=1).sum()
        if negative_sites > 0:
            LOG.info(
                "Explicit demolitions found on %s sites in %s data", negative_sites, key
            )

        demos = df.copy()
        demos.loc[:, build_out_columns] = demos.loc[:, build_out_columns].multiply(
            config.land_use.demolition_dampener
        )
        demolition_land_use_data[key] = demos

    demolition_land_use_data["residential"] = convert_to_gfa(
        demolition_land_use_data["residential"],
        "total_site_area_size_hectares",
        "units_(dwellings)",
        build_out_columns,
        average_infill_values.average_gfa_site_area_ratio,
    )

    LOG.info("Disaggregating employment existing LUCs")

    demolition_land_use_data["employment"] = disagg_land_use_codes(
        demolition_land_use_data["employment"],
        "existing_land_use",
        build_out_columns,
        input_data.existing_land_use_split,
    )
    LOG.info("Disaggregating residential existing LUCs")
    demolition_land_use_data["residential"] = disagg_land_use_codes(
        demolition_land_use_data["residential"],
        "existing_land_use",
        build_out_columns,
        input_data.existing_land_use_split,
    )

    demolition_land_use_data["residential"].loc[:, build_out_columns] = (
        -demolition_land_use_data["residential"].loc[:, build_out_columns]
    )
    demolition_land_use_data["employment"].loc[:, build_out_columns] = (
        -demolition_land_use_data["employment"].loc[:, build_out_columns]
    )

    demolition_land_use_data["residential"].columns = demolition_land_use_data[
        "employment"
    ].columns

    construction_land_use_data["employment"]["land_use"] = construction_land_use_data[
        "employment"
    ]["proposed_land_use"]
    demolition_land_use_data["employment"]["land_use"] = demolition_land_use_data[
        "employment"
    ]["existing_land_use"]
    demolition_land_use_data["residential"]["land_use"] = demolition_land_use_data[
        "residential"
    ]["existing_land_use"]

    residential_build_out = construction_land_use_data["residential"]
    employment_build_out = pd.concat(
        [
            construction_land_use_data["employment"],
            demolition_land_use_data["employment"],
            demolition_land_use_data["residential"],
        ],
        ignore_index=True,
    )

    res_sites = residential_build_out.loc[
        :,
        res_key_columns + build_out_columns,
    ]

    res_sites_expand = expand_site_certainty(
        res_sites, config.land_use.web_tag_certainty_path
    )

    emp_sites = employment_build_out.loc[:, emp_key_columns + build_out_columns]

    LOG.info("performing LSOA geospatial lookup")
    res_lsoa_sites = zone_site_geospatial_lookup(residential_build_out, lsoa)
    res_lsoa_sites = res_lsoa_sites.loc[
        :,
        build_out_columns + res_key_columns + ["LSOA21CD"],
    ]
    res_lsoa_sites = res_lsoa_sites.rename(columns={"LSOA21CD": "lsoa2021_id"})
    emp_lsoa_sites = zone_site_geospatial_lookup(employment_build_out, lsoa)
    emp_lsoa_sites = emp_lsoa_sites.loc[
        :,
        build_out_columns + emp_key_columns + ["LSOA21CD"],
    ]

    emp_lsoa_sites = emp_lsoa_sites.rename(columns={"LSOA21CD": "lsoa2021_id"})

    LOG.info("Export site level dwelling and floorspace data")

    res_sites_uncertainty = res_sites.groupby(res_key_columns).sum()

    emp_sites_uncerntainty = emp_sites.groupby(emp_key_columns).sum()

    res_sites_uncertainty_file = "dwelling_sites_uncertainty_build_out.csv"
    emp_sites_uncertainty_file = "floorspace_sites_uncerntainty_build_out.csv"

    utilities.write_to_csv(
        lu_output_path / res_sites_uncertainty_file, res_sites_uncertainty
    )  # input needed by next module
    utilities.write_to_csv(
        lu_output_path / emp_sites_uncertainty_file, emp_sites_uncerntainty
    )

    # Files needed by LUTI
    if LUTI:
        LOG.info("Creating LUTI zonal data")
        luti_output_path = config.output_folder / "LUTI_outputs"
        luti_output_path.mkdir(exist_ok=True)
        # Need LUTI zone system and shapefile here to convert site data into luti zonal data
        luti_zones = parser.parse_zone(config.land_use.luti_zone_shapefile_path)
        res_luti_sites = zone_site_geospatial_lookup(res_sites_expand, luti_zones)
        res_luti_sites = res_luti_sites.loc[
            :,
            build_out_columns + res_key_columns + ["zone_id"],
        ]
        res_luti = res_luti_sites.groupby(["zone_id", "web_tag_certainty"])[
            build_out_columns
        ].sum()
        emp_luti_sites = zone_site_geospatial_lookup(emp_sites, luti_zones)
        emp_luti_sites = emp_luti_sites.loc[
            :,
            build_out_columns + emp_key_columns + ["zone_id"],
        ]
        emp_luti = emp_luti_sites.groupby(["zone_id", "web_tag_certainty", "land_use"])[
            build_out_columns
        ].sum()

        res_luti_file = "dwelling_luti_build_out.csv"
        emp_luti_file = "floorspace_luti_build_out.csv"

        utilities.write_to_csv(luti_output_path / res_luti_file, res_luti)
        utilities.write_to_csv(luti_output_path / emp_luti_file, emp_luti)

    LOG.info("Convert site development to jobs")

    emp_lsoa_sites_jobs = convert_gfa_to_jobs_site(
        emp_lsoa_sites,
        config.land_use.employment_density_matrix_path,
        build_out_columns,
    )
    emp_lsoa_sites_jobs = convert_luc_to_sic_site(
        emp_lsoa_sites_jobs, config.land_use.luc_sic_conversion_path
    )

    LOG.info("Export site level job data")

    emp_sites_jobs_sic = (
        emp_lsoa_sites_jobs.reset_index(drop=False)
        .groupby(common_key_columns + ["sic_2d"])[build_out_columns]
        .sum()
    )
    emp_sites_jobs_tot = (
        emp_lsoa_sites_jobs.reset_index(drop=False)
        .groupby(common_key_columns)[build_out_columns]
        .sum()
    )

    emp_sites_jobs_sic_file = "jobs_sic_sites_uncertainty_build_out.csv"
    emp_sites_tot_file = "jobs_sites_uncertainty_build_out.csv"

    utilities.write_to_csv(
        lu_output_path / emp_sites_tot_file, emp_sites_jobs_tot
    )  # inputs needed by module dev_pattern
    utilities.write_to_csv(
        lu_output_path / emp_sites_jobs_sic_file, emp_sites_jobs_sic
    )  # inputs needed by module dev_pattern

    LOG.info("Disaggregating dwellings into population by dwelling type")
    res_lsoa_sites_pop = disagg_dwelling(
        res_lsoa_sites,
        lsoa_dwelling_ratio,
        build_out_columns,
    )
    res_lsoa_sites_pop = res_lsoa_sites_pop.loc[
        :,
        build_out_columns + res_key_columns + ["lsoa2021_id", "accom_h"],
    ]

    LOG.info("Disaggregating site level population further by traveller type")
    res_lsoa_sites_pop = res_lsoa_sites_pop.groupby(res_key_columns + ["lsoa2021_id"])[
        build_out_columns
    ].sum()

    res_sites_pop = (
        res_lsoa_sites_pop.reset_index()
        .groupby(res_key_columns)[build_out_columns]
        .sum()
    )

    res_sites_pop_tot_file = "population_sites_uncertainty_build_out.csv"

    utilities.write_to_csv(lu_output_path / res_sites_pop_tot_file, res_sites_pop)

    LOG.info("Checking total yearly dwelling and population")

    year_tot_dwel = res_sites[build_out_columns].sum(axis=0)

    year_tot_pop = res_lsoa_sites_pop[build_out_columns].sum(axis=0)

    year_tot_job = emp_sites_jobs_tot[build_out_columns].sum(axis=0)
    year_tot_df = pd.DataFrame(
        {
            "year_tot_dwel": year_tot_dwel,
            "year_tot_pop": year_tot_pop,
            "year_tot_job": year_tot_job,
        }
    )
    year_tot_df_file_name = "year_totals.csv"
    utilities.write_to_csv(lu_output_path / year_tot_df_file_name, year_tot_df)

    LOG.info("Ending Land Use Module")


def convert_to_gfa(
    data: pd.DataFrame,
    area_col: str,
    unit_col: str,
    unit_year_columns: list[str],
    factor: float,
) -> pd.DataFrame:
    """Converts dwellings to GFA

    uses site area and factor to calculate total GFA and
    distributes the build out profile in the orginal ratio

    Parameters
    ----------
    data : pd.DataFrame
        DataFrame containing the data to be converted
    area_col : str
        Column name for the area data
    unit_col : str
        Column name for the unit data
    unit_year_columns : list[str]
        List of column names for the year unit data
    factor : float
        Conversion factor to use in converting the data

    Returns
    -------
    pd.DataFrame
        DataFrame with the converted data
    """
    data_to_gfa = data.copy()
    data_to_gfa.loc[:, unit_col] = data_to_gfa[area_col] * factor
    data_to_gfa.loc[:, unit_year_columns] = (
        data.loc[:, unit_year_columns]
        .divide(data.loc[:, unit_col], axis=0)
        .multiply(data_to_gfa[unit_col], axis=0)
    )
    return data_to_gfa


def expand_site_certainty(
    data: pd.DataFrame, certainty_path: pathlib.Path
) -> pd.DataFrame:
    """Expand to have full combination between site (with easting/northing) and certainty types.

    Parameters
    ----------
    data : pd.DataFrame
        The DataFrame containing site information.
    certainty_path : pathlib.Path
        The path to a CSV file that provides all web tag certainty types.

    Returns
    -------
    pd.DataFrame
        The expanded DataFrame with all unique site-reference ID, easting, and northing combinations.
    """

    # Load certainty types
    certainty = pd.read_csv(certainty_path).loc[:, ["web_tag_certainty"]]

    # Get unique site_reference_id, easting, and northing combinations
    unique_sites = data.loc[
        :, ["site_reference_id", "easting", "northing"]
    ].drop_duplicates()

    # Create all combinations of (site_reference_id, easting, northing) with certainty types
    expanded_df = pd.DataFrame(
        itertools.product(
            unique_sites.itertuples(index=False, name=None),
            certainty["web_tag_certainty"],
        ),
        columns=["site_info", "web_tag_certainty"],
    )

    # Split site_info tuple back into separate columns
    expanded_df[["site_reference_id", "easting", "northing"]] = pd.DataFrame(
        expanded_df["site_info"].tolist()
    )

    # Drop the temporary tuple column
    expanded_df.drop(columns=["site_info"], inplace=True)

    # Merge with the original data to preserve existing values while keeping new combinations
    expanded_data = expanded_df.merge(
        data,
        on=["site_reference_id", "easting", "northing", "web_tag_certainty"],
        how="left",
    )

    # Fill NaN values with 0
    expanded_data.fillna(0, inplace=True)

    return expanded_data


def convert_gfa_to_jobs_site(
    data: pd.DataFrame, matrix_path: pathlib.Path, unit_cols
) -> pd.DataFrame:
    """Converts GFA build-out profile to jobs

    Parameters
    ----------
    data : pd.DataFrame
        DataFrame with GFA build out profiles
    matrix_path : pathlib.Path
        Path to the job density matrix
    unit_cols : list[str]
        Columns in the data that contain build-out profile data

    Returns
    -------
    pd.DataFrame
        DataFrame containing job build-out profiles
    """
    matrix = pd.read_csv(matrix_path).loc[:, ["land_use_code", "fte_floorspace"]]
    matrix.loc[:, "land_use_code"] = matrix["land_use_code"].str.lower()
    data_jobs = data.reset_index().merge(
        matrix, how="left", left_on="land_use", right_on="land_use_code"
    )
    data_jobs.loc[:, unit_cols] = data_jobs.loc[:, unit_cols].divide(
        data_jobs.loc[:, "fte_floorspace"], axis=0
    )
    data_jobs.loc[data_jobs["fte_floorspace"].isnull(), unit_cols] = 0
    has_jobs = (
        ~pd.DataFrame([data_jobs[col] == 0 for col in unit_cols])
        .transpose()
        .all(axis=1)
    )
    data_jobs.drop(columns=["fte_floorspace", "land_use_code"], inplace=True)
    data_jobs = data_jobs[has_jobs]

    return data_jobs


def convert_luc_to_sic_site(
    data: pd.DataFrame, conversion_path: pathlib.Path
) -> pd.DataFrame:
    """Convert the land use codes (LUC) to standard industrial classification (SIC) codes.

    Parameters
    ----------
    data : pd.DataFrame
        The DataFrame containing the land use codes.
    conversion_path : pathlib.Path
        The path to a csv file that maps the LUC codes to the SIC codes.

    Returns
    -------
    pd.DataFrame
        The DataFrame with the SIC codes

    """
    conversion = pd.read_csv(conversion_path).loc[:, ["land_use_code", "sic_code"]]
    conversion["land_use_code"] = conversion["land_use_code"].str.lower()
    data_sic_code = data.merge(
        conversion,
        how="left",
        left_on="land_use",
        right_on="land_use_code",
    )
    data_sic_code.drop(columns=["land_use_code", "land_use"], inplace=True)
    data_sic_code.rename(columns={"sic_code": "sic_2d"}, inplace=True)

    return data_sic_code


def add_all_year_units(
    data: pd.DataFrame,
    distribution_column: str,
    unit_column: str,
    unit_year_column: list[str],
    years_lookup: pd.DataFrame,
) -> pd.DataFrame:
    """create a build out profile for any consecutive years

    calculates a build out profile for consecutive years defined in unit year columns.
    adds them as new columns to the inputted data. assunes infill years are 1 year apart.

    Parameters
    ----------
    data : pd.DataFrame
        data to produce build out profile
    distribution_column : str
        column that contains the distribution ID
    unit_column : str
        column that contains unit to disagregate build out profile
    unit_year_column : list[int]
        columns to produce build out year. must be in 4 digit year format and
        passed as strings
    years_lookup : pd.DataFrame
        years lookup table from DLog

    Returns
    -------
    pd.DataFrame:
        data with new build out profile calculated

    Raises
    ------
    ValueError
        if any values have distribution IDs of 0 (not specified) or
        1 (specified - unable to calculate build out from this)
    """

    period = 1

    not_specified = data[data[distribution_column] == 0]
    years_defined = data[data[distribution_column] == 1]

    if len(not_specified) != 0 or len(years_defined) != 0:
        raise ValueError("distrubtion contains not specified or defined years values")

    flat = data[data[distribution_column] == 2]
    flat_years = data_repair.strip_year(
        flat["start_year_id"], flat["end_year_id"], years_lookup
    )
    early = data[data[distribution_column] == 3]
    early_years = data_repair.strip_year(
        early["start_year_id"], early["end_year_id"], years_lookup
    )
    late = data[data[distribution_column] == 4]
    late_years = data_repair.strip_year(
        late["start_year_id"], late["end_year_id"], years_lookup
    )
    mid = data[data[distribution_column] == 5]
    mid_years = data_repair.strip_year(
        mid["start_year_id"], mid["end_year_id"], years_lookup
    )

    for column in unit_year_column:
        year = int(column)
        flat.loc[:, column] = data_repair.flat_distribution(
            flat[unit_column],
            flat_years["start_year"],
            flat_years["end_year"],
            year,
            period,
        )
        early.loc[:, column] = data_repair.early_distribution(
            early[unit_column],
            early_years["start_year"],
            early_years["end_year"],
            year,
            period,
        )
        late.loc[:, column] = data_repair.late_distribution(
            late[unit_column],
            late_years["start_year"],
            late_years["end_year"],
            year,
            period,
        )
        mid.loc[:, column] = data_repair.mid_distribution(
            mid[unit_column],
            mid_years["start_year"],
            mid_years["end_year"],
            year,
            period,
        )

    updated_data = pd.concat([flat, early, late, mid])

    return updated_data


def disagg_mixed(data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """disaggregates the mixed data set into residential and employment

    assumes the columns in mixed relevent to each sheet will have identical
    column names to the those in the sheet

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        data set to disagg mixed

    Returns
    -------
    dict[str, pd.DataFrame]
        data set with just residential and employment
    """

    mix = data["mixed"]
    res = data["residential"].reset_index(drop=True)
    emp = data["employment"].reset_index(drop=True)

    mix_res = mix.loc[:, res.columns.unique()].reset_index(drop=True)
    mix_emp = mix.loc[:, emp.columns.unique()].reset_index(drop=True)

    mix_res.loc[:, "total_site_area_size_hectares"] = mix_res["total_area_ha"]
    # this does not feel right, it should be the other way around
    # mix_emp.loc[:, "total_area_ha"] = mix_emp["site_area_ha"]
    mix_emp.loc[:, "site_area_ha"] = mix_emp["total_area_ha"]

    res_new = pd.concat([res, mix_res], ignore_index=True)
    emp_new = pd.concat([emp, mix_emp], ignore_index=True)

    return {"residential": res_new, "employment": emp_new}


def disagg_dwelling(
    data: pd.DataFrame,
    lsoa_ratio: pd.DataFrame,
    unit_columns: list[str],
) -> pd.DataFrame:
    """_summary_

    _extended_summary_

    Parameters
    ----------
    data : pd.DataFrame
        DataFrame containing residential dwelling data
    lsoa_ratio : pd.DataFrame
        contains the existing ratio of each type of dwelling and average occupancy for each LSOA
    unit_columns : list[str]
        columns names for dwelling column to be disaggregated

    Returns
    -------
    pd.DataFrame
        Path to the population data file
    """

    lsoa_ratio.reset_index(inplace=True)

    data = data.merge(lsoa_ratio, how="left", on="lsoa2021_id")

    for column in unit_columns:
        data.loc[:, column] = (
            data[column] * data["dwelling_ratio"] * data["pop_per_dwelling"]
        )

    return data


def zone_site_geospatial_lookup(
    data: pd.DataFrame,
    zone: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """spatially joins zone shapefile to DLOG sites
    Parameters
    ----------
    data : pd.DataFrame
        data to join to zone
    zone : gpd.GeoDataFrame
        zone data

    Returns
    -------
    gpd.GeoDataFrame
        spatially joined data
    """
    drop_cols = {"easting", "northing"} & set(zone.columns)
    if drop_cols:
        zone = zone.drop(columns=list(drop_cols))

    dlog_geom = gpd.GeoDataFrame(
        data, geometry=gpd.points_from_xy(data["easting"], data["northing"])
    )
    dlog_zone = gpd.sjoin(dlog_geom, zone, how="left")
    return dlog_zone


def calc_lsoa_proportion(by_hh_pop: pd.DataFrame) -> pd.DataFrame:
    """calculates the lsoa population by dwelling type
    Parameters
    ----------
    lsoa_hh: pd.DataFrame
        TfN population land use

    Returns
    -------
    pd.DataFrame
        population and ratio of dwellings by dwelling type
    """
    lsoa_hh = by_hh_pop.copy()
    lsoa_hh.set_index(["lsoa2021_id", "accom_h"], inplace=True)
    lsoa_hh["dwelling_ratio"] = (
        lsoa_hh["household"] / lsoa_hh["household"].groupby(level="lsoa2021_id").sum()
    )

    return lsoa_hh


def disagg_land_use_codes(
    data: pd.DataFrame,
    luc_column: str,
    unit_columns: list[str],
    land_use_split: pd.DataFrame,
) -> pd.DataFrame:
    """disaggregates land use into seperate rows

    calculates the split of the GFA using total GFA for each land use as a input

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        data to disaggregate
    luc_column : str
        columns to disaggregate
    unit_columns : dict[str, str]
        unit column to disagregate
    land_use_split : pd.DataFrame
        contains each land use and the total GFA the take up in the Dlog

    Returns
    -------
    pd.DataFrame
        disaggregated land use
    """

    disagg = data.explode(luc_column).reset_index(drop=True)

    site_luc = disagg.loc[:, ["site_reference_id", luc_column]]
    site_luc = site_luc.merge(
        land_use_split,
        how="left",
        left_on=luc_column,
        right_on="land_use_codes",
    )
    ratio_demonitator = (
        site_luc.groupby(["site_reference_id"])["total_floorspace"]
        .sum()
        .rename({"total_floorspace": "denom"})
    )
    site_luc = site_luc.merge(
        ratio_demonitator,
        how="left",
        left_on="site_reference_id",
        right_index=True,
        suffixes=["", "_denom"],
    )
    ratio = site_luc["total_floorspace"] / site_luc["total_floorspace_denom"]
    ratio.index = disagg.index
    disagg.loc[:, unit_columns] = disagg.loc[:, unit_columns].multiply(ratio, axis=0)
    return disagg


def disagg_expected_land_use_codes(
    data: pd.DataFrame,
    luc_column: str,
    unit_columns: list[str],
    lcl_luc_split_column: str,
    land_use_split: pd.DataFrame,
    ratio_column: str,
) -> pd.DataFrame:
    """disaggregates land use into seperate rows

    calculates the split of the GFA using total GFA for each land use as a input

    Parameters
    ----------
    data : dict[str, pd.DataFrame]
        data to disaggregate
    luc_column : str
        columns to disaggregate
    unit_columns : dict[str, str]
        unit column to disagregate
    lcl_luc_split_column : str
        The column in the `data` DataFrame containing the proportions (expected splits) of the land use
        codes, which are used to determine how the GFA should be split between land uses.
    land_use_split : pd.DataFrame
        A DataFrame that contains the total GFA for each land use code. This will be used to calculate
        the total floor area for each land use category across all sites.
    ratio_column : str
        The column name in `site_luc` DataFrame where the calculated ratio for each land use will be stored.


    Returns
    -------
    pd.DataFrame
        disaggregated expected land use
    """

    disagg = data.explode(luc_column).reset_index(drop=True)
    # Extract proportion values if category exists in expected_split
    disagg[ratio_column] = disagg.apply(
        lambda row: (
            row[lcl_luc_split_column].get(row[luc_column], "")
            if isinstance(row[lcl_luc_split_column], dict)
            and "unknown" not in row[lcl_luc_split_column]
            else ""
        ),
        axis=1,
    )
    site_luc = disagg.loc[:, ["site_reference_id", luc_column, ratio_column]]
    site_luc = site_luc.merge(
        land_use_split,
        how="left",
        left_on=luc_column,
        right_on="land_use_codes",
    )
    ratio_demonitator = (
        site_luc.groupby(["site_reference_id"])["total_floorspace"]
        .sum()
        .rename({"total_floorspace": "denom"})
    )
    site_luc = site_luc.merge(
        ratio_demonitator,
        how="left",
        left_on="site_reference_id",
        right_index=True,
        suffixes=["", "_denom"],
    )
    site_luc[ratio_column] = site_luc.apply(
        lambda row: (
            row["total_floorspace"] / row["total_floorspace_denom"]
            if pd.isna(row[ratio_column]) or row[ratio_column] == ""
            else row[ratio_column]
        ),
        axis=1,
    )
    ratio = site_luc[ratio_column]
    ratio.index = disagg.index
    disagg.loc[:, unit_columns] = disagg.loc[:, unit_columns].multiply(ratio, axis=0)
    return disagg
