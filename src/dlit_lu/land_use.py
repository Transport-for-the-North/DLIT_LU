"""Performs the conversion from D-Log formatting to the input to the Trip-Ends Module.
Conversion process involves disagregating by:
    land use code,
    aggregating by MSOA
    
    Residential:
        disagregate by dwelling type and convert to population
    Employment:
        convert to jobs and SIC codes

"""
# standard imports
import logging
import pathlib
# third party imports
import pandas as pd
import geopandas as gpd
import numpy as np
# local imports
from dlit_lu import utilities, global_classes, parser, inputs, data_repair

# constants
LOG = logging.getLogger(__name__)


def run(input_data: global_classes.DLogData, config: inputs.DLitConfig):
    """runs process for converting DLOG to MSOA build out profiles

    disaggregaes mixed into employment and residential and land use codes
    applys dwelling types using land use split by MSOA
    rebases to MSOA build-out profiles

    Parameters
    ----------
    input_data : global_classes.DLogData
        data to perform conversion
    config : inputs.DLitConfig
        config file
    """
    LOG.info("Initialising Land Use Module")

    config.output_folder.mkdir(exist_ok=True)

    average_infill_values = inputs.InfillingAverages.load_yaml(
        config.output_folder/inputs.AVERAGE_INFILLING_VALUES_FILE)

    traveller_type_factor = analyse_traveller_type_distribution(
        config.land_use.msoa_traveller_type_path)

    msoa_jobs = pd.read_csv(config.land_use.msoa_jobs_path).rename(columns = {"2018":"jobs"})

    msoa_pop_column_names = ["zone_id", "dwelling_type",
                             "n_uprn", "pop_per_dwelling",
                             "zone", "pop_aj_factor", "population"]

    msoa_dwelling_ratio = calc_msoa_proportion(
        config.land_use.msoa_dwelling_pop_path, msoa_pop_column_names)

    msoa = parser.parse_msoa(config.land_use.msoa_shapefile_path)
    LOG.info("Disaggregating mixed into residential and employment")
    data = disagg_mixed(
        utilities.to_dict(input_data))

    build_out_columns = np.arange(2000, 2067, 1).tolist()
    build_out_columns = [str(year) for year in build_out_columns]

    LOG.info("Calulating build out profile for all years")

    data["residential"] = add_all_year_units(
        data["residential"], "res_distribution", "units_(dwellings)", build_out_columns, input_data.lookup.years)
    data["employment"] = add_all_year_units(
        data["employment"], "emp_distribution", "units_(floorspace)", build_out_columns, input_data.lookup.years)

    emp_redundant_columns = list(filter(lambda x: x.startswith(
        "emp_year_"), data["employment"].columns.to_list()))
    res_redundant_columns = list(filter(lambda x: x.startswith(
        "res_year_"), data["residential"].columns.to_list()))

    data["residential"].drop(columns=res_redundant_columns, inplace=True)
    data["employment"].drop(columns=emp_redundant_columns, inplace=True)

    LOG.info("Disaggregating employment proposed LUCs")
    construction_land_use_data = data.copy()
    construction_land_use_data["employment"
                               ] = disagg_land_use_codes(
        construction_land_use_data["employment"],
        "proposed_land_use",
        build_out_columns,
        input_data.proposed_land_use_split,
    )

    demolition_land_use_data = data.copy()
    demolition_land_use_data["residential"] = convert_to_gfa(
        demolition_land_use_data["residential"],
        "total_site_area_size_hectares",
        "units_(dwellings)",
        build_out_columns,
        average_infill_values.average_gfa_site_area_ratio)

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

    demolition_land_use_data["residential"].loc[:, build_out_columns] = - \
        demolition_land_use_data["residential"].loc[:, build_out_columns]
    demolition_land_use_data["employment"].loc[:, build_out_columns] = - \
        demolition_land_use_data["employment"].loc[:, build_out_columns]

    demolition_land_use_data["residential"].columns = demolition_land_use_data[
        "employment"].columns

    construction_land_use_data["employment"]["land_use"] = construction_land_use_data[
        "employment"]["proposed_land_use"]
    demolition_land_use_data["employment"]["land_use"] = demolition_land_use_data[
        "employment"]["existing_land_use"]
    demolition_land_use_data["residential"]["land_use"] = demolition_land_use_data[
        "residential"]["existing_land_use"]

    residential_build_out = construction_land_use_data["residential"]
    employment_build_out = pd.concat([
        construction_land_use_data["employment"],
        demolition_land_use_data["employment"],
        demolition_land_use_data["residential"]
    ], ignore_index=True)

    LOG.info("performing MSOA geospatial lookup")
    res_msoa_sites = msoa_site_geospatial_lookup(
        residential_build_out, msoa)
    emp_msoa_sites = msoa_site_geospatial_lookup(
        employment_build_out, msoa)

    LOG.info("Disaggregating dwellings into population by dwelling type")

    comparison_path = config.output_folder / \
        "existing_proposed_development_comparison"

    comparison_path.mkdir(exist_ok=True)

    compare_existing_proposed_dwellings(
        msoa_dwelling_ratio,
        res_msoa_sites,
        build_out_columns,
        comparison_path / "existing_proposed_dwelling_comparison.csv"
    )

    res_msoa_sites = disagg_dwelling(
        res_msoa_sites,
        msoa_dwelling_ratio,
        build_out_columns,
    )

    res_msoa_sites = res_msoa_sites.loc[:, build_out_columns +
                                        ["msoa11cd", "dwelling_type"]]
    emp_msoa_sites = emp_msoa_sites.loc[:, build_out_columns +
                                        ["msoa11cd", "land_use"]]

    LOG.info("Rebasing to MSOA and Land use")

    res_msoa_base = res_msoa_sites.groupby(["msoa11cd", "dwelling_type"]).sum()
    emp_msoa_base = emp_msoa_sites.groupby(["msoa11cd", "land_use"]).sum()

    LOG.info("Disaggregating by traveller type")
    res_msoa_base = apply_pop_land_use(
        res_msoa_base, build_out_columns, traveller_type_factor)

    # rename columns
    res_msoa_base.reset_index(drop=False, inplace=True)
    emp_msoa_base.reset_index(drop=False,  inplace=True)
    res_msoa_base.rename(columns={"msoa11cd": "msoa_zone_id"}, inplace=True)
    emp_msoa_base.rename(columns={"msoa11cd": "msoa_zone_id"}, inplace=True)
    res_msoa_base.set_index(
        ["msoa_zone_id", "dwelling_type", "tfn_traveller_type"], inplace=True)
    emp_msoa_base.set_index(["msoa_zone_id", "land_use"], inplace=True)

    LOG.info("Converting GFA to jobs")
    emp_msoa_base = convert_gfa_to_jobs(
        emp_msoa_base, config.land_use.employment_density_matrix_path, build_out_columns)
    emp_msoa_base = convert_luc_to_sic(
        emp_msoa_base, config.land_use.luc_sic_conversion_path)

    compare_existing_proposed_jobs(msoa_jobs,emp_msoa_base, build_out_columns, comparison_path / "existing_proposed_jobs_comparison.csv")

    LOG.info("Writing Land Use disaggregation and geospatial lookup results")

    res_file_name = "residential_msoa_build_out.csv"
    emp_file_name = "employment_msoa_build_out.csv"

    utilities.write_to_csv(
        config.output_folder / res_file_name, res_msoa_base)
    utilities.write_to_csv(
        config.output_folder / emp_file_name, emp_msoa_base)

    LOG.info("Ending Land Use Module")

def compare_existing_proposed_jobs(
    existing_data:pd.DataFrame,
    proposed_data:pd.DataFrame,
    build_out_profile_cols: list[str],
    file_path:pathlib.Path,
    )->None:
    """compares the number of existing jobsto the number of proposed jobs

    outputs a comparion the number of existing jobs from an external input,
    to the number of proposed jobs infered from the D-Log

    Parameters
    ----------
    existing_data : pd.DataFrame
        data containing the number of existing jobs (TfN land use data)
    proposed_data : pd.DataFrame
        data containing the number of proposed jobs (from D-Log)
    build_out_profile_cols : list[str]
        build-out profiles columns
    file_path : pathlib.Path
        path to save comparison output
    """    
    existing_jobs = (existing_data.groupby("msoa_zone_id")["jobs"].sum()).to_frame(name="total_existing_jobs")
    proposed_data["total_proposed_jobs"] = proposed_data[build_out_profile_cols].sum(
        axis=1)
    proposed_jobs = proposed_data.groupby(
        "msoa_zone_id")["total_proposed_jobs"].sum()
    comparison = existing_jobs.merge(
        proposed_jobs, how= "outer", left_index=True, right_index=True)
    comparison["ratio (percentage)"] = 100 * comparison["total_proposed_jobs"] / \
        comparison["total_existing_jobs"]
    utilities.write_to_csv(file_path, comparison)


def analyse_traveller_type_distribution(file_path: pathlib.Path) -> pd.DataFrame:
    """calculates the factors for each traveller type

    aggregates across all zones and dwelling types

    Parameters
    ----------
    file_path : pathlib.Path
        file path to TfN population land use

    Returns
    -------
    pd.DataFrame
        contains factors for msoa traveller type
    """
    data = pd.read_csv(file_path)
    agg_zones = data.groupby("tfn_traveller_type").sum()
    ratios = (agg_zones["people"]/agg_zones["people"].sum()
              ).reset_index(drop=False)
    msoa_ratios = []
    for id_ in data["msoa_zone_id"].unique():
        temp = ratios.copy()
        temp["msoa_zone_id"] = pd.Series([id_]).repeat(
            len(ratios)).reset_index(drop=True)
        msoa_ratios.append(temp)
    all_msoa_ratios = pd.concat(msoa_ratios, axis=0).set_index(
        ["msoa_zone_id", "tfn_traveller_type"])
    all_msoa_ratios.columns = ["ratios"]
    return all_msoa_ratios


def apply_pop_land_use(
    data: pd.DataFrame,
    unit_columns: list[str],
    tt_factors: pd.DataFrame,
) -> pd.DataFrame:
    """applies TfN population land use factors to data

    Parameters
    ----------
    data : pd.DataFrame
        data containing the unit values
    unit_columns : list[str]
        list of column names in data that contain the unit values to be updated
    tt_factors : pd.DataFrame
        dataframe containing the TfN population land use factors

    Returns
    -------
    pd.DataFrame
        dataframe with updated unit values, indexed by msoa11cd,
        dwelling_type, and tfn_traveller_type
    """

    data_ratios = data.reset_index(drop=False).merge(tt_factors.reset_index(
        drop=False), left_on="msoa11cd", right_on="msoa_zone_id").set_index([
            "msoa11cd", "dwelling_type", "tfn_traveller_type"])
    data_ratios = data_ratios.loc[:, unit_columns].multiply(
        data_ratios["ratios"], axis=0)
    return data_ratios


def compare_existing_proposed_dwellings(
    exisiting_data: pd.DataFrame,
    proposed_data: pd.DataFrame,
    build_out_profile_cols: list[str],
    file_path: pathlib.Path
    ) -> None:
    """produces a comparison of existing and proposed dwelling types

    outputs a csvfile at a defined location with the total existing and proposed
    dwellings by msoa. existing jobs are taken from a defined external data
    source (TfN landuse)

    Parameters
    ----------
    exisiting_data : pd.DataFrame
        TfN land use data contain the number of dwellings by msoa
    proposed_data : pd.DataFrame
        Dlog data
    build_out_profile_cols : list[str]
        build-out profile data in proposed data
    file_path : pathlib.Path
        path to save outputted csv
    """    
    existing_dwellings = (exisiting_data.groupby("zone_id")[
                          "n_uprn"].sum()).to_frame(name="total_existing_dwellings")
    proposed_data["total_proposed_dwellings"] = proposed_data[build_out_profile_cols].sum(
        axis=1)
    proposed_dwellings = proposed_data.groupby(
        "msoa11cd")["total_proposed_dwellings"].sum()
    comparison = existing_dwellings.merge(
        proposed_dwellings, how = "outer", left_index=True, right_index=True)
    comparison["ratio (percentage)"] = 100 * comparison["total_proposed_dwellings"] / \
        comparison["total_existing_dwellings"]
    utilities.write_to_csv(file_path, comparison)


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
    data_to_gfa.loc[:, unit_col] = data_to_gfa[area_col]*factor
    data_to_gfa.loc[:, unit_year_columns] = data.loc[:, unit_year_columns].divide(
        data.loc[:, unit_col], axis=0).multiply(data_to_gfa[unit_col], axis=0)
    return data_to_gfa


def convert_gfa_to_jobs(data: pd.DataFrame, matrix_path: pathlib.Path, unit_cols) -> pd.DataFrame:
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
    matrix = pd.read_csv(matrix_path).loc[:, [
        "land_use_code", "fte_floorspace"]]
    matrix.loc[:, "land_use_code"] = matrix["land_use_code"].str.lower()
    data_jobs = data.reset_index().merge(
        matrix, how="left", left_on="land_use", right_on="land_use_code")
    data_jobs.loc[:, unit_cols] = data_jobs.loc[:, unit_cols].divide(
        data_jobs.loc[:, "fte_floorspace"], axis=0)
    data_jobs.loc[data_jobs["fte_floorspace"].isnull(), unit_cols] = 0
    has_jobs = ~ pd.DataFrame(
        [data_jobs[col] == 0 for col in unit_cols]).transpose().all(axis=1)
    data_jobs.drop(columns=["fte_floorspace", "land_use_code"], inplace=True)
    data_jobs = data_jobs[has_jobs]
    data_jobs.set_index(["msoa_zone_id", "land_use"], inplace=True)
    return data_jobs


def convert_luc_to_sic(data: pd.DataFrame, conversion_path: pathlib.Path) -> pd.DataFrame:
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
    conversion = pd.read_csv(conversion_path).loc[:, [
        "land_use_code", "sic_code"]]
    conversion["land_use_code"] = conversion["land_use_code"].str.lower()
    data_sic_code = data.reset_index(drop=False).merge(
        conversion,
        how="left",
        left_on="land_use",
        right_on="land_use_code",
    )
    data_sic_code.drop(columns=["land_use_code", "land_use"], inplace=True)
    data_sic_code.set_index(["msoa_zone_id", "sic_code"], inplace=True)
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
        raise ValueError(
            "distrubtion contains not specified or defined years values")

    flat = data[data[distribution_column] == 2]
    flat_years = data_repair.strip_year(
        flat["start_year_id"], flat["end_year_id"], years_lookup)
    early = data[data[distribution_column] == 3]
    early_years = data_repair.strip_year(
        early["start_year_id"], early["end_year_id"], years_lookup)
    late = data[data[distribution_column] == 4]
    late_years = data_repair.strip_year(
        late["start_year_id"], late["end_year_id"], years_lookup)
    mid = data[data[distribution_column] == 5]
    mid_years = data_repair.strip_year(mid["start_year_id"],
                                       mid["end_year_id"], years_lookup)

    for column in unit_year_column:
        year = int(column)
        flat.loc[:, column] = data_repair.flat_distribution(
            flat[unit_column], flat_years["start_year"], flat_years["end_year"], year, period)
        early.loc[:, column] = data_repair.early_distribution(
            early[unit_column], early_years["start_year"], early_years["end_year"], year, period)
        late.loc[:, column] = data_repair.late_distribution(
            late[unit_column], late_years["start_year"], late_years["end_year"], year, period)
        mid.loc[:, column] = data_repair.mid_distribution(
            mid[unit_column], mid_years["start_year"], mid_years["end_year"], year, period)

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
    mix_emp.loc[:, "total_area_ha"] = mix_emp["site_area_ha"]
    res_new = pd.concat([res, mix_res],  ignore_index=True)
    emp_new = pd.concat([emp, mix_emp], ignore_index=True)

    return {"residential": res_new, "employment": emp_new}


def disagg_dwelling(
    data: pd.DataFrame,
    msoa_ratio: pd.DataFrame,
    unit_columns: list[str],
) -> pd.DataFrame:
    """_summary_

    _extended_summary_

    Parameters
    ----------
    data : pd.DataFrame
        DataFrame containing residential dwelling data
    msoa_ratio : pd.DataFrame
        contains the existing ratio of each type of dwelling and average occupancy for each MSOA
    unit_columns : list[str]
        columns names for dwelling column to be disaggregated

    Returns
    -------
    pd.DataFrame
        Path to the population data file
    """    

    msoa_ratio.reset_index("dwelling_type", inplace=True)

    data = data.merge(
        msoa_ratio, how="left", left_on="msoa11cd", right_on="zone_id")

    for column in unit_columns:
        data.loc[:, column] = data[column] * \
            data["dwelling_ratio"]*data["pop_per_dwelling"]

    return data


def msoa_site_geospatial_lookup(
        data: pd.DataFrame,
        msoa: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """spatially joins MSOA shapefile to DLOG sites


    Parameters
    ----------
    data : pd.DataFrame
        data to join to msoa
    msoa : gpd.GeoDataFrame
        msoa data

    Returns
    -------
    gpd.GeoDataFrame
        spatially joined data
    """

    dlog_geom = gpd.GeoDataFrame(data, geometry=gpd.points_from_xy(
        data["easting"], data["northing"]))
    dlog_msoa = gpd.sjoin(dlog_geom, msoa, how="left")
    return dlog_msoa


def calc_msoa_proportion(msoa_pop_path: pathlib.Path, columns: list[str]) -> pd.DataFrame:
    """calculates the msoa population by dwelling type


    Parameters
    ----------
    msoa_pop_path : pathlib.Path
        path to TfN population land use
    columns : list[str]
        column names in for the land use data

    Returns
    -------
    pd.DataFrame
        population and ratio of dwellings by dwelling type
    """
    msoa_pop = pd.read_csv(msoa_pop_path)
    msoa_pop.columns = columns
    msoa_pop.set_index(["zone_id", "dwelling_type"], inplace=True)
    msoa_pop["dwelling_ratio"] = msoa_pop["n_uprn"
                                          ]/msoa_pop["n_uprn"].groupby(level="zone_id").sum()

    return msoa_pop


def disagg_land_use_codes(
    data: pd.DataFrame,
    luc_column: str,
    unit_columns: list[str],
    land_use_split: pd.DataFrame
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
    ratio_demonitator = site_luc.groupby(
        ["site_reference_id"])[
            "total_floorspace"].sum().rename({"total_floorspace": "denom"})
    site_luc = site_luc.merge(
        ratio_demonitator, how="left", left_on="site_reference_id",
        right_index=True, suffixes=["", "_denom"])
    ratio = site_luc["total_floorspace"]/site_luc["total_floorspace_denom"]
    ratio.index = disagg.index
    disagg.loc[:, unit_columns] = disagg.loc[:,
                                             unit_columns].multiply(ratio, axis=0)
    return disagg
