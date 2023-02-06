# standard imports
import logging
import pathlib
# third party imports
import pandas as pd
# local imports
from dlit_lu import utilities, global_classes, parser, inputs

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

    msoa = parser.parse_msoa(config.land_use.msoa_shapefile_path)
    LOG.info("Disaggregating mixed into residential and employment")
    data = utilities.disagg_mixed(
        utilities.to_dict(input_data))

    emp_unit_year_columns = list(filter(lambda x: x.startswith(
        "emp_year_"), data["employment"].columns.to_list()))
    res_unit_year_columns = list(filter(lambda x: x.startswith(
        "res_year_"), data["residential"].columns.to_list()))

    LOG.info("Disaggregating employment proposed LUCs")
    construction_land_use_data = data.copy()
    construction_land_use_data["employment"
        ] = utilities.disagg_land_use_codes(
            construction_land_use_data["employment"],
            "proposed_land_use",
            emp_unit_year_columns,
            input_data.proposed_land_use_split,
            )

    demolition_land_use_data = data.copy()
    demolition_land_use_data["residential"] = convert_to_gfa(
        demolition_land_use_data["residential"],
        "total_site_area_size_hectares",
        "units_(dwellings)",
        res_unit_year_columns,
        average_infill_values.average_gfa_site_area_ratio)

    LOG.info("Disaggregating employment existing LUCs")

    demolition_land_use_data["employment"] = utilities.disagg_land_use_codes(
        demolition_land_use_data["employment"],
        "existing_land_use",
        emp_unit_year_columns,
        input_data.existing_land_use_split,
        )
    LOG.info("Disaggregating residential existing LUCs")
    demolition_land_use_data["residential"] = utilities.disagg_land_use_codes(
        demolition_land_use_data["residential"],
        "existing_land_use",
        res_unit_year_columns,
        input_data.existing_land_use_split,
        )

    demolition_land_use_data["residential"].loc[:, res_unit_year_columns] = - \
        demolition_land_use_data["residential"].loc[:, res_unit_year_columns]
    demolition_land_use_data["employment"].loc[:, emp_unit_year_columns] = - \
        demolition_land_use_data["employment"].loc[:, emp_unit_year_columns]

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
    res_msoa_sites = utilities.msoa_site_geospatial_lookup(
        residential_build_out, msoa)
    emp_msoa_sites = utilities.msoa_site_geospatial_lookup(
        employment_build_out, msoa)

    LOG.info("Disaggregating dwellings into population by dwelling type")
    msoa_pop_column_names = ["zone_id", "dwelling_type",
                                "n_uprn", "pop_per_dwelling",
                                "zone", "pop_aj_factor", "population"]

    res_msoa_sites = utilities.disagg_dwelling(
        res_msoa_sites,
        config.land_use.msoa_dwelling_pop_path,
        msoa_pop_column_names,
        res_unit_year_columns,
    )

    res_msoa_sites = res_msoa_sites.loc[:, res_unit_year_columns +
                                        ["msoa11cd", "dwelling_type"]]
    emp_msoa_sites = emp_msoa_sites.loc[:, emp_unit_year_columns +
                                        ["msoa11cd", "land_use"]]

    LOG.info("Rebasing to MSOA and Land use")

    res_msoa_base = res_msoa_sites.groupby(["msoa11cd", "dwelling_type"]).sum()
    emp_msoa_base = emp_msoa_sites.groupby(["msoa11cd", "land_use"]).sum()

    LOG.info("Disaggregating by traveller type")
    res_msoa_base = apply_pop_land_use(
        res_msoa_base, res_unit_year_columns, traveller_type_factor)

    #rename columns
    res_msoa_base.reset_index(drop=False, inplace =True)
    emp_msoa_base.reset_index(drop=False,  inplace =True)
    res_msoa_base.rename(columns = {"msoa11cd": "msoa_zone_id"}, inplace=True)
    emp_msoa_base.rename(columns = {"msoa11cd": "msoa_zone_id"}, inplace=True)
    res_msoa_base.set_index(["msoa_zone_id", "dwelling_type", "tfn_traveller_type"], inplace= True)
    emp_msoa_base.set_index(["msoa_zone_id", "land_use"], inplace= True)

    res_msoa_base = clean_column_names(res_msoa_base)
    emp_msoa_base = clean_column_names(emp_msoa_base)



    LOG.info("Converting GFA to jobs")
    emp_unit_year_columns = [col for col in emp_msoa_base.columns if all(
        c.isdigit() for c in emp_msoa_base.columns.astype(str))]
    emp_msoa_base = convert_gfa_to_jobs(
        emp_msoa_base, config.land_use.employment_density_matrix_path, emp_unit_year_columns)
    emp_msoa_base = convert_luc_to_sic(emp_msoa_base, config.land_use.luc_sic_conversion_path)

    LOG.info("Writing Land Use disaggregation and geospatial lookup results")

    res_file_name = "residential_msoa_build_out.csv"
    emp_file_name = "employment_msoa_build_out.csv"

    utilities.write_to_csv(
        config.output_folder / res_file_name, res_msoa_base)
    utilities.write_to_csv(
        config.output_folder / emp_file_name, emp_msoa_base)

    LOG.info("Ending Land Use Module")


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

def clean_column_names(data:pd.DataFrame)->pd.DataFrame:
    """Cleans up column names in a dataframe by extracting digits

    column names replaced with just the digits in the existing name
    column names without digits are not changed

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing the columns to be cleaned up

    Returns
    -------
    pd.DataFrame
        DataFrame with updated column names, containing only the extracted digits
    """

    cols = data.columns
    new_cols = {}
    for col in cols:
        # Check if the column name contains a digit
        if any(char.isdigit() for char in col):
            # If it does, extract the digits from the string and use them as the new column name
            new_col = "".join(filter(str.isdigit, col))
            new_cols[col] = new_col
        else:
            # If it doesn't, use the original column name
            new_cols[col] = col
    data.rename(columns=new_cols, inplace=True)
    return data


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
    matrix.loc[:,"land_use_code"] = matrix["land_use_code"].str.lower()
    data_jobs = data.reset_index().merge(
        matrix, how="left", left_on="land_use", right_on="land_use_code")
    data_jobs.loc[:, unit_cols] = data_jobs.loc[:, unit_cols].divide(
        data_jobs.loc[:, "fte_floorspace"], axis=0)
    data_jobs.loc[data_jobs["fte_floorspace"].isnull(), unit_cols] = 0
    has_jobs = ~ pd.DataFrame([data_jobs[col] == 0 for col in unit_cols]).transpose().all(axis = 1)
    data_jobs.drop(columns=["fte_floorspace", "land_use_code"], inplace=True)
    data_jobs = data_jobs[has_jobs]
    data_jobs.set_index(["msoa_zone_id", "land_use"], inplace = True)
    return data_jobs

def convert_luc_to_sic(data: pd.DataFrame, conversion_path:pathlib.Path)->pd.DataFrame:
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
    conversion = pd.read_csv(conversion_path).loc[:,["land_use_code", "sic_code"]]
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
     