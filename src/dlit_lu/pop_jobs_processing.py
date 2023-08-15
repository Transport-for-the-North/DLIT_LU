# standard imports
import argparse
import logging
import pathlib

# third party
import pandas as pd
from tqdm.contrib import logging as tqdm_log
import logging
import numpy as np

# local imports
from dlit_lu import utilities, inputs, land_use, constraint

LOG_FILE = "DLIT.log"

LOG = logging.getLogger(__package__)
UNASSIGNED_JOBS_COLUMN = "unassigned_jobs"
LAD_COLUMN = "LAD"
SIC_MIN = 1
SIC_MAX = 99
SIC_COlUMN = "sic_code"
LOOKUP_LAD_COLUMN = "LAD20NM"
LOOKUP_CODE_COLUMN = "LAD20CD"


def run(args: argparse.Namespace) -> None:
    """initilises Logging and calls main"""
    with utilities.DLitLog() as dlit_log:
        with tqdm_log.logging_redirect_tqdm([dlit_log.logger]):
            main(dlit_log, args)


def main(log: utilities.DLitLog, args: argparse.Namespace) -> None:
    config = inputs.JobPopConfig.load_yaml(args.config)

    config.output_folder.mkdir(exist_ok=True)

    # set log file
    log.add_file_handler(config.output_folder / LOG_FILE)

    parsed_inputs = config.parse()

    forecast = combine_forecasts(
        parsed_inputs.forecast_year_growth,
        parsed_inputs.additional_growth,
        parsed_inputs.lad_name_lookup,
    )

    data = format_data(parsed_inputs)

    # TODO why is Population so high?
    constrained_pop = constraint.constrain_to_forecast(
        data["population"],
        forecast,
        parsed_inputs.lad_name_lookup,
        "2050",
        LOOKUP_CODE_COLUMN,
        "population",
    )

    constrained_jobs = constraint.constrain_to_forecast(
        data["jobs"],
        forecast,
        parsed_inputs.lad_name_lookup,
        "2050",
        LOOKUP_CODE_COLUMN,
        "jobs",
    )
    constrained_pop.reset_index(inplace=True)
    constrained_pop = lad_to_msoa(
        constrained_pop,
        parsed_inputs.msoa_to_lad_conversion,
        "LAD20CD",
        "lad_2020_zone_id",
        "msoa_zone_id",
        "2050",
        "lad_2020_to_msoa",
    ).set_index(["msoa_zone_id", "dwelling_type", "tfn_traveller_type"])
    utilities.write_to_csv(
        parsed_inputs.output_folder / "constrained_population.csv", constrained_pop
    )
    constrained_jobs.reset_index(inplace=True)
    constrained_jobs = lad_to_msoa(
        constrained_jobs,
        parsed_inputs.msoa_to_lad_conversion,
        "LAD20CD",
        "lad_2020_zone_id",
        "msoa_zone_id",
        "2050",
        "lad_2020_to_msoa",
    ).set_index(["msoa_zone_id", "sic_code"])
    utilities.write_to_csv(
        parsed_inputs.output_folder / "constrained_jobs.csv", constrained_jobs
    )

def format_data(inputs: inputs.JobPopInputs) -> dict[str, pd.DataFrame]:
    LOG.info("processing jobs data")
    dist_baseline_jobs = distribute_baseline_jobs(
        inputs.baseline_growth,
        inputs.proposed_luc_split,
        inputs.luc_sic_conversion,
        inputs.lad_name_lookup,
    )
    dist_baseline_pop = distribute_baseline_population(
        inputs.baseline_growth,
        inputs.msoa_to_lad_conversion,
        inputs.msoa_traveller_type,
        inputs.msoa_dwelling_pop,
        inputs.lad_name_lookup,
    )
    jobs = process_job_data(
        inputs.jobs_input,
        inputs.luc_sic_conversion,
        inputs.proposed_luc_split,
        inputs.lad_name_lookup,
    )
    LOG.info("outputting formatted jobs")
    utilities.write_to_csv(
        inputs.output_folder / "formatted_distributed_jobs.csv", jobs
    )

    LOG.info("processing jobs")
    population = process_pop_data(
        inputs.population_input,
        inputs.msoa_traveller_type,
        inputs.msoa_to_lad_conversion,
        inputs.msoa_dwelling_pop,
        inputs.lad_name_lookup,
    )

    jobs = add_baseline_growth(
        jobs,
        dist_baseline_jobs,
        inputs.lad_name_lookup,
        "2050",
        "Jobs",
        "Name",
        LOOKUP_LAD_COLUMN,
        LOOKUP_CODE_COLUMN,
    )

    population = add_baseline_growth(
        population,
        dist_baseline_pop,
        inputs.lad_name_lookup,
        "2050",
        "Total",
        "Name",
        LOOKUP_LAD_COLUMN,
        LOOKUP_CODE_COLUMN,
    )
    LOG.info("outputting formatted population")
    utilities.write_to_csv(
        inputs.output_folder / "formatted_distributed_population.csv", population
    )
    return {"population": population, "jobs": jobs}


def process_job_data(
    jobs: pd.DataFrame,
    luc_to_sic: pd.DataFrame,
    luc_distribution: pd.DataFrame,
    lad_name_lookup: pd.DataFrame,
) -> pd.DataFrame:
    """formats and distributes job data

    any jobs unassigned to a SIC code are distrubuted according to the LUC distribution and added to any
    assigned jobs. outputted in pivot table format

    Parameters
    ----------
    jobs : pd.DataFrame
        jobs to foramt and assign (should have sic columns (and an unassigned jobs column) and LAD indexs)
    luc_to_sic : pd.DataFrame
        land use code to sic code lookup
    luc_distribution : pd.DataFrame
        land use code distribution - output from d-lit infilling

    Returns
    -------
    pd.DataFrame
        formatted and distributed jobs
    """
    columns = jobs.columns

    jobs.reset_index(inplace=True)
    jobs.drop(jobs[jobs[LAD_COLUMN] == "sum"].index, inplace=True)

    jobs = name_to_code(
        jobs,
        lad_name_lookup,
        LAD_COLUMN,
        LOOKUP_LAD_COLUMN,
        LOOKUP_CODE_COLUMN,
    )

    # initilise variables to test whether data exists
    # variable for jobs for unassigned jobs column
    distributed_sic_jobs = None
    # variable for jobs which have been assigned to a sic code column
    assigned_sic_jobs = None

    # if unassigned jobs distrubute them by SIC code
    if UNASSIGNED_JOBS_COLUMN in columns:
        # remove greenfield and brownfield from distribution (assume no developments result in greenfield and brownfield sites)
        luc_distribution.drop(
            luc_distribution[luc_distribution["land_use_codes"] == "greenfield"].index,
            inplace=True,
        )
        luc_distribution.drop(
            luc_distribution[luc_distribution["land_use_codes"] == "brownfield"].index,
            inplace=True,
        )

        luc_to_sic.drop(
            luc_to_sic[luc_to_sic["land_use_code"] == "greenfield"].index,
            inplace=True,
        )

        luc_to_sic.drop(
            luc_to_sic[luc_to_sic["land_use_code"] == "brownfield"].index,
            inplace=True,
        )

        unassigned_jobs = jobs.loc[:, [LOOKUP_CODE_COLUMN, UNASSIGNED_JOBS_COLUMN]]
        # add land use codes to distribute to
        unassigned_jobs["land_use_codes"] = None
        unassigned_jobs["land_use_codes"] = unassigned_jobs["land_use_codes"].apply(
            lambda x: luc_distribution["land_use_codes"].tolist()
        )
        # distribute jobs by land use code
        distributed_jobs = land_use.disagg_land_use_codes(
            unassigned_jobs,
            "land_use_codes",
            UNASSIGNED_JOBS_COLUMN,
            luc_distribution,
            LOOKUP_CODE_COLUMN,
        )

        # remove irrelevnt rows

        distributed_jobs.drop(
            distributed_jobs[distributed_jobs[UNASSIGNED_JOBS_COLUMN] == 0].index,
            inplace=True,
        )
        distributed_jobs.rename(
            columns={"land_use_codes": "land_use", "unassigned_jobs": "2050"},
            inplace=True,
        )
        # convert to SIC code
        luc_to_sic["land_use_code"] = luc_to_sic["land_use_code"].str.lower()
        distributed_sic_jobs = distributed_jobs.reset_index(drop=False).merge(
            luc_to_sic,
            how="left",
            left_on="land_use",
            right_on="land_use_code",
        )
        distributed_sic_jobs.drop(
            columns=["land_use_code", "land_use", "index"], inplace=True
        )
        distributed_sic_jobs = (
            distributed_sic_jobs.groupby([LOOKUP_CODE_COLUMN, SIC_COlUMN])
            .sum()
            .reset_index()
        )

    # restructure assigned jobs
    assigned_columns = columns.to_list()
    assigned_columns.remove(UNASSIGNED_JOBS_COLUMN)

    if len(assigned_columns) > 0:
        # test for invalid SIC codes
        try:
            columns = [int(x) for x in assigned_columns]
        except ValueError as e:
            LOG.warning(
                "Columns assigning jobs to SIC codes must have column names of ONLY the two digit SIC code"
            )
            raise e
        for name in columns:
            if name < SIC_MIN or name > SIC_MAX:
                raise IndexError(
                    "Columns assigning jobs to SIC codes must have column names of ONLY the two digit SIC code"
                )

        # restructure df
        assigned_columns.insert(0, LOOKUP_CODE_COLUMN)
        assigned_jobs = jobs.loc[:, assigned_columns]

        assigned_sic_jobs = assigned_jobs.melt(
            id_vars=LOOKUP_CODE_COLUMN, value_vars=assigned_columns
        )

        assigned_sic_jobs.rename(
            columns={"variable": SIC_COlUMN, "value": "2050"}, inplace=True
        )

        assigned_sic_jobs[SIC_COlUMN] = assigned_sic_jobs[SIC_COlUMN].astype(int)

    if assigned_sic_jobs is not None and distributed_sic_jobs is not None:
        all_sic_jobs = assigned_sic_jobs.merge(
            distributed_sic_jobs,
            how="outer",
            on=[LOOKUP_CODE_COLUMN, SIC_COlUMN],
            suffixes=["_assigned", "_distributed"],
        )
        all_sic_jobs.fillna(0, inplace=True)
        all_sic_jobs["2050"] = (
            all_sic_jobs["2050_assigned"] + all_sic_jobs["2050_distributed"]
        )
        all_sic_jobs.drop(columns=["2050_assigned", "2050_distributed"], inplace=True)
        all_sic_jobs.set_index(
            [LOOKUP_CODE_COLUMN, SIC_COlUMN], drop=True, inplace=True
        )
        all_sic_jobs.sort_index(level=0, inplace=True)

        return all_sic_jobs
    elif assigned_sic_jobs is not None:
        assigned_sic_jobs = assigned_sic_jobs.set_index(
            [LOOKUP_CODE_COLUMN, SIC_COlUMN], drop=True
        ).sort_index(level=0)
        return assigned_sic_jobs
    elif distributed_sic_jobs is not None:
        distributed_sic_jobs = distributed_sic_jobs.set_index(
            [LAD_COLUMN, SIC_COlUMN], drop=True
        ).sort_index(level=0)
        return distributed_sic_jobs
    else:
        raise IndexError(
            "Jobs input must contained columns name of either/or 'unassigned_jobs' and two digit sic codes"
        )

def process_pop_data(
    population_data: pd.DataFrame,
    traveller_type_split: pd.DataFrame,
    msoa_to_lad_conversion: pd.DataFrame,
    dwelling_split: pd.DataFrame,
    lad_name_code: pd.DataFrame,
) -> pd.DataFrame:
    population_data = name_to_code(
        population_data,
        lad_name_code,
        LAD_COLUMN,
        LOOKUP_LAD_COLUMN,
        LOOKUP_CODE_COLUMN,
    )
    tt_ratios = land_use.analyse_traveller_type_distribution(traveller_type_split)
    tt_ratios = aggregate_msoa_to_lad(
        tt_ratios.reset_index(),
        msoa_to_lad_conversion,
        "msoa_zone_id",
        "msoa_zone_id",
        "ratios",
        ["lad_2020_zone_id", "tfn_traveller_type"],
        "msoa_to_lad_2020",
    )
    tt_ratios.columns = ["ratios"]
    msoa_pop_column_names = [
        "zone_id",
        "dwelling_type",
        "n_uprn",
        "pop_per_dwelling",
        "zone",
        "pop_aj_factor",
        "population",
    ]
    dwelling_ratios = population_dwelling_proportion(
        dwelling_split, msoa_pop_column_names
    )
    dwelling_ratios = dwelling_ratios.reset_index().merge(
        msoa_to_lad_conversion, left_on="zone_id", right_on="msoa_zone_id"
    )
    dwelling_ratios = dwelling_ratios.groupby(["lad_2020_zone_id", "dwelling_type"]).sum()
    dwelling_ratios = (
        dwelling_ratios["population"] / dwelling_ratios["population"].groupby("lad_2020_zone_id").sum()
    ).to_frame(name="ratio")
    dwelling_ratios.columns = ["ratio"]
    disagg_dwellings = disagg_dwelling(
        population_data, dwelling_ratios, ["population"], "LAD20CD", "lad_2020_zone_id"
    )
    disagg_pop_data = land_use.apply_pop_land_use(
        disagg_dwellings, "population", tt_ratios, "LAD20CD", "lad_2020_zone_id"
    ).to_frame()
    disagg_pop_data.columns = ["2050"]
    return disagg_pop_data


def name_to_code(
    data: pd.DataFrame,
    lookup: pd.DataFrame,
    name_data_column: str,
    name_lookup_column: str,
    code_lookup_column,
) -> pd.DataFrame:
    lookup = lookup.loc[:, [name_lookup_column, code_lookup_column]]
    code_data = data.merge(
        lookup, left_on=name_data_column, right_on=name_lookup_column
    ).drop(columns=[name_lookup_column])
    return code_data


def aggregate_msoa_to_lad(
    msoa_data: pd.DataFrame,
    msoa_to_lad_lookup: pd.DataFrame,
    data_zone_col: str,
    lookup_msoa_zone_col: str,
    on: str,
    by: list[str],
    weights_col: str,
) -> pd.DataFrame:
    joined_data = msoa_data.merge(
        msoa_to_lad_lookup, left_on=data_zone_col, right_on=lookup_msoa_zone_col
    )
    converted_data = joined_data.groupby(by).apply(
        lambda x: np.average(x[on], weights=x[weights_col])
    )
    return converted_data.to_frame()


def disagg_dwelling(
    data: pd.DataFrame,
    zone_ratio: pd.DataFrame,
    unit_columns: list[str],
    data_zone_column: str = "msoa11cd",
    ratio_zone_column: str = "zone_id",
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

    zone_ratio.reset_index("dwelling_type", inplace=True)

    data = data.merge(
        zone_ratio, how="left", left_on=data_zone_column, right_on=ratio_zone_column
    )

    for column in unit_columns:
        data.loc[:, column] = data[column] * data["ratio"]

    return data


def population_dwelling_proportion(
    msoa_pop_path: pathlib.Path, columns: list[str]
) -> pd.DataFrame:
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
    msoa_pop["dwelling_ratio"] = (
        msoa_pop["population"] / msoa_pop["population"].groupby(level="zone_id").sum()
    )

    return msoa_pop


def combine_forecasts(
    baseline: pd.DataFrame, additional: pd.DataFrame, lad_name_to_code: pd.DataFrame
) -> pd.DataFrame:
    baseline.set_index("Name", inplace=True)
    additional.set_index("Name", inplace=True)
    try:
        baseline_jobs = baseline["Jobs"].apply(lambda x: float(x.replace(",", "")))
    except AttributeError:
        baseline_jobs = baseline["Jobs"]

    try:
        additional_jobs = additional["Jobs"].apply(lambda x: float(x.replace(",", "")))
    except AttributeError:
        additional_jobs = additional["Jobs"]

    try:
        baseline_pop = baseline["Total"].apply(lambda x: float(x.replace(",", "")))
    except AttributeError:
        baseline_pop = baseline["Total"]

    try:
        additional_pop = additional["Total"].apply(lambda x: float(x.replace(",", "")))
    except AttributeError:
        additional_pop = additional["Total"]

    total_jobs = baseline_jobs + additional_jobs
    total_pop = baseline_pop + additional_pop
    forecast = pd.DataFrame({"jobs": total_jobs, "population": total_pop})
    return name_to_code(forecast, lad_name_to_code, "Name", "LAD20NM", "LAD20CD")


def distribute_baseline_jobs(
    baseline_forecast: pd.DataFrame,
    luc_distribution: pd.DataFrame,
    luc_to_sic,
    lad_name_code_lookup: pd.DataFrame,
) -> pd.DataFrame:
    baseline_forecast = name_to_code(
        baseline_forecast,
        lad_name_code_lookup,
        "Name",
        LOOKUP_LAD_COLUMN,
        LOOKUP_CODE_COLUMN,
    )

    luc_distribution.drop(
        luc_distribution[luc_distribution["land_use_codes"] == "greenfield"].index,
        inplace=True,
    )
    luc_distribution.drop(
        luc_distribution[luc_distribution["land_use_codes"] == "brownfield"].index,
        inplace=True,
    )

    luc_to_sic.drop(
        luc_to_sic[luc_to_sic["land_use_code"] == "greenfield"].index,
        inplace=True,
    )

    luc_to_sic.drop(
        luc_to_sic[luc_to_sic["land_use_code"] == "brownfield"].index,
        inplace=True,
    )

    unassigned_jobs = baseline_forecast.loc[:, ["LAD20CD", "Jobs"]]
    # add land use codes to distribute to
    unassigned_jobs["land_use_codes"] = None
    unassigned_jobs["land_use_codes"] = unassigned_jobs["land_use_codes"].apply(
        lambda x: luc_distribution["land_use_codes"].tolist()
    )
    unassigned_jobs["Jobs"] = unassigned_jobs["Jobs"].apply(
        lambda x: float(x.replace(",", ""))
    )

    # distribute jobs by land use code
    distributed_jobs = land_use.disagg_land_use_codes(
        unassigned_jobs,
        "land_use_codes",
        "Jobs",
        luc_distribution,
        "LAD20CD",
    )

    distributed_jobs = distributed_jobs.reset_index(drop=False).merge(
            luc_to_sic,
            how="left",
            left_on="land_use_codes",
            right_on="land_use_code",
    )
    distributed_jobs.drop(
        columns=["land_use_code", "land_use_codes", "index"], inplace=True
    )
    distributed_jobs = distributed_jobs.groupby(["LAD20CD", "sic_code"]).sum()
    return distributed_jobs.loc[distributed_jobs["Jobs"] > 0]


def distribute_baseline_population(
    baseline_forecast: pd.DataFrame,
    msoa_to_lad_conversion: pd.DataFrame,
    tt_split: pd.DataFrame,
    dwelling_split: pd.DataFrame,
    lad_name_code: pd.DataFrame,
) -> pd.DataFrame:
    population_data = name_to_code(
        baseline_forecast,
        lad_name_code,
        "Name",
        LOOKUP_LAD_COLUMN,
        LOOKUP_CODE_COLUMN,
    )
    tt_ratios = land_use.analyse_traveller_type_distribution(tt_split)
    tt_ratios = aggregate_msoa_to_lad(
        tt_ratios.reset_index(),
        msoa_to_lad_conversion,
        "msoa_zone_id",
        "msoa_zone_id",
        "ratios",
        ["lad_2020_zone_id", "tfn_traveller_type"],
        "msoa_to_lad_2020",
    )
    tt_ratios.columns = ["ratios"]
    msoa_pop_column_names = [
        "zone_id",
        "dwelling_type",
        "n_uprn",
        "pop_per_dwelling",
        "zone",
        "pop_aj_factor",
        "population",
    ]
    dwelling_ratios = population_dwelling_proportion(
        dwelling_split, msoa_pop_column_names
    )
    dwelling_ratios = dwelling_ratios.reset_index().merge(
        msoa_to_lad_conversion, left_on="zone_id", right_on="msoa_zone_id"
    )
    dwelling_ratios = dwelling_ratios.groupby(["lad_2020_zone_id", "dwelling_type"]).sum()
    dwelling_ratios = (
        dwelling_ratios["population"] / dwelling_ratios["population"].groupby("lad_2020_zone_id").sum()
    ).to_frame(name="ratio")

    population_data["Total"] = population_data["Total"].apply(
        lambda x: float(x.replace(",", ""))
    )
    disagg_dwellings = disagg_dwelling(
        population_data, dwelling_ratios, ["Total"], "LAD20CD", "lad_2020_zone_id"
    )
    disagg_pop_data = land_use.apply_pop_land_use(
        disagg_dwellings, "Total", tt_ratios, "LAD20CD", "lad_2020_zone_id"
    ).to_frame()
    disagg_pop_data.columns = ["Total"]
    return disagg_pop_data


def add_baseline_growth(
    calculated_growth: pd.DataFrame,
    baseline_growth: pd.DataFrame,
    zone_name_code_lookup: pd.DataFrame,
    calc_value_col: str,
    baseline_value_col: str,
    baseline_zone_name: str,
    lookup_zone_name: str,
    lookup_zone_code: str,
) -> pd.DataFrame:
    merged_growth = calculated_growth.merge(
        baseline_growth, how="outer", left_index=True, right_index=True
    )
    merged_growth.fillna(0, inplace=True)
    merged_growth[calc_value_col] = (
        merged_growth[calc_value_col] + merged_growth[baseline_value_col]
    )
    merged_growth.drop(columns=[baseline_value_col], inplace=True)
    return merged_growth


def lad_to_msoa(
    lad_data: pd.DataFrame,
    lad_to_msoa_lookup: pd.DataFrame,
    data_lad_col: str,
    lookup_lad_col: str,
    msoa_col: str,
    value_col: str,
    factor_col: str,
) -> pd.DataFrame:
    msoa_data = lad_data.merge(
        lad_to_msoa_lookup, how="left", left_on=data_lad_col, right_on=lookup_lad_col
    )
    msoa_data[value_col] = msoa_data[value_col] * msoa_data[factor_col]
    keep_cols = lad_data.columns.to_list()
    keep_cols.remove(data_lad_col)
    keep_cols.append(msoa_col)
    msoa_data = msoa_data.loc[:, keep_cols]
    return msoa_data