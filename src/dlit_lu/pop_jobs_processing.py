# standard imports
import argparse
import logging
import pathlib
from typing import Optional

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
SIC_COLUMN = "sic_code"
LOOKUP_LAD_COLUMN = "LAD20NM"
LOOKUP_CODE_COLUMN = "LAD20CD"
MODEL_ZONE_COL = "miham_zone_id"


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


    #I dont think these do anything useful --------
    forecast = combine_forecasts(
        parsed_inputs.forecast_year_growth.copy(),
        parsed_inputs.lad_name_lookup.copy(),
        change_name_to_code = True
        #parsed_inputs.additional_growth.copy(),
    )
    #to process NTEM data
    
    forecast_no_addtional = combine_forecasts(
        parsed_inputs.forecast_year_growth.copy(),
        parsed_inputs.lad_name_lookup.copy(),
        change_name_to_code =False, 
    )

    processed_forecast_no_addtional = process_job_forecast(
        forecast_no_addtional.reset_index(),
        parsed_inputs.luc_sic_conversion,
        parsed_inputs.proposed_luc_split,
        parsed_inputs.msoa_to_lad_conversion,
        zone_name= "Name",
    )
    processed_forecast_no_addtional = name_to_code(
        processed_forecast_no_addtional,
        parsed_inputs.lad_name_lookup,
        "Name",
        "LAD20NM",
        "LAD20CD"
    )
    #below actually does something


    data = format_data(parsed_inputs)

    jobs_check = data["jobs"].groupby("LAD20CD").sum()
    jobs_check = jobs_check.merge(parsed_inputs.lad_name_lookup, on="LAD20CD")

    
    utilities.write_to_csv(
        parsed_inputs.output_folder / "LAD_job_totals.csv", jobs_check
    )

    LOG.info("Constraining Population")
    constrained_pop = constraint.constrain_to_forecast(
        data["population"],
        forecast.copy(),
        parsed_inputs.lad_name_lookup,
        "2050",
        LOOKUP_CODE_COLUMN,
        "population",
    )
    LOG.info("Constraining Jobs")
    constrained_jobs = constraint.constrain_to_forecast(
        data["jobs"],
        forecast.copy(),
        parsed_inputs.lad_name_lookup,
        "2050",
        LOOKUP_CODE_COLUMN,
        "jobs",
    )
    utilities.write_to_csv(
        parsed_inputs.output_folder / "constrained_jobs.csv", constrained_jobs
    )

    utilities.write_to_csv(
        parsed_inputs.output_folder / "constrained_population.csv", constrained_pop
    )

    #drop duplicates, as all the constraint factors should be equal
    constrained_pop = constrained_pop.reset_index()
    pop_constraint_ratio = constrained_pop[["LAD20CD", "constraint_factor"]].drop_duplicates()
    pop_constraint_ratio.set_index("LAD20CD")
    #processing population allocated to development zone
    allocated_pop = process_pop_data(
        parsed_inputs.allocated_land_use,
        parsed_inputs.msoa_traveller_type,
        parsed_inputs.msoa_to_lad_conversion,
        parsed_inputs.msoa_dwelling_pop,
        parsed_inputs.lad_name_lookup,
        "LAD20NM",
        [MODEL_ZONE_COL],
    )

    allocated_pop_indices = allocated_pop.index.names
    allocated_pop.reset_index(inplace=True)
    allocated_pop_cols = allocated_pop.columns

    allocated_pop = allocated_pop.merge(pop_constraint_ratio, how = "left", on="LAD20CD")
    allocated_pop_constraint_factor = allocated_pop["constraint_factor"]
    allocated_pop_constraint_factor[allocated_pop_constraint_factor>1]=1
    allocated_pop["2050"] = allocated_pop["2050"]*allocated_pop_constraint_factor
    allocated_pop = allocated_pop[allocated_pop_cols]
    allocated_pop.set_index(allocated_pop_indices, inplace=True)
    

    constrained_pop = lad_to_model_zone(
        constrained_pop,
        parsed_inputs.lad_to_model_zone_pop,
        allocated_pop,
        "LAD20CD",
        "lad_2020_zone_id",
        "mrtm2_zone_id",
        MODEL_ZONE_COL,
        "2050",
        "lad_2020_to_mrtm2",
        ["dwelling_type", "tfn_traveller_type"],
    )
    constrained_pop.rename(columns={"miham_zone_id": "miham_dev_zone_id"}, inplace=True)
    constrained_pop.set_index(
        ["mrtm2_zone_id", "dwelling_type", "tfn_traveller_type"], inplace=True
    )

    utilities.write_to_csv(
        parsed_inputs.output_folder / "constrained_population_model_zone.csv",
        constrained_pop,
    )
    constrained_jobs_check = constrained_jobs.groupby("LAD20CD").sum()
    constrained_jobs_check = constrained_jobs_check.merge(
        parsed_inputs.lad_name_lookup, on="LAD20CD"
    )
    utilities.write_to_csv(
        parsed_inputs.output_folder / "LAD_job_constrained_totals.csv",
        constrained_jobs_check,
    )

    constrained_jobs.reset_index(inplace=True)
    allocated_jobs = parsed_inputs.allocated_land_use.loc[
        :, [MODEL_ZONE_COL, "LAD20NM", "jobs"]
    ]
    allocated_jobs.rename(
        columns={"LAD20NM": "LAD", "jobs": UNASSIGNED_JOBS_COLUMN}, inplace=True
    )


    jobs_constraint_ratio = constrained_jobs[["LAD20CD", "constraint_factor"]].drop_duplicates()
    jobs_constraint_ratio.set_index("LAD20CD")


    allocated_jobs = process_job_data(
        allocated_jobs,
        parsed_inputs.luc_sic_conversion,
        parsed_inputs.proposed_luc_split,
        parsed_inputs.lad_name_lookup,
        aggregate_to="miham_zone_id",
    )
    allocated_jobs_indices = allocated_jobs.index.names
    allocated_jobs.reset_index(inplace=True)
    allocated_jobs_cols = allocated_jobs.columns

    allocated_jobs = allocated_jobs.merge(jobs_constraint_ratio, how = "left", on="LAD20CD")
    allocated_jobs_constraint_factor = allocated_jobs["constraint_factor"]
    allocated_jobs_constraint_factor[allocated_jobs_constraint_factor>1]=1
    allocated_jobs["2050"] = allocated_jobs["2050"]*allocated_jobs_constraint_factor
    allocated_jobs = allocated_jobs[allocated_jobs_cols]
    allocated_jobs.set_index(allocated_jobs_indices, inplace =True)


    msoa_allocated_jobs = (
        allocated_jobs.reset_index(drop=False)
        .rename(columns={"miham_zone_id": "msoa_zone_id"})
        .set_index(["LAD20CD", "sic_code", "msoa_zone_id"])
    )

    dist_base_year_jobs = distribute_baseline_jobs(
        parsed_inputs.base_year_pop_emp,
        parsed_inputs.proposed_luc_split,
        parsed_inputs.luc_sic_conversion,
        parsed_inputs.lad_name_lookup,
    )

    jobs_diff = remove_baseline_growth(
        constrained_jobs.set_index(["LAD20CD", "sic_code"]),
        dist_base_year_jobs,
        "2050",
        "Jobs",
    )

    jobs_diff_msoa = lad_to_model_zone(
        jobs_diff.reset_index(),
        parsed_inputs.msoa_to_lad_conversion,
        msoa_allocated_jobs.copy(),
        "LAD20CD",
        "lad_2020_zone_id",
        "msoa_zone_id",
        "msoa_zone_id",
        "2050",
        "lad_2020_to_msoa",
        ["sic_code"],
    )

    jobs_diff_msoa_sic_section = convert_sic_division_to_section(
        jobs_diff_msoa, parsed_inputs.luc_sic_conversion, "msoa_zone_id", []
    )

    

    utilities.write_to_csv(parsed_inputs.output_folder / "jobs_diff.csv", jobs_diff_msoa_sic_section)

    constrained_msoa_jobs = lad_to_model_zone(
        constrained_jobs,
        parsed_inputs.msoa_to_lad_conversion,
        msoa_allocated_jobs.copy(),
        "LAD20CD",
        "lad_2020_zone_id",
        "msoa_zone_id",
        "msoa_zone_id",
        "2050",
        "lad_2020_to_msoa",
        ["sic_code"],
    )

    constrained_jobs = lad_to_model_zone(
        constrained_jobs,
        parsed_inputs.lad_to_model_zone_jobs,
        allocated_jobs,
        "LAD20CD",
        "lad_2020_zone_id",
        "mrtm2_zone_id",
        MODEL_ZONE_COL,
        "2050",
        "lad_2020_to_mrtm2",
        ["sic_code"],
    )

    utilities.write_to_csv(
        parsed_inputs.output_folder / "constrained_jobs_msoa.csv", constrained_msoa_jobs
    )
    constrained_jobs.rename(
        columns={"mrtm2_zone_id": "miham_dev_zone_id"}, inplace=True
    )
    constrained_jobs.set_index(["miham_dev_zone_id", "sic_code"], inplace=True)
    utilities.write_to_csv(
        parsed_inputs.output_folder / "constrained_jobs_model_zone.csv",
        constrained_jobs["2050"],
    )


def process_job_forecast(
    forecast_data: pd.DataFrame,
    luc_to_sic: pd.DataFrame,
    luc_distribution: pd.DataFrame,
    msoa_to_lad_conversion: pd.DataFrame,
    zone_name: str = "LAD20CD"
) -> pd.DataFrame:
    forecast_data = forecast_data.loc[:, [zone_name, "jobs"]]

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

    # add land use codes to distribute to
    forecast_data["land_use_codes"] = None
    forecast_data["land_use_codes"] = forecast_data["land_use_codes"].apply(
        lambda x: luc_distribution["land_use_codes"].tolist()
    )
    # distribute jobs by land use code

    distributed_jobs = land_use.disagg_land_use_codes(
        forecast_data,
        "land_use_codes",
        "jobs",
        luc_distribution,
        zone_name,
    )

    distributed_sic_jobs = distributed_jobs.merge(
        luc_to_sic,
        how="left",
        left_on="land_use_codes",
        right_on="land_use_code",
    )

    distributed_sic_jobs = (
        distributed_sic_jobs.loc[:, [zone_name, "sic_code_section", "jobs"]]
        .groupby([zone_name, "sic_code_section"])
        .sum()
        .reset_index()
    )

    return distributed_sic_jobs


def format_data(inputs: inputs.JobPopInputs) -> dict[str, pd.DataFrame]:
    LOG.info("distributing reference growth jobs data")
    #dist_baseline_jobs = distribute_baseline_jobs(
    #    inputs.baseline_growth,
    #    inputs.proposed_luc_split,
    #    inputs.luc_sic_conversion,
    #    inputs.lad_name_lookup,
    #)

    LOG.info("distributing reference growth population data")
    #dist_baseline_pop = distribute_baseline_population(
    #    inputs.baseline_growth,
    #    inputs.msoa_to_lad_conversion,
    #    inputs.msoa_traveller_type,
    #    inputs.msoa_dwelling_pop,
    #    inputs.lad_name_lookup,
    #)
    LOG.info("processing jobs forecast")

    forecast_year_pop = inputs.forecast_year_growth[["Name", "Total"]]
    forecast_year_jobs = inputs.forecast_year_growth[["Name", "Jobs"]]

    forecast_year_pop.rename(columns={"Name":"LAD", "Total":"population"}, inplace = True)
    forecast_year_jobs.rename(columns={"Name":"LAD", "Jobs":"unassigned_jobs"}, inplace = True)
    try:
        forecast_year_pop["population"] = forecast_year_pop["population"].str.replace(",", "").astype(float)
    except:
        LOG.info("Growth already had processing - this is fine")
    try:
        forecast_year_jobs["unassigned_jobs"] = forecast_year_jobs["unassigned_jobs"].str.replace(",", "").astype(float)
    except:
        LOG.info("Growth already had processing - this is fine")

    forecast_year_pop.set_index("LAD", inplace=True)
    forecast_year_jobs.set_index("LAD", inplace=True)
    #dropping data we already have in NTEM
    forecast_year_pop.drop(index=inputs.population_input.index, inplace= True)
    forecast_year_jobs.drop(index=inputs.jobs_input.index, inplace= True)

    gb_pop =pd.concat([inputs.population_input, forecast_year_pop])
    gb_jobs = pd.concat([inputs.jobs_input, forecast_year_jobs])
    gb_jobs.fillna(0, inplace=True)
    jobs = process_job_data(
        gb_jobs,
        inputs.luc_sic_conversion,
        inputs.proposed_luc_split,
        inputs.lad_name_lookup,
    )

    LOG.info("processing population forecast")
    population = process_pop_data(
        gb_pop,
        inputs.msoa_traveller_type,
        inputs.msoa_to_lad_conversion,
        inputs.msoa_dwelling_pop,
        inputs.lad_name_lookup,
    )
    LOG.info("combining forecast and reference growth jobs")

    LOG.info("outputting formatted jobs")
    utilities.write_to_csv(
        inputs.output_folder / "formatted_distributed_jobs.csv", jobs
    )

    #jobs = add_baseline_growth(
    #    jobs,
    #    dist_baseline_jobs,
    #    inputs.lad_name_lookup,
    #    "2050",
    #    "Jobs",
    #    "Name",
    #    LOOKUP_LAD_COLUMN,
    #    LOOKUP_CODE_COLUMN,
    #)

    LOG.info("combining forecast and reference growth population")

    utilities.write_to_csv(
        inputs.output_folder / "formatted_distributed_population.csv", population
    )

    #population = add_baseline_growth(
    #    population,
    #    dist_baseline_pop,
    #    inputs.lad_name_lookup,
    #    "2050",
    #    "Total",
    #    "Name",
    #    LOOKUP_LAD_COLUMN,
    #    LOOKUP_CODE_COLUMN,
    #)

    return {"population": population, "jobs": jobs}


def convert_sic_division_to_section(
    data: pd.DataFrame, sic_div_to_sec: pd.DataFrame, zone_name: str, groupby_cols
) -> pd.DataFrame:
    data.reset_index(inplace=True)
    sic_div_to_sec_lookup = sic_div_to_sec[["sic_code", "sic_code_section"]].drop_duplicates()
    merged_data = data.merge(sic_div_to_sec_lookup, on="sic_code").drop(columns=["sic_code"])
    merged_data.rename(columns={"sic_code_section": "sic_code"}, inplace=True)
    rebased_data = merged_data.groupby([zone_name, "sic_code"] + groupby_cols).sum()
    return rebased_data


def process_job_data(
    jobs: pd.DataFrame,
    luc_to_sic: pd.DataFrame,
    luc_distribution: pd.DataFrame,
    lad_name_lookup: pd.DataFrame,
    additional_indexes: Optional[list[str]] = None,
    aggregate_to: str = LOOKUP_CODE_COLUMN,
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
    columns = jobs.columns.to_list()

    if additional_indexes is None:
        additional_indexes = []

    if LAD_COLUMN in columns:
        columns.remove(LAD_COLUMN)

    jobs.reset_index(inplace=True)

    jobs.drop(jobs[jobs[LAD_COLUMN] == "sum"].index, inplace=True)

    jobs = name_to_code(
        jobs,
        lad_name_lookup,
        LAD_COLUMN,
        LOOKUP_LAD_COLUMN,
        LOOKUP_CODE_COLUMN,
    )

    if len(additional_indexes):
        jobs.set_index(additional_indexes, inplace=True)

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

        if aggregate_to != LOOKUP_CODE_COLUMN:
            keep_cols = additional_indexes + [aggregate_to]
            unassigned_jobs = jobs.loc[
                :, [LOOKUP_CODE_COLUMN, aggregate_to, UNASSIGNED_JOBS_COLUMN]
            ]

        else:
            keep_cols = additional_indexes
            unassigned_jobs = jobs.loc[:, [LOOKUP_CODE_COLUMN, UNASSIGNED_JOBS_COLUMN]]

        # add land use codes to distribute to
        unassigned_jobs["land_use_codes"] = None
        unassigned_jobs["land_use_codes"] = unassigned_jobs["land_use_codes"].apply(
            lambda x: luc_distribution["land_use_codes"].tolist()
        )
        # distribute jobs by land use code
        if len(additional_indexes) > 0:
            unassigned_jobs.reset_index(inplace=True)

        distributed_jobs = land_use.disagg_land_use_codes(
            unassigned_jobs,
            "land_use_codes",
            UNASSIGNED_JOBS_COLUMN,
            luc_distribution,
            LOOKUP_CODE_COLUMN,
            keep_cols,
        )

        # remove irrelevnt rows
        # distributed_jobs.drop(
        #    distributed_jobs[distributed_jobs[UNASSIGNED_JOBS_COLUMN] == 0].index,
        #    inplace=True,
        # )
        distributed_jobs.rename(
            columns={"land_use_codes": "land_use", "unassigned_jobs": "2050"},
            inplace=True,
        )
        # convert to SIC code
        luc_to_sic["land_use_code"] = luc_to_sic["land_use_code"].str.lower()
        distributed_sic_jobs = distributed_jobs.merge(
            luc_to_sic,
            how="left",
            left_on="land_use",
            right_on="land_use_code",
        )

        distributed_sic_jobs.drop(columns=["land_use_code", "land_use"], inplace=True)
        if aggregate_to != LOOKUP_CODE_COLUMN:
            distributed_sic_jobs = (
                distributed_sic_jobs.groupby(
                    [aggregate_to, LOOKUP_CODE_COLUMN, SIC_COLUMN]
                )
                .sum()
                .reset_index()
            )
        else:
            distributed_sic_jobs = (
                distributed_sic_jobs.groupby([aggregate_to, SIC_COLUMN])
                .sum()
                .reset_index()
            )

    # restructure assigned jobs
    assigned_columns = columns
    if UNASSIGNED_JOBS_COLUMN in assigned_columns:
        assigned_columns.remove(UNASSIGNED_JOBS_COLUMN)

    if aggregate_to in assigned_columns:
        assigned_columns.remove(aggregate_to)

    for col in additional_indexes:
        assigned_columns.remove(col)

    if len(assigned_columns) > 0:
        # test for invalid SIC codes
        try:
            columns = [int(float(x)) for x in assigned_columns]
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
            columns={"variable": SIC_COLUMN, "value": "2050"}, inplace=True
        )

        assigned_sic_jobs[SIC_COLUMN] = assigned_sic_jobs[SIC_COLUMN].astype(float).astype(int)

    if assigned_sic_jobs is not None and distributed_sic_jobs is not None:

        #TODO don't have time to figure out why LAD NaNs are appearing - this is a sticking plaster
        if assigned_sic_jobs["LAD20CD"].isna().any():
            if assigned_sic_jobs.loc[assigned_sic_jobs["LAD20CD"].isna(), "2050"].sum() == 0:
                assigned_sic_jobs.dropna(subset = "LAD20CD", inplace=True)
            else:
                raise ValueError("assigned sic jobs has nan LADs with jobs")
        if distributed_sic_jobs["LAD20CD"].isna().any():
            if distributed_sic_jobs.loc[assigned_sic_jobs["LAD20CD"].isna(), "2050"].sum() == 0:
                distributed_sic_jobs.dropna(subset = "LAD20CD", inplace=True)
            else:
                raise ValueError("distributed sic jobs has nan LADs with jobs")
            
        all_sic_jobs = assigned_sic_jobs.merge(
            distributed_sic_jobs,
            how="outer",
            on=[LOOKUP_CODE_COLUMN, SIC_COLUMN],
            suffixes=["_assigned", "_distributed"],
        )
        all_sic_jobs.fillna(0, inplace=True)
        all_sic_jobs["2050"] = (
            all_sic_jobs["2050_assigned"] + all_sic_jobs["2050_distributed"]
        )
        all_sic_jobs.drop(columns=["2050_assigned", "2050_distributed"], inplace=True)
        all_sic_jobs.set_index(
            [LOOKUP_CODE_COLUMN, SIC_COLUMN] + additional_indexes,
            drop=True,
            inplace=True,
        )
        all_sic_jobs.sort_index(level=0, inplace=True)

        return all_sic_jobs
    elif assigned_sic_jobs is not None:
        assigned_sic_jobs = assigned_sic_jobs.set_index(
            [LOOKUP_CODE_COLUMN, SIC_COLUMN] + additional_indexes, drop=True
        ).sort_index(level=0)
        return assigned_sic_jobs
    elif distributed_sic_jobs is not None:
        distributed_sic_jobs = distributed_sic_jobs.set_index(
            [aggregate_to, SIC_COLUMN] + additional_indexes, drop=True
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
    data_lad_column: pd.DataFrame = LAD_COLUMN,
    additonal_indexes: Optional[list[str]] = None,
) -> pd.DataFrame:
    if additonal_indexes is None:
        additonal_indexes = []
    population_data = name_to_code(
        population_data,
        lad_name_code,
        data_lad_column,
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
    dwelling_ratios = dwelling_ratios.groupby(
        ["lad_2020_zone_id", "dwelling_type"]
    ).sum()
    dwelling_ratios = (
        dwelling_ratios["population"]
        / dwelling_ratios["population"].groupby("lad_2020_zone_id").sum()
    ).to_frame(name="ratio")
    dwelling_ratios.columns = ["ratio"]
    disagg_dwellings = disagg_dwelling(
        population_data, dwelling_ratios, ["population"], "LAD20CD", "lad_2020_zone_id"
    )
    disagg_pop_data = land_use.apply_pop_land_use(
        disagg_dwellings,
        "population",
        tt_ratios,
        "LAD20CD",
        "lad_2020_zone_id",
        additonal_indexes,
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
        lookup, left_on=name_data_column, right_on=name_lookup_column, how = "left"
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
    baseline: pd.DataFrame,
    lad_name_to_code: pd.DataFrame,
    additional: Optional[pd.DataFrame] = None,
    change_name_to_code: bool = True,
) -> pd.DataFrame:
    baseline.set_index("Name", inplace=True)
    try:
        baseline_jobs = baseline["Jobs"].apply(lambda x: float(x.replace(",", "")))
    except AttributeError:
        baseline_jobs = baseline["Jobs"]

    try:
        baseline_pop = baseline["Total"].apply(lambda x: float(x.replace(",", "")))
    except AttributeError:
        baseline_pop = baseline["Total"]

    if additional is not None:
        additional.set_index("Name", inplace=True)
        try:
            additional_jobs = additional["Jobs"].apply(
                lambda x: float(x.replace(",", ""))
            )
        except AttributeError:
            additional_jobs = additional["Jobs"]

        try:
            additional_pop = additional["Total"].apply(
                lambda x: float(x.replace(",", ""))
            )
        except AttributeError:
            additional_pop = additional["Total"]

        total_jobs = baseline_jobs + additional_jobs
        total_pop = baseline_pop + additional_pop

    else:
        total_jobs = baseline_jobs
        total_pop = baseline_pop

    forecast = pd.DataFrame({"jobs": total_jobs, "population": total_pop})
    if change_name_to_code:
        return name_to_code(forecast, lad_name_to_code, "Name", "LAD20NM", "LAD20CD")
    else:
        return forecast


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
    try:
        unassigned_jobs["Jobs"] = unassigned_jobs["Jobs"].apply(
            lambda x: float(x.replace(",", ""))
        )
    except:
        LOG.info("Baseline jobs already formatted as floats - this is fine")

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
    dwelling_ratios = dwelling_ratios.groupby(
        ["lad_2020_zone_id", "dwelling_type"]
    ).sum()
    dwelling_ratios = (
        dwelling_ratios["population"]
        / dwelling_ratios["population"].groupby("lad_2020_zone_id").sum()
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


def remove_baseline_growth(
    calculated_growth: pd.DataFrame,
    baseline_growth: pd.DataFrame,
    calc_value_col: str,
    baseline_value_col: str,
) -> pd.DataFrame:
    merged_growth = calculated_growth.merge(
        baseline_growth, how="outer", left_index=True, right_index=True
    )
    merged_growth.fillna(0, inplace=True)
    merged_growth[calc_value_col] = (
        merged_growth[calc_value_col] - merged_growth[baseline_value_col]
    )
    merged_growth.drop(columns=[baseline_value_col], inplace=True)
    return merged_growth


def lad_to_model_zone(
    lad_data: pd.DataFrame,
    lad_to_zone_lookup: pd.DataFrame,
    allocated_land_use: pd.DataFrame,
    data_lad_col: str,
    lookup_lad_col: str,
    zone_col: str,
    allocated_zone_col: str,
    value_col: str,
    factor_col: str,
    groupby: list[str],
    keep_zone: bool = False,
) -> pd.DataFrame:
    zone_data = lad_data.merge(
        lad_to_zone_lookup,
        how="left",
        left_on=data_lad_col,
        right_on=lookup_lad_col,
    )

    allocated_land_use.reset_index(inplace=True)
    allocated_land_use.rename(columns={allocated_zone_col: zone_col}, inplace=True)
    allocated_land_use_agg = (
        allocated_land_use.loc[:, [data_lad_col, value_col] + groupby]
        .groupby([data_lad_col] + groupby)
        .sum()
    )

    zone_data = zone_data.merge(
        allocated_land_use_agg,
        how="outer",
        on=[data_lad_col] + groupby,
        suffixes=["_unallocated", "_allocated"],
    )
    zone_data[value_col + "_allocated"].fillna(0, inplace=True)

    zone_data[value_col] = (
        zone_data[value_col + "_unallocated"] - zone_data[value_col + "_allocated"]
    ) * zone_data[factor_col]
    keep_cols = lad_data.columns.to_list()

    keep_cols.remove(data_lad_col)
    keep_cols.append(zone_col)
    allocated_land_use.drop(columns=[data_lad_col], inplace=True)
    zone_data = zone_data.loc[:, keep_cols]

    zone_data = pd.concat([zone_data, allocated_land_use], ignore_index=True)
    return zone_data


def add_dev_zones(
    data: pd.DataFrame,
    dev_zones: pd.DataFrame,
) -> pd.DataFrame:
    data.merge(
        dev_zones,
        right_on="",
    )
