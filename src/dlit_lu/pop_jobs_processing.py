# standard imports
import argparse
import logging

# third party
import pandas as pd
from tqdm.contrib import logging as tqdm_log
import logging

# local imports
from dlit_lu import utilities, inputs, land_use, utilities

LOG_FILE = "DLIT.log"

LOG = logging.getLogger(__package__)
UNASSIGNED_JOBS_COLUMN = "unassigned_jobs"
LAD_COLUMN = "LAD"
SIC_MIN = 1
SIC_MAX = 99
SIC_COlUMN = "sic_code"


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

    format_data(parsed_inputs)


def format_data(inputs: inputs.JobPopInputs) -> dict[str, pd.DataFrame]:
    population = inputs.population_input
    LOG.info("processing jobs data")
    jobs = process_job_data(
        inputs.jobs_input, inputs.luc_sic_conversion, inputs.proposed_luc_split
    )
    utilities.write_to_csv(inputs.output_folder/"formatted_distributed_jobs.csv", jobs)
    print("stop")


def process_job_data(
    jobs: pd.DataFrame, luc_to_sic: pd.DataFrame, luc_distribution: pd.DataFrame
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

        unassigned_jobs = jobs.loc[:, [LAD_COLUMN, UNASSIGNED_JOBS_COLUMN]]
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
            LAD_COLUMN,
        )

        # remove irrelevnt rows

        distributed_jobs.drop(
            distributed_jobs[distributed_jobs[UNASSIGNED_JOBS_COLUMN] == 0].index,
            inplace=True,
        )
        distributed_jobs.rename(
            columns={"land_use_codes": "land_use", "unassigned_jobs": "jobs"},
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
        distributed_sic_jobs.drop(columns=["land_use_code", "land_use", "index"], inplace=True)
        distributed_sic_jobs = distributed_sic_jobs.groupby([LAD_COLUMN, SIC_COlUMN]).sum().reset_index()

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
        assigned_columns.insert(0, LAD_COLUMN)
        assigned_jobs = jobs.loc[:, assigned_columns]

        assigned_sic_jobs = assigned_jobs.melt(
            id_vars=LAD_COLUMN, value_vars=assigned_columns
        )

        assigned_sic_jobs.rename(
            columns={"variable": SIC_COlUMN, "value": "jobs"}, inplace=True
        )

        assigned_sic_jobs[SIC_COlUMN] = assigned_sic_jobs[SIC_COlUMN].astype(int)

    if assigned_sic_jobs is not None and distributed_sic_jobs is not None:
        all_sic_jobs = assigned_sic_jobs.merge(
            distributed_sic_jobs,
            on=[LAD_COLUMN, SIC_COlUMN],
            suffixes=["_assigned", "_distributed"],
        )
        all_sic_jobs["jobs"] = all_sic_jobs["jobs_assigned"] + all_sic_jobs["jobs_distributed"]
        all_sic_jobs.drop(columns=["jobs_assigned", "jobs_distributed"], inplace= True)
        all_sic_jobs.set_index([LAD_COLUMN, SIC_COlUMN], drop=True, inplace=True)
        return all_sic_jobs.sort_index(level=0)
    elif assigned_sic_jobs is not None:
        return assigned_sic_jobs.set_index([LAD_COLUMN, SIC_COlUMN], drop=True).sort_index(level=0)
    elif distributed_sic_jobs is not None:
        return distributed_sic_jobs.set_index([LAD_COLUMN, SIC_COlUMN], drop=True).sort_index(level=0)
    else:
        raise IndexError("Jobs input must contained columns name of either/or 'unassigned_jobs' and two digit sic codes")
    
