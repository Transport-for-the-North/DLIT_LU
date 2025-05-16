"""DLit DLog land use analysis and repair tool: Version 0.0.0: 09/12/2021

This tool reads the DLog excel spreadsheet and analyses the data
for any invalid data, returning the findings in a excel
spradsheet. Any fixes that can be perfomed automatically will be
performed. Any issues that require user intervention will be
outputted to the user, to be infilled manually, read back in and
intergrated with the DLog

Kieran Fishwick: kieran.fishwick@wsp.com
"""

# standard imports
import pathlib
import logging
import argparse

# third party imports
from tqdm.contrib import logging as tqdm_log

# local imports
from dlit_lu import (
    constraint,
    infilling,
    inputs,
    utilities,
    land_use,
    parser,
    large_sites,
    split,
    # dev_pattern,
    tripend,
)

# constants
CONFIG_PATH = pathlib.Path("d_lit-config.yml")
LOG = logging.getLogger(__package__)
LOG_FILE = "DLIT.log"


def run(args: argparse.Namespace) -> None:
    """initilises Logging and calls main"""
    with utilities.DLitLog() as dlit_log:
        with tqdm_log.logging_redirect_tqdm([dlit_log.logger]):
            main(dlit_log, args)


def main(log: utilities.DLitLog, args: argparse.Namespace) -> None:
    """DLit DLog land use analysis and repair tool

    Parameters
    ----------
    log : utilities.DLitLog
        logging object
    """
    # load in config file
    config = inputs.DLitConfig.load_yaml(args.config)

    config.output_folder.mkdir(exist_ok=True)

    # set log file
    log.add_file_handler(config.output_folder / LOG_FILE)

    if config.run_infill:
        infilled_data = infilling.run(config, args)
    else:
        infilled_data = parser.parse_land_use_input(config)

    if config.run_land_use and infilled_data is not None:
        land_use.run(infilled_data, config)

    assess_data = parser.parse_large_sites_input(config)
    if config.run_large_sites and assess_data is not None:
        fy_zone_data = large_sites.run(assess_data, config)
    else:
        fy_zone_data = parser.parse_fy_zone_input(config)

    if config.run_split and fy_zone_data is not None:
        split.run(fy_zone_data, config)

    if config.run_constraint:
        constraint.run(config)

    if config.run_tripend:
        tripend.run(config)
