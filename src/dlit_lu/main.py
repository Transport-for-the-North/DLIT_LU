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
# third party imports
from tqdm.contrib import logging as tqdm_log
# local imports
from dlit_lu import analyse_and_infill, inputs, utilities, land_use, parser

# constants
CONFIG_PATH = pathlib.Path("d_lit-config.yml")
LOG = logging.getLogger(__package__)
LOG_FILE = "DLIT.log"

def run() -> None:
    """initilises Logging and calls main
    """
    with utilities.DLitLog() as dlit_log:
        with tqdm_log.logging_redirect_tqdm([dlit_log.logger]):
            main(dlit_log)


def main(log: utilities.DLitLog) -> None:
    """DLit DLog land use analysis and repair tool

    Parameters
    ----------
    log : utilities.DLitLog
        logging object
    """
    #load in config file
    config = inputs.DLitConfig.load_yaml(CONFIG_PATH)
    config.check_inputs()
    
    config.output_folder.mkdir(exist_ok=True)

    #set log file
    log.add_file_handler(config.output_folder / LOG_FILE)

    if config.run_infill:
        infilled_data = analyse_and_infill.run(config)
    else:
        infilled_data = parser.parse_land_use_input(config)

    if config.run_land_use:
        land_use.run(infilled_data, config)

    #________________end of infilling____________________
   

    