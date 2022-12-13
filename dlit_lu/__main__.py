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
#third party imports 
from tqdm.contrib import logging as tqdm_log
# local imports
from dlit_lu import inputs, parser, utilities, analyse, syntax_fixes

# constants
CONFIG_PATH = pathlib.Path("d_lit-config.yml")
LOG = logging.getLogger(__package__)
LOG_FILE = "DLIT.log"

def main(log: utilities.DLitLog) -> None:
    """DLit DLog land use analysis and repair tool

    Parameters
    ----------
    log : utilities.DLitLog
        logging object
    """    

    config = inputs.DLitConfig.load_yaml(CONFIG_PATH)

        
    config.output_folder.mkdir(exist_ok=True)
    
    log.add_file_handler(config.output_folder / LOG_FILE)

    dlog_data = parser.parse_dlog(
        input_file_path=config.dlog_input_file,
        comb_sheet_name=config.combined_sheet_name,
        res_sheet_name=config.residential_sheet_name,
        emp_sheet_name=config.employment_sheet_name,
        mix_sheet_name=config.mixed_sheet_name,
        lookup_sheet_name=config.lookups_sheet_name,
        comb_column_names_path=config.combined_column_names_path,
        res_column_names_path=config.residential_column_names_path,
        emp_column_names_path=config.employment_column_names_path,
        mix_column_names_path=config.mixed_column_names_path,
    )
    auxiliary_data = parser.read_auxiliary_data(
        config.valid_luc_path,
        config.out_of_date_luc_path,
        config.incomplete_luc_path,
        config.regions_shapefiles_path,
        )
    #TODO testing setup for syntax fixes
    syntax_fixes.fix_inavlid_syntax(dlog_data,auxiliary_data)
     
    analyse.data_report(dlog_data, config.data_report_file_path, config.output_folder, auxiliary_data)
if __name__ == "__main__":
    # Initialise the program logger and setup to work with tqdm
    with utilities.DLitLog() as dlit_log:
        with tqdm_log.logging_redirect_tqdm([dlit_log.logger]):
            main(dlit_log)