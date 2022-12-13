"""Automatically fixes and infills data where possible

    IN PROGRESS
"""
#third party imports
import pandas as pd
#local imports
from dlit_lu import global_classes
def fix_inavlid_syntax(data: global_classes.DLogData, auxiliary_data: global_classes.AuxiliaryData):
    #tempory testing setup
    data_ = {"residential":data.residential_data, "employment": data.employment_data, "mixed": data.mixed_data}
    incorrect_luc_formatting(data_, {"residential":["existing_land_use"], "employment":["existing_land_use","proposed_land_use"], "mixed":["existing_land_use","proposed_land_use"]}, auxiliary_data)

def incorrect_luc_formatting(invalid_luc_format: dict[str, pd.DataFrame], columns: dict[str,list[str]], auxiliary_data: global_classes.AuxiliaryData):
    #fixed_codes = {}
    #create incorrect format lookup table
    possible_error_codes = [s for s in auxiliary_data.allowed_codes["land_use_codes"].tolist() if "(" in s or ")" in s]
    wrong_format_check = [
        s.replace("(", "").replace(")", "") for s in possible_error_codes
    ]
    format_lookup = pd.DataFrame([possible_error_codes, wrong_format_check]).transpose()
    format_lookup.columns = ["land_use_code", "incorrect_format"]
    
    for key, value in invalid_luc_format.items():
        for column in columns[key]:
            exploded_land_use_codes = (#splits land use into new rows with same index
                value[column].explode()
            )
            #TODO revisit this section, possible to do with joins?
            for invalid_code in format_lookup["incorrect_format"]:
                exploded_land_use_codes[
                    exploded_land_use_codes==invalid_code
                    ] = format_lookup.loc[
                        format_lookup["incorrect_format"]==invalid_code, "land_use_code"
                    ]
            

            