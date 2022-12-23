#standard imports
import pathlib
import os
import logging
#third party imports
import pandas as pd

#local imports
from dlit_lu import global_classes, utilities

LOG = logging.getLogger(__name__)

def user_input_file_builder(path: pathlib.Path, data: global_classes.DLogData)->None:
    data = utilities.to_dict(data)
    utilities.write_to_excel(path, data)


def infill_user_inputs(data: dict[str, pd.DataFrame], modified_path: pathlib.Path, sheet_names: dict[str, str], uneditable_columns: dict[str, list[str]]):
    # Read in the Excel file containing the subset of the data
    infilled_data = {}
    if not os.path.exists(modified_path):
        raise FileNotFoundError(f"{modified_path} does not exisit")
    for key, value in data.items():
        
        data_subset = pd.read_excel(modified_path, engine = "openpyxl", sheet_name=sheet_names[key])
        if len(uneditable_columns[key]) == 0:
            infilled_data[key] = value.copy()
            # Update the values in the original dataframe with the values in the subset
            infilled_data[key].update(data_subset)
            continue
        # Check whether any values in the selected columns have been changed
        if not value[uneditable_columns[key]].equals(data_subset[uneditable_columns[key]]):
            infilled_data[key] = value.copy()
            # Update the values in the original dataframe with the values in the subset
            infilled_data[key].update(data_subset)
        else:
            raise ValueError(f"values in {uneditable_columns[key]} within {modified_path} have been modified, these values must remain constant")
    return infilled_data
