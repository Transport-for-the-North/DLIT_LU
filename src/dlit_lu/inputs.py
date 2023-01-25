"""handles reading config file
"""
# standard imports
import json
import pathlib
from typing import Optional
import os 
# third party imports
import strictyaml
import pydantic


class BaseConfig(pydantic.BaseModel):
    r"""Base class for storing model parameters.

    Contains functionality for reading / writing parameters to
    config files in the YAML format. Class copied from NorMITs-Demand, source:
    https://github.com/Transport-for-the-North/NorMITs-Demand/blob/4b58a06c8ff6240d443b2dd5a3d9717e78a5afcc/normits_demand/utils/config_base.py)

    See Also
    --------
    [pydantic docs](https://pydantic-docs.helpmanual.io/):
        for more information about using pydantic's model classes.
    `pydantic.BaseModel`: which handles converting data to Python types.
    `pydantic.validator`: which allows additional custom validation methods.
    """

    @classmethod
    def from_yaml(cls, text: str):
        """Parse class attributes from YAML `text`.

        Parameters
        ----------
        text : str
            YAML formatted string, with parameters for
            the class attributes.

        Returns
        -------
        Instance of self
            Instance of class with attributes filled in from
            the YAML data.
        """
        data = strictyaml.load(text).data
        return cls.parse_obj(data)

    @classmethod
    def load_yaml(cls, path: pathlib.Path):
        """Read YAML file and load the data using `from_yaml`.

        Parameters
        ----------
        path : pathlib.Path
            Path to YAML file containing parameters.

        Returns
        -------
        Instance of self
            Instance of class with attributes filled in from
            the YAML data.
        """
        with open(path, "rt") as file:
            text = file.read()
        return cls.from_yaml(text)

    def to_yaml(self) -> str:
        """Convert attributes from self to YAML string.

        Returns
        -------
        str
            YAML formatted string with the data from
            the class attributes.
        """
        # Use pydantic to convert all types to json compatiable,
        # then convert this back to a dictionary to dump to YAML
        json_dict = json.loads(self.json())
        return strictyaml.as_document(json_dict).as_yaml()

    def save_yaml(self, path: pathlib.Path) -> None:
        """Write data from self to a YAML file.

        Parameters
        ----------
        path : pathlib.Path
            Path to YAML file to output.
        """
        with open(path, "wt") as file:
            file.write(self.to_yaml())


class DLitConfig(BaseConfig):
    """Manages reading / writing the tool's config file.


    Parameters
    ----------
    dlog_input_file: pathlib.Path
        Location of the DLog excel spreadsheet
    combined_sheet_name: str
        name of the combined sheet in the DLog spreadsheet
    residential_sheet_name: str
        name of the residential sheet in the DLog spreadsheet
    employment_sheet_name: str
        name of the employment sheet in the DLog spreadsheet
    mixed_sheet_name: str
        name of the mixed sheet in the DLog spreadsheet
    lookups_sheet_name: str
        name of the lookup sheet in the DLog spreadsheet
    output_folder: pathlib.Path
        file path of the folder where the outputs should be saved
    data_report_file_path: pathlib.Path
        file path where the data report should be saved
    combined_column_names_path: pathlib.Path
        path to a CSV containing the column names for the combined sheet in the DLog
        excel spreadsheet
    residential_column_names_path: pathlib.Path
        path to a CSV containing the column names for the residential sheet in the
        DLog excel spreadsheet
    employment_column_names_path: pathlib.Path
        path to a CSV containing the column names for the employment sheet in the
        DLog excel spreadsheet
    mixed_column_names_path: pathlib.Path
        path to a CSV containing the column names for the mixed sheet in the
        DLog excel spreadsheet
    ignore_columns_path: pathlib.Path
        path to a csv containing the columns in the dlog to ignore when reading
        in
    valid_luc_path: pathlib.Path
        path to a CSV containing valid land use codes
    out_of_date_luc_path: pathlib.Path
        path to a CSV containing out of date land use codes
    incomplete_luc_path: pathlib.Path
        path to a CSV containing incomplete land use codes 
    regions_shapefiles_path: pathlib.Path
        file path for the regions shape file
    user_inputs_path: pathlib.Path
        file path for the user input excel spreadsheet
    msoa_shapefile_path: pathlib.Path
        file path for the msoa shapefile

    Raises
    ------
    ValueError
        file doesn't exisit
    """
    #set up
    run_infill: bool
    run_land_use: bool

    #mandatory
    output_folder: pathlib.Path
    proposed_luc_split_path: pathlib.Path
    existing_luc_split_path: pathlib.Path
    dlog_input_file: pathlib.Path
    lookups_sheet_name: str

    #required for infill
    combined_sheet_name: Optional[str]
    residential_sheet_name: Optional[str]
    employment_sheet_name: Optional[str]
    mixed_sheet_name :Optional[str]
    combined_column_names_path: Optional[pathlib.Path]
    residential_column_names_path: Optional[pathlib.Path]
    employment_column_names_path: Optional[pathlib.Path]
    mixed_column_names_path: Optional[pathlib.Path]
    ignore_columns_path: Optional[pathlib.Path]
    user_input_path: Optional[pathlib.Path]
    valid_luc_path: Optional[pathlib.Path]
    out_of_date_luc_path: Optional[pathlib.Path]
    incomplete_luc_path: Optional[pathlib.Path]
    known_invalid_luc_path: Optional[pathlib.Path]
    regions_shapefiles_path: Optional[pathlib.Path]

    #required for land use
    land_use_input: Optional[pathlib.Path]
    msoa_shapefile_path: Optional[pathlib.Path]
    msoa_dwelling_pop_path: Optional[pathlib.Path]

    @pydantic.validator(
        "dlog_input_file",
    )
    def _file_exists(  # Validator is class method pylint: disable=no-self-argument
        cls, value: pathlib.Path
    ) -> pathlib.Path:
        if not value.is_file():
            raise ValueError(f"file doesn't exist: {value}")
        return value

    def check_inputs(self)->None:
        if self.run_land_use:
            self.check_land_use_params()
        if self.run_infill:
            self.check_infill_params()

    def check_infill_params(self)->None:
        str_params = [
            self.combined_sheet_name,
            self.residential_sheet_name,
            self.employment_sheet_name,
            self.mixed_sheet_name,
        ]
        read_path_params  = [
            self.combined_column_names_path,
            self.residential_column_names_path,
            self.employment_column_names_path,
            self.mixed_column_names_path,
            self.ignore_columns_path,
            self.valid_luc_path,
            self.out_of_date_luc_path,
            self.incomplete_luc_path,
            self.known_invalid_luc_path,
            self.regions_shapefiles_path,
            ]
        write_path_params  = [
            self.user_input_path,
        ]
        for param in write_path_params + read_path_params + str_params:
            if param is None:
                raise ValueError("Infill Parameters incomplete, please"
                " complete these within the config file before continuing. Cheers!")

        for param in read_path_params:
            if not os.path.exists(param):
                raise ValueError("Infill parameters contains write file paths"
                " that do not exist. Please update the config file before continuing. Cheers!")


    def check_land_use_params(self)->None:
    
        read_path_params  = [
            self.land_use_input,
            self.msoa_shapefile_path,
            self.msoa_dwelling_pop_path,
            ]
        write_path_params =[

        ]
        for param in read_path_params + write_path_params:
            if param is None:
                raise ValueError("Land use parameters incomplete, please"
                " complete these within the config file before continuing. Cheers!")
        for param in read_path_params:
            if not os.path.exists(param) or param == pathlib.Path("."):
                raise ValueError("Land use parameters contains write file paths"
                " that do not exist. Please update the config file before continuing. Cheers!")
    