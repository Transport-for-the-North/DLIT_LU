"""handles reading config file
"""
# standard imports
import json
import pathlib
from typing import Optional
import os
import dataclasses
# third party imports
import strictyaml
import pydantic

AVERAGE_INFILLING_VALUES_FILE = "infilling_average_values.yml"


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


@dataclasses.dataclass
class InfillConfig:
    """
    Manages 

    Parameters
    ----------
    combined_sheet_name: str
        name of the combined sheet within the D-Log file
    residential_sheet_name: str
        name of the residential sheet within the D-log file
    employment_sheet_name: str
        name of the employment sheet within the D-log file
    mixed_sheet_name: str
        name of the mixed sheet within the D-log file
    dlog_column_names_path: pathlib.Path
        path to column names in the dlog. contains column names for each
        sheet and column names to drop for all sheets 
    user_input_path: pathlib.Path
        Path to file when user inpjut file with but read/written
    valid_luc_path: pathlib.Path
        path to valid land_use codes file
    out_of_date_luc_path: pathlib.Path
        path to out of date land use codes file
    incomplete_luc_path: pathlib.Path
        path to incomplete land use codes file
    known_invalid_luc_path: pathlib.Path
        path to known invalid land use codes and their replacements 
    regions_shapefiles_path: pathlib.Path
        path to LPA regions shapefile

    """
    combined_sheet_name: str
    residential_sheet_name: str
    employment_sheet_name: str
    mixed_sheet_name: str
    dlog_column_names_path: pathlib.Path
    user_input_path: pathlib.Path
    valid_luc_path: pathlib.Path
    out_of_date_luc_path: pathlib.Path
    incomplete_luc_path: pathlib.Path
    known_invalid_luc_path: pathlib.Path
    regions_shapefiles_path: pathlib.Path

    def check_params(self) -> None:
        str_params = [
            self.combined_sheet_name,
            self.residential_sheet_name,
            self.employment_sheet_name,
            self.mixed_sheet_name,
        ]
        read_path_params = [
            self.dlog_column_names_path,
            self.valid_luc_path,
            self.out_of_date_luc_path,
            self.incomplete_luc_path,
            self.known_invalid_luc_path,
            self.regions_shapefiles_path,
        ]
        write_path_params = [
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


@dataclasses.dataclass
class LandUseConfig:
    """Manages reading / writing the tool's config file.


    Parameters
    ----------
    land_use_input: pathlib.Path
        path to land use input (output of infill)
    msoa_shapefile_path: pathlib.Path
        path to msoa shape file
    msoa_dwelling_pop_path: pathlib.Path
        path to msoa dwelling population file 
    msoa_traveller_type_path: pathlib.Path
        path to msoa split of traveller type
    employment_density_matrix_path: pathlib.Path
        path to employment density matrix
    luc_sic_conversion_path: pathlib.Path
        path to land use code to SIC code conversion matrix
    """
    land_use_input: pathlib.Path
    msoa_shapefile_path: pathlib.Path
    msoa_dwelling_pop_path: pathlib.Path
    msoa_traveller_type_path: pathlib.Path
    employment_density_matrix_path: pathlib.Path
    luc_sic_conversion_path: pathlib.Path

    def check_params(self) -> None:

        read_path_params = [
            self.land_use_input,
            self.msoa_shapefile_path,
            self.msoa_dwelling_pop_path,
            self.msoa_traveller_type_path,
            self.employment_density_matrix_path,
            self.luc_sic_conversion_path,
        ]
        write_path_params = [

        ]
        for param in read_path_params + write_path_params:
            if param is None:
                raise ValueError("Land use parameters incomplete, please"
                                 " complete these within the config file before continuing. Cheers!")
        for param in read_path_params:
            if not os.path.exists(param) or param == pathlib.Path("."):
                raise ValueError("Land use parameters contains write file paths"
                                 " that do not exist. Please update the config file before continuing. Cheers!")


class DLitConfig(BaseConfig):
    """Manages reading / writing the tool's config file.


    Parameters
    ----------
    run_infill: bool
        whether to run the infilling module
    run_land_use: bool
        whether to run the land use module
    output_folder: pathlib.Path
        output folder file path
    proposed_luc_split_path: pathlib.Path
        path to proposed land use split (output from infill)
    existing_luc_split_path: pathlib.Path
        path to existing land use split (output from infill) 
    dlog_input_file: pathlib.Path
        path to D-log file
    lookups_sheet_name: str
        name of lookup sheet in D-Log
    infill: Optional[InfillConfig]
        infilling config parameters
    land_use: Optional[LandUseConfig]
        land use config parameters

    Raises
    ------
    ValueError
        file doesn't exisit
    """
    # set up
    run_infill: bool
    run_land_use: bool

    # mandatory
    output_folder: pathlib.Path
    proposed_luc_split_path: pathlib.Path
    existing_luc_split_path: pathlib.Path
    dlog_input_file: pathlib.Path
    lookups_sheet_name: str

    # required for infill
    infill: Optional[InfillConfig]

    land_use: Optional[LandUseConfig]

    # required for land use

    @pydantic.validator(
        "dlog_input_file",
    )
    def _file_exists(  # Validator is class method pylint: disable=no-self-argument
        cls, value: pathlib.Path
    ) -> pathlib.Path:
        if not value.is_file():
            raise ValueError(f"file doesn't exist: {value}")
        return value

    def check_inputs(self) -> None:
        if self.run_land_use:
            self.land_use.check_params()
        if self.run_infill:
            self.infill.check_params()


class InfillingAverages(BaseConfig):
    average_res_area: float
    average_emp_area: float
    average_mix_area: float
    average_gfa_site_area_ratio: float
    average_dwelling_site_area_ratio: float
