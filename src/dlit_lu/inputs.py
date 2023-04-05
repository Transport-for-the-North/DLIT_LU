"""handles reading config file
"""
# standard imports
from __future__ import annotations

import enum
import pathlib
from typing import Optional
import os

# third party imports
import pydantic
from pydantic import dataclasses
import caf.toolkit

AVERAGE_INFILLING_VALUES_FILE = "infilling_average_values.yml"


class GFAInfillMethod(enum.Enum):
    """Method for infilling the GFA from the site area."""

    MEAN = "mean"
    REGRESSION = "regression"
    REGRESSION_NO_NEGATIVES = "regression_no_negatives"

    @classmethod
    def regression_methods(cls) -> list[GFAInfillMethod]:
        """List of methods which use HistGradientBoostingRegressor."""
        return [cls.REGRESSION, cls.REGRESSION_NO_NEGATIVES]


@dataclasses.dataclass
class InfillConfig:
    """Manages reading / writing the tool's config file.


    Parameters
    ----------
    user_infill: bool
        whether to run user infilling functionality
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
    gfa_infill_method : GFAInfillMethod
        Method to use when infilling the site area and GFA columns.
    """

    user_infill: bool
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
    gfa_infill_method: GFAInfillMethod

    def check_params(self) -> None:
        """performs checks as to whether values exist

        if path is a read file will also check if path is valid

        Raises
        ------
        ValueError
            if parameters are incomplete
        ValueError
            if read paths are invalid
        """
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
                raise ValueError(
                    "Infill Parameters incomplete, please"
                    " complete these within the config file before continuing. Cheers!"
                )

        for param in read_path_params:
            if not os.path.exists(param):
                raise ValueError(
                    "Infill parameters contains write file paths"
                    " that do not exist. Please update the config file before continuing. Cheers!"
                )


@dataclasses.dataclass
class SummaryInputs:
    """Lookup file and shapefile for creating output summaries."""

    summary_zone_name: str
    lookup_file: pydantic.FilePath
    shapefile: pydantic.FilePath
    shapefile_id_column: str


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
    msoa_jobs_path: pathlib.Path
        path to msoa split of jobs
    employment_density_matrix_path: pathlib.Path
        path to employment density matrix
    luc_sic_conversion_path: pathlib.Path
        path to land use code to SIC code conversion matrix
    demolition_dampener: float, default 1.0
        Factor to apply when calculating number of demolitions,
        0 would mean no demolitions and 1 would mean maximum
        demolitions.
    summary_data: SummaryData, optional
        Lookup file and shapefile for creating output summaries
        at a different zone system.
    """

    land_use_input: Optional[pathlib.Path]
    msoa_shapefile_path: pathlib.Path
    msoa_dwelling_pop_path: pathlib.Path
    msoa_traveller_type_path: pathlib.Path
    msoa_jobs_path: pathlib.Path
    employment_density_matrix_path: pathlib.Path
    luc_sic_conversion_path: pathlib.Path
    demolition_dampener: pydantic.types.confloat(ge=0, le=1, allow_inf_nan=False) = 1
    summary_data: SummaryInputs | None = None

    def check_params(self) -> None:
        """performs checks as to whether values exist

        if path is a read file will also check if path is valid

        Raises
        ------
        ValueError
            if parameters are incomplete
        ValueError
            if read paths are invalid
        """

        read_path_params = [
            self.msoa_shapefile_path,
            self.msoa_dwelling_pop_path,
            self.msoa_traveller_type_path,
            self.msoa_jobs_path,
            self.employment_density_matrix_path,
            self.luc_sic_conversion_path,
        ]
        write_path_params = []
        for param in read_path_params + write_path_params:
            if param is None:
                raise ValueError(
                    "Land use parameters incomplete, please"
                    " complete these within the config file before continuing. Cheers!"
                )
        for param in read_path_params:
            if not os.path.exists(param) or param == pathlib.Path("."):
                raise ValueError(
                    "Land use parameters contains write file paths"
                    " that do not exist. Please update the config file before continuing. Cheers!"
                )


class DLitConfig(caf.toolkit.BaseConfig):
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

        if (not self.run_infill) and self.run_land_use:
            if self.land_use.land_use_input is None:
                raise ValueError(
                    "Land use input path is required when not running infilling module"
                )
            if not os.path.exists(
                self.land_use.land_use_input
            ) or self.land_use.land_use_input == pathlib.Path("."):
                raise ValueError("Land use input path is not valid")


class InfillingAverages(caf.toolkit.BaseConfig):
    average_res_area: float
    average_emp_area: float
    average_mix_area: float
    average_gfa_site_area_ratio: float
    average_dwelling_site_area_ratio: float
