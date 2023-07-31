"""handles reading config file
"""
# standard imports
from __future__ import annotations

import enum
import pathlib
from typing import Any, Optional
from dataclasses import dataclass

# third party imports
import pydantic
from pydantic import dataclasses
import caf.toolkit
import pandas as pd

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
    dlog_column_names_path: pydantic.FilePath
    user_input_path: pathlib.Path
    valid_luc_path: pydantic.FilePath
    out_of_date_luc_path: pydantic.FilePath
    incomplete_luc_path: pydantic.FilePath
    known_invalid_luc_path: pydantic.FilePath
    regions_shapefiles_path: pydantic.FilePath
    gfa_infill_method: GFAInfillMethod


@dataclasses.dataclass
class SummaryInputs:
    """Lookup file and shapefile for creating output summaries."""

    summary_zone_name: str
    lookup_file: pydantic.FilePath
    shapefile: pydantic.FilePath
    shapefile_id_column: str
    geometry_simplify_tolerance: int | None = None


@dataclasses.dataclass
class LandUseConfig:
    """Manages reading / writing the tool's config file.

    Parameters
    ----------
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
    land_use_input: pathlib.Path, optional
        path to land use input (output of infill),
        not required if running infilling module.
    demolition_dampener: float, default 1.0
        Factor to apply when calculating number of demolitions,
        0 would mean no demolitions and 1 would mean maximum
        demolitions.
    summary_data: SummaryData, optional
        Lookup file and shapefile for creating output summaries
        at a different zone system.
    """

    msoa_shapefile_path: pydantic.FilePath
    msoa_dwelling_pop_path: pydantic.FilePath
    msoa_traveller_type_path: pydantic.FilePath
    msoa_jobs_path: pydantic.FilePath
    employment_density_matrix_path: pydantic.FilePath
    luc_sic_conversion_path: pydantic.FilePath

    land_use_input: Optional[pydantic.FilePath] = None
    demolition_dampener: pydantic.types.confloat(ge=0, le=1, allow_inf_nan=False) = 1
    summary_data: SummaryInputs | None = None


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
    infill: InfillConfig, optional
        infilling config parameters, required for running infilling.
    land_use: LandUseConfig, optional
        land use config parameters, required for land use processing.

    Raises
    ------
    ValidationError
        If any required parameters aren't given or are invalid.
    """

    run_infill: bool
    run_land_use: bool

    output_folder: pathlib.Path
    proposed_luc_split_path: pathlib.Path
    existing_luc_split_path: pathlib.Path
    dlog_input_file: pydantic.FilePath
    lookups_sheet_name: str

    infill: Optional[InfillConfig] = None
    land_use: Optional[LandUseConfig] = None

    @pydantic.validator("infill")
    def check_running_infill(  # pylint: disable=no-self-argument
        cls, value: InfillConfig | None, values: dict[str, Any]
    ) -> dict[str, Any]:
        """Check infill parameters are given if running module."""
        if not values["run_infill"] and value is None:
            raise ValueError("infill is required if run_infill is true")

        return value

    @pydantic.validator("land_use")
    def land_use_input_check(  # pylint: disable=no-self-argument
        cls, value: LandUseConfig | None, values: dict[str, Any]
    ) -> LandUseConfig:
        """Check land use is given if running module."""
        if not values["run_land_use"]:
            # Don't need to check if we aren't running land use module
            return value

        if value is None:
            raise ValueError("land_use required if run_land_use is true")

        if not values["run_infill"] and value.land_use_input is None:
            # Need land use input path if not running infill module
            raise ValueError("land_use_input value required if not running infilling")

        return value

    @pydantic.root_validator
    def check_running(  # pylint: disable=no-self-argument
        cls, values: dict[str, Any]
    ) -> dict[str, Any]:
        """Raise error if neither module has running set to True."""
        if not values["run_infill"] and not values["run_land_use"]:
            raise ValueError(
                "run_infill and run_land_use cannot both be "
                "false because there is nothing to run"
            )

        return values
@dataclass
class JobPopInputs():
    jobs_input: pd.DataFrame
    population_input: pd.DataFrame
    output_folder: pd.DataFrame
    proposed_luc_split: pd.DataFrame
    existing_luc_split: pd.DataFrame
    msoa_dwelling_pop: pd.DataFrame
    msoa_traveller_type: pd.DataFrame
    msoa_to_lad_conversion: pd.DataFrame
    msoa_jobs: pd.DataFrame
    employment_density_matrix: pd.DataFrame
    luc_sic_conversion: pd.DataFrame

class JobPopConfig(caf.toolkit.BaseConfig):
    jobs_input_path: pathlib.Path
    population_input_path: pathlib.Path
    output_folder: pathlib.Path
    proposed_luc_split_path: pathlib.Path
    existing_luc_split_path: pathlib.Path
    msoa_dwelling_pop_path: pathlib.Path
    msoa_traveller_type_path: pathlib.Path
    msoa_to_lad_conversion_path: pathlib.Path
    msoa_jobs_path: pathlib.Path
    employment_density_matrix_path: pathlib.Path
    luc_sic_conversion_path: pathlib.Path

    def parse(self)->JobPopInputs:
        #read in
        jobs_input = pd.read_csv(self.jobs_input_path, index_col=0)
        pop_input = pd.read_csv(self.population_input_path, index_col=0)
        proposed_luc_split = pd.read_csv(self.proposed_luc_split_path)
        existing_luc_split = pd.read_csv(self.existing_luc_split_path)
        msoa_dwelling_pop = pd.read_csv(self.msoa_dwelling_pop_path)
        msoa_traveller_type = pd.read_csv(self.msoa_traveller_type_path)
        msoa_to_lad_conversion = pd.read_csv(self.msoa_to_lad_conversion_path)
        msoa_jobs = pd.read_csv(self.msoa_jobs_path)
        employment_density_matrix = pd.read_csv(self.employment_density_matrix_path)
        
        luc_sic_conversion = pd.read_csv(self.luc_sic_conversion_path).loc[:, ["land_use_code", "sic_code"]]
        luc_sic_conversion["land_use_code"] = luc_sic_conversion["land_use_code"].str.lower()

        #TODO add validation

        return JobPopInputs(
            jobs_input,
            pop_input,
            self.output_folder,
            proposed_luc_split,
            existing_luc_split,
            msoa_dwelling_pop,
            msoa_traveller_type,
            msoa_to_lad_conversion,
            msoa_jobs,
            employment_density_matrix,
            luc_sic_conversion,
            )
class InfillingAverages(caf.toolkit.BaseConfig):
    """Averages calculated for use in MEAN infill method."""

    average_res_area: float
    average_emp_area: float
    average_mix_area: float
    average_gfa_site_area_ratio: float
    average_dwelling_site_area_ratio: float
