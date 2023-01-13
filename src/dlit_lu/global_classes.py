"""contains NamedTuple subclasses and other classes used globally
"""#docstring
#standard imports
from typing import NamedTuple, Optional
#third party imports
import pandas as pd
import geopandas as gpd

class Lookup(NamedTuple):
    """contains all the lookup tables from the DLog lookup sheet

    all tables are stored with the ID (int) as index and value as str
    except for existing_landuse (pd.Series) which is not strictly a
    lookup table but a list of possible vales

    Parameters
    ----------
    site_type: pd.DataFrame
        site stype ID look up table
    construction_status: pd.DataFrame
        construction status ID look up table
    planning_status: pd.DataFrame
        planning status ID look up tables
    webtag: pd.DataFrame
        webtag status ID look up table
    development_type: pd.DataFrame
        development type ID lookup table
    years: pd.DataFrame
        years ID look up table
    distribution_profile: pd.DataFrame
        distribution profile look up table
    land_use_codes: pd.Series
        a list of allowed land use codes, though to incomplete for the
        2021 set, so a complete list is read in in auxiliary data  
    adoption_status: pd.DataFrame
        adoption status look up table
    local_authority: pd.DataFrame
        local authority id look up table
    """
    site_type: pd.DataFrame
    construction_status: pd.DataFrame
    planning_status: pd.DataFrame
    webtag: pd.DataFrame
    development_type: pd.DataFrame
    years: pd.DataFrame
    distribution_profile: pd.DataFrame
    land_use_codes: pd.Series
    adoption_status: pd.DataFrame
    local_authority: pd.DataFrame


class DLogData(NamedTuple):
    """used to store and pass the read DLOg data set

    note that the DataFrames do not have identical column names
    Parameters
    ----------
    combined_data: Optional[pd.DataFrame]
        the combined data set
    residential_data: pd.DataFrame
        the residential data set
    employment_data: pd.DataFrame
        the employment data set
    mixed_data: Optional[pd.DataFrame]
        sites which have residential and employment developments
    lookup: Lookup
        a look up table for the IDs used in the data
    """    
    combined_data: Optional[pd.DataFrame]
    residential_data: pd.DataFrame
    employment_data: pd.DataFrame
    mixed_data: pd.DataFrame
    lookup: Lookup

class AuxiliaryData(NamedTuple):
    """stores data not contained in the DLog required for processing

    contains allowed and not allowed land use codes and lpa region 
    shape data

    Parameters
    ----------
    allowed_codes:pd.DataFrame

    out_of_date_luc: pd.DataFrame
        out of date landuse codes lookup
    known_invalid_lucs:pd.DataFrame
        known issues landuse code lookup
    incomplete_luc: pd.DataFrame
        incomplete land use codes lookup
    regions: gpd.GeoDataFrame
        regions shape file
    """    
    allowed_codes:pd.DataFrame
    known_invalid_luc: pd.DataFrame
    out_of_date_luc: pd.DataFrame
    incomplete_luc: pd.DataFrame
    regions: gpd.GeoDataFrame

class ResultsReport(NamedTuple):
    """stores the results report

    Parameters
    ----------
    data_filter: dict[str, pd.DataFrame]
        data with the filter column added
    analysis_summary: list[dict[str, int]]
        the number of invalid values in each filter column 
    analysis_summary_index_labels: list[str]
        labels each of the indexs in the summary
    analysis_summary_notes: list[str]
        notes for each of the filter column displayed in the summary sheet
    filter_columns:list[str]
        list of the filter column names
    """    
    data_filter: dict[str, pd.DataFrame]
    analysis_summary: list[dict[str, int]]
    analysis_summary_index_labels: list[str]
    analysis_summary_notes: list[str]
    filter_columns:list[str]