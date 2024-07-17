# standard imports
import logging
import dataclasses
# third party
import pandas as pd

LOG = logging.getLogger(__name__)
REGION_COLUMN = "Region"

def constrain_to_forecast(
    data: pd.DataFrame,
    constraint: pd.DataFrame,
    zone_to_constraint_region_lookup: pd.DataFrame,
    data_unit_col: str,
    data_zone_col: str,
    constraint_unit_col: str,
    region_column: str = REGION_COLUMN,
) -> pd.DataFrame:
    
    if data_unit_col not in data.columns:
        raise ValueError("unit columns are not in data")
    if constraint_unit_col not in constraint.columns:
        raise ValueError("columns are not in forecast")
    index_cols = data.index.names
    data.reset_index(inplace=True)

    data_region = data.merge(zone_to_constraint_region_lookup, on=data_zone_col)
    data_region_sums = (
        data_region.loc[:, [data_unit_col, region_column]].groupby(region_column).sum()
    ).rename(columns={"2050": "forecast_value"})
    try:
        constraint_region = constraint.merge(zone_to_constraint_region_lookup, on=data_zone_col)
    except KeyError:
        constraint.reset_index(inplace = True)
        constraint.rename(columns = {"Name":data_zone_col}, inplace=True)
        constraint_region = constraint.merge(zone_to_constraint_region_lookup, on=data_zone_col, how="left")

    constraint_region_sums = (
        constraint_region.loc[:, [constraint_unit_col, region_column]]
        .groupby(region_column)
        .sum()
    ).rename(columns= {constraint_unit_col: "constraint_value"})

    #set up params
    data_constraint = data_region.merge(constraint_region_sums, how= "left", on=REGION_COLUMN)
    data_constraint = data_constraint.merge(data_region_sums, how="left", on= REGION_COLUMN)

    #calculate constraint factor]

    data_constraint["constraint_factor"] = data_constraint["constraint_value"]/data_constraint["forecast_value"]

    #if undefined, then the zone is outside of constraint area
    data_constraint["constraint_factor"].fillna(1, inplace=True)

    #apply constraint

    data_constraint[data_unit_col]=data_constraint[data_unit_col]*data_constraint["constraint_factor"]

    data_constraint.set_index(index_cols, inplace = True)

    return data_constraint


def data_above_forecast(
    data: pd.DataFrame, forecast: float, max_displacement: pd.DataFrame, value_col: str
) -> pd.Series:
    
    #calculate factor for each MSOA 
    #write factors out so they can be stored

    
    
    
    total_displacement = data[value_col].sum() - forecast
    disaggregate_displacement = (data[value_col]*total_displacement)/data[value_col].sum()
    disaggregate_displacement.loc[disaggregate_displacement>data[value_col]*max_displacement] = data[value_col]*max_displacement
    displaced_data = data[value_col] - disaggregate_displacement
    if displaced_data.sum() > forecast:
        factor = forecast/displaced_data.sum()
        displaced_data = displaced_data*factor
    return displaced_data

def data_below_forecast(
    data: pd.Series, forecast: float
) -> pd.Series:
        factor = forecast/data.sum()
        displaced_data = data*factor
        return displaced_data
