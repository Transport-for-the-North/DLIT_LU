# D-LIT

Development Log (D-Log) integration toolkit analyses, infills and converts the D-Log
to employment and population land use.

## Infilling
The infilling module loads, analyses and performs data fixes on the D-Log data set.
It outputs analysis results as an Excel workbook with summary maps.

## Land Use 
The land use module converts DLog into MSOA base build-out profiles disaggregated by SIC
code for employment and, census dwelling type and TfN traveller type for residential. 

## Dev Pattern
The Dev pattern module defines and categorizes large-scale developments that have the 
potential to influence existing travel patterns, ensuring they are appropriately 
integrated into demand forecasting.

## Constraint 
The constrain module merges future-year growth from the Development Log (D-Log)
with EDGE growth for rail and NTEM growth for other modes, 
constraining combined LAD or LAD-level growth appropriately.

## Trip End
The Trip End module converts the development-related population and employment
 data into trip ends for use in forecasting demand matrices.


# Environment
Setup the Python environment using your preferred Python package / environment manager tool.
This section will assume you're using [Miniconda](https://docs.conda.io/en/latest/miniconda.html)
(or Anaconda). All Python dependancies are listed in [requirements.txt](requirements.txt).

- To create a conda environment run the following (in the Anaconda prompt):

    `conda create --file requirements.txt -n dlit --channel conda-forge`

- To activate the environment run: `conda activate dlit`

*See the [Conda User Guide](https://docs.conda.io/projects/conda/en/stable/user-guide/index.html)
for more information on using conda.*

# Running
Running DLIT is done from the [run.py](run.py), this script runs both the infilling and land use
modules based on the parameters defined in [Config File](#config-file). The following commands and
arguments are used for running DLIT.

`run.py [-h] [-c CONFIG] [-m MAPS] [-i INITIAL_REPORT]`

## Options

- CONFIG: Config file path
- MAPS (True or False):  Whether the tool should plot maps displaying the results of the data reports
- INITIAL_REPORT (True or False): Whether the tool should output a data report on the inputted DLog

# Config File
The tool uses a configuration file which contains the necessary options, parameters, and file paths
to run each module. The variables are divided into setup, infilling and land use. Setup parameters
are always required and if undefined the tool will raise an error. Infilling and land use are only
necessary if the respective module has been turned on for that run of the tool. The config file is
written in [YAML format](https://yaml.org/), an example with parameters is given here 
[d_lit-config.yml](d_lit-config.yml).

## Setup Parameters
These parameters are all mandatory with no default values.

| Parameter               |          Type           | Description                                             |
| :---------------------- | :---------------------: | :------------------------------------------------------ |
| run_infill              | Boolean (True or False) | Whether to run the infill module                        |
| run_land_use            | Boolean (True or False) | Whether to run the land use module                      |
| dlog_input_file         |        File Path        | Path to the D-Log Excel Worksheet                       |
| lookups_sheet_name      |          Text           | Name of the lookup sheet in the D-Log Excel Worksheet   |
| output_folder           |     Directory Path      | Folder to save outputs to                               |
| proposed_luc_split_path |        File Path        | Path to read/write the proposed land use code split csv |
| existing_luc_split_path |        File Path        | Path to read/write the existing land use code split csv |


## Infilling Module Parameters

| Parameter               |                    Type                    | Description                                                                                                                                      |
| :---------------------- | :----------------------------------------: | :----------------------------------------------------------------------------------------------------------------------------------------------- |
| user_infill             |          Boolean (True or False)           | Whether to allow user infilling                                                                                                                  |
| combined_sheet_name     |                    Text                    | Name of the combined sheet in the D-Log Excel Worksheet                                                                                          |
| residential_sheet_name  |                    Text                    | Name of the residential sheet in the D-Log Excel Worksheet                                                                                       |
| employment_sheet_name   |                    Text                    | Name of the employment sheet in the D-Log Excel Worksheet                                                                                        |
| mixed_sheet_name        |                    Text                    | Name of the mixed sheet in the D-Log Excel Worksheet                                                                                             |
| dlog_column_names_path  |                 File Path                  | Path to the column names csv file, which defines the column names for each sheet in the D-Log and which columns should be ignored in all sheets. |
| user_input_path         |                 File Path                  | Path to read/write user inputs Excel Worksheet.                                                                                                  |
| valid_luc_path          |                 File Path                  | Path to csv file containing all the valid land use codes.                                                                                        |
| out_of_date_luc_path    |                 File Path                  | Path to csv containing all the out-of-date land use codes with a look-up to their successors.                                                    |
| incomplete_luc_path     |                 File Path                  | Path to csv containing all the possible incomplete land use code with a look-up to all the possible valid codes.                                 |
| known_invalid_luc_path  |                 File Path                  | Path to known invalid land use codes, which do not fall into the above categories, with a look-up to the land use code it is referencing         |
| regions_shapefiles_path |                 File Path                  | Path to LPA regions shapefile                                                                                                                    |
| gfa_infill_method       | mean, regression or regression_no_negative | Method to infill GFA and site area using.                                                                                                        |

## Land Use

| Parameter                      |            Type            | Description                                                                                                                                            |
| :----------------------------- | :------------------------: | :----------------------------------------------------------------------------------------------------------------------------------------------------- |
| land_use_input                 |    File Path (Optional)    | Path to Excel Worksheet to use as Land Use input, usually post fix data output from infilling module (required if running land use without infilling). |
| lsoa_shapefile_path            |         File Path          | Path to LSOA shape file.                                                                                                                               |
| lsoa_dwelling_pop_path         |         File Path          | Path to CSV containing TfN’s dwelling population data by MSOA.                                                                                         |
| lsoa_traveller_type_path       |         File Path          | Path to CSV containing TfN’s Traveller Type split by MSOA CSV.                                                                                         |
| lsoa_jobs_path                 |         File Path          | Path to CSV containing TfN's employment data by MSOA.                                                                                                  |
| employment_density_matrix_path |         File Path          | Path to the CSV containing the employment density matrix.                                                                                              |
| luc_sic_conversion_path        |         File Path          | Path to CSV containing land use code to standard industrial code conversion.                                                                           |
| demolition_dampener            | Real 0 - 1.0 (default 1.0) | Factor to apply to GFA when calculating demolitions of existing land use values, 0 indicates no implied demolitions.                                   |


## Land Use

| Parameter                      |            Type            | Description                                                                                                                                            |
| :----------------------------- | :------------------------: | :----------------------------------------------------------------------------------------------------------------------------------------------------- |
| base_year                      |    Text                    | Base Year                                                           |
| geo_boundary                   |    Text                    | Model zone , among the options of lsoa, normits, noham, norms, msoa |
| assessment_input               |    File Path (optional)    | Path to inputs exported from previous infilling module              |
| emp_site_data                  |    File Path (optional)    | Path to inputs exported from previous land use module, 
site level total jobs from D-log              |
| res_site_data                  |    File Path (optional)    | Path to inputs exported from previous land use module, 
site level total dwellings from D-log              |
| lsoa_data_path                 |    File Path (optional)    | Path to CSV containing TfN’s base year total population, 
household and jobs               |
| pop_tt_site_data               |    File Path (optional)    | Path to inputs exported from previous land use module,
site level population segmented by tt             |
| emp_sic_soc_site_data          |    File Path (optional)    | Path to inputs exported from previous land use module,
site level jobs segmented by SIC 2 digit and SOC              |
| normits_shapefile_path         |         File Path          | Path to normits zone shape file                                     |
| noham_shapefile_path           |         File Path          | Path to normits zone shape file                                     |
| norms_shapefile_path           |         File Path          | Path to normits zone shape file                                     |
| msoa_shapefile_path            |         File Path          | Path to normits zone shape file                                     |
| lsoa_hh_centroids              |         File Path          | Path to hh weighted lsoa centroids                                  |
| lsoa_emp_centroids             |         File Path          | Path to emp weighted lsoa centroids                                 |
| lsoa_pop_centroids             |         File Path          | Path to pop weighted lsoa centroids                                 |
| normits_hh_centroids           |         File Path          | Path to hh weighted normits centroids                               |
| normits_emp_centroids          |         File Path          | Path to emp weighted normits centroids                              |
| normits_pop_centroids          |         File Path          | Path to pop weighted normits centroids                              |
| noham_hh_centroids             |         File Path          | Path to hh weighted noham centroids                                 |
| noham_emp_centroids            |         File Path          | Path to emp weighted noham centroids                                |
| noham_pop_centroids            |         File Path          | Path to pop weighted noham centroids                                |
| norms_hh_centroids             |         File Path          | Path to hh weighted norms centroids                                 |
| norms_emp_centroids            |         File Path          | Path to emp weighted norms centroids                                |
| norms_pop_centroids            |         File Path          | Path to pop weighted norms centroids                                |
| msoa_hh_centroids              |         File Path          | Path to hh weighted msoa centroids                                  |
| msoa_emp_centroids             |         File Path          | Path to emp weighted msoa centroids                                 |
| msoa_pop_centroids             |         File Path          | Path to pop weighted msoa centroids                                 |
| lsoa_to_normits                |         File Path          | Path to translation file bewteen lsoa and normits                   |
| lsoa_to_noham                  |         File Path          | Path to translation file bewteen lsoa and noham                     |
| lsoa_to_norms                  |         File Path          | Path to translation file bewteen lsoa and norms                     |
| lsoa_to_msoa                   |         File Path          | Path to translation file bewteen lsoa and msoa                      |

### Summary Data
Optional parameters for creating the land use summaries workbooks and heatmaps. MSOA land use data
will be aggregated to given summary zone system.

| Parameter                   |        Type        | Description                                                              |
| :-------------------------- | :----------------: | :----------------------------------------------------------------------- |
| summary_lad                 |        Text        | Name of LAD version.                                                     |
| summary_region              |        Text        | Name of Region version.                                                  |
| lad_to_region_file          |     File Path      | Path to translation file bewteen lad and region                          |
| lsoa_to_lad_file            |     File Path      | Path to translation file bewteen lsoa and LAD                            |
| msoa_to_lad_file            |     File Path      | Path to translation file bewteen msoa and LAD                            |
| norms_to_lad_file           |     File Path      | Path to translation file bewteen norms and LAD                           |
| noham_to_lad_file           |     File Path      | Path to translation file bewteen noham and LAD                           |
| normits_to_lad_file         |     File Path      | Path to translation file bewteen normits and LAD                         |
| lad_shapefile               |     File Path      | Path to LAD shapefile.                                   |
| shapefile_id_column         |        Text        | Name of ID column in shapefile.                                          |
| geometry_simplify_tolerance | Integer (Optional) | Optional tolerance parameter to simplify the shapefile to.               |
