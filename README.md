# D-LIT
Infilling
loads, analyses and performs data fixes to the DLog data set.
outputs analysis results as an excel spread sheet

Land Use 
converts DLog into MSOA base build-out profiles disaggregated by SIC code for employment and, census dwelling type and TfN traveller type for residential. 

Update the and preferences file paths in the config file and install the enivronment before use (see below)

# Environment
install miniconda or anaconda on your machine before performing these commands
to create an environment from the requirements.txt file run the command

conda create --file requirements.txt -n dlit --channel conda-forge

to activate the environment
    conda activate dlit

to deactivate the environment
    conda deactivate

# config file
The tool uses a configuration file which contains the necessary options, parameters, and file paths to run each module. The variables are divided into setup, mandatory, infilling and land use. Setup and mandatory are always required and if undefined the tool will raise an error. Infilling and land use are only necessary if the respective module has been turned on for that run of the tool. The default config file will be used if no config file path is defined as an argument when calling the tool.

SETUP PARAMETERS
run_infill: Whether to run the infill module (True or False)
run_land_use: Whether to run the land use module (True or False)


MANDATORY PARAMETERS
lookups_sheet_name: Name of the lookup sheet in the D-Log Excel Worksheet
dlog_input_file: file path of the D-Log Excel Worksheet
output_folder: File path to output directory (tool will create directory if it doesn’t exist
proposed_luc_split_path: file path to read/write the proposed land use code split csv
existing_luc_split_path: file path to read/write the existing land use code split csv

INFILLING MODULE PARAMETERS
user_infill: Whether to allow user infilling (True or False)
combined_sheet_name: Name of the combined sheet in the D-Log Excel Worksheet
residential_sheet_name: Name of the residential sheet in the D-Log Excel Worksheet
employment_sheet_name: Name of the employment sheet in the D-Log Excel Worksheet
mixed_sheet_name: Name of the mixed sheet in the D-Log Excel Worksheet
dlog_column_names_path: File path to the column names csv file, which defines the column names for each sheet in the D-Log and which columns should be ignored in all sheets.
valid_luc_path: File path to csv file containing all the valid land use codes.
out_of_date_luc_path: File path to csv containing all the out-of-date land use codes with a look-up to their successors.
incomplete_luc_path: File path to csv containing all the possible incomplete land use code with a look-up to all the possible valid codes.
known_invalid_luc_path: file path to known invalid land use codes, which do not fall into the above categories, with a look-up to the land use code it is referencing
regions_shapefiles_path: file path to LPA regions shapefile
user_input_path: file path to read/write user inputs Excel Worksheet.
LAND USE 
land_use_input: file path to Excel Worksheet to use as Land Use input, usually post fix data output from Infilling module (only required if running land use without infilling)
msoa_shapefile_path:  File path to MSOA shape file
msoa_dwelling_pop_path: File path to csv containing TfN’s dwelling population data by MSOA 
msoa_traveller_type_path: File path to csv containing TfN’s Traveller Type split by MSOA csv
msoa_jobs_path: File path to csv containing TfNs employment data by MSOA
employment_density_matrix_path: File path to the csv containing the employment density matrix
luc_sic_conversion_path: File path to csv containing land use code to standard industrial code conversion 
