# D-LIT
loads, analyses and performs data fixes to the DLog data set.
outputs analysis results as an excel spread sheet

Update the file paths in the config file and install the enivronment before use (see below)

# Config file
d_lit-config.yml contains the file paths for the axiliary data (see below), the DLog spread sheet and output folder. It also contains the sheet names for the DLOg spread sheet and the column names in list format for each of the sheets (2021 DLog had duplicate column names).
# Axilliary Data
land use codes spreadsheet: an excel spreadsheet with 3 required sheets:
`
    allowed_codes: contains the current land use codes (column name = "land_use_codes")

    out_of_date_codes: contains the out of date codes (column name = "out_of_date_land_use_codes") and the replacement codes (column name = "replacement_land_use_codes")

    incomplete_codes: contains a list of possible incomplete land use codes (column name = "incomplete_land_use_codes")

regions shape file:
    a shape file (.shp) that contains polygons of the regions that the dlog data can be spatially devided into
    assumed to have columns, OBJECTID , LPA19CD, LPA19NM, "BNG_E""BNG_N","LONG","LAT",  "Shape__Are""Shape__Len","geometry", OBJECTID (the unique id for each polygon) and geometery are necessary column. should the columns names be different, the geo_plotter, geo_explorer, spatial_analysis, spatial_invalid_ratio will need updating, with the new column names.

examples of each of these input have been provided in DLIT_inputs, but it the user's responsibility to check these are up to date and fit for use.
# Install Environment
to use the tool, create a new enviroment: (assumes anaconda or miniconda is installed)

    conda env create -f environment.yml

to activate:

    conda activate dlit


