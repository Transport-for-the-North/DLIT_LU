import argparse
import os
import pathlib
from dlit_lu import main, inputs, parser, infilling, land_use

configs = pathlib.Path('config')


parser = argparse.ArgumentParser(description="No description")

parser.add_argument(
    "-m",
    "--maps",
    help="Whether the tool should plot maps displaying"
    " the results of the data reports",
    type=bool,
    default=False,
)
parser.add_argument(
    "-i",
    "--initial_report",
    help="Whether the tool should output a data report" " on the inputted DLog",
    type=bool,
    default=True,
)

parser = argparse.ArgumentParser(description="Process some integers.")
parser.add_argument(
    "-c", "--config", help="Config file path", default="d_lit-config.yml", type=str
)

args = parser.parse_args()

config = inputs.DLitConfig.load_yaml(args.config)


for meth in ['regression', 'regression_no_negatives']:
    config.output_folder = config.output_folder / f"{meth}"
    config.output_folder.mkdir(exist_ok=True)
    config.infill.gfa_infill_method = meth
    infilled_data = infilling.run(config, args)
    for dem in [0, 1]:
        directory = os.getcwd() / 'inputs' / 'emp_densities'
        for file in os.listdir(directory):
            config.land_use.employment_density_matrix = file
            land_use_data = land_use.run(config, args)


