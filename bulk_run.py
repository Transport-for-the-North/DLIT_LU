import argparse
import os
import pathlib
import sys
from distutils.dir_util import copy_tree
import logging

if "src" not in sys.path:
    sys.path.append("src")


from dlit_lu import main, inputs

configs = pathlib.Path('config')
LOG = logging.getLogger(__package__)
LOG_FILE = "DLIT.log"
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

parser.add_argument(
    "-c", "--config", help="Config file path", default="d_lit-config_infill.yml", type=str
)
args = parser.parse_args()

if __name__ == "__main__":
    for meth in ['no_neg', 'neg']:
        args.config = f'inputs/configs/infill_{meth}.yml'
        main.run(args)
        for dem in [1, 0]:
            for den in ['min','max']:
                args.config = f'inputs/configs/LU_{dem}_{den}.yml'
                main.run(args)
                config = inputs.DLitConfig.load_yaml(args.config)
                copy_tree(config.output_folder, rf"E:\dlit\bulk_outputs\{meth}\{den}_{dem}")
                