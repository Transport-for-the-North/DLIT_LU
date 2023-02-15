"""
runs the DLIT_LU tool
"""
import sys
import argparse

if "src" not in sys.path:
    sys.path.append("src")

from dlit_lu import main

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument("-c", "--config", help="Config file path", default="d_lit-config.yml", type = str)
parser.add_argument("-m", "--maps", help="Whether the tool should plot maps displaying"
    " the results of the data reports", type = bool, default=False)
parser.add_argument("-i", "--initial_report", help="Whether the tool should output a data report"
    " on the inputted DLog", type = bool, default=True)
args = parser.parse_args()

if __name__ == "__main__":
    main.run(args)
