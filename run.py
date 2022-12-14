"""
runs the DLIT_LU tool
"""
import sys

if "src" not in sys.path:
    sys.path.append("src")

from dlit_lu import main

if __name__ == "__main__":    
    main.run()