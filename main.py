import sys
import os


sys.path.insert(0, os.path.dirname(__file__))

from src.tui import CSVQLApp



def main():
    csv_dir = sys.argv[1] if len(sys.argv) > 1 else "."
    