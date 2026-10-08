"""Databricks job: merge SMARD chunks from the bronze volume into silver."""

import sys

from spotgrid_lakehouse.transforms.smard import main

main(sys.argv[1:])
