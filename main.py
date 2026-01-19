#!/usr/bin/env python

from datetime import datetime
import cohorts

"""
This project runs several functions that allowed to get a list of potential non-
contemporary cohort ICARIA participants that meet age criteria. A minimum number of
participants per letter is needed to be compared with non-ICARIA cohort. This script
is crucial to determine, daily, how many participants need to be recruited per month
per HF per letter to get this minimum number of participants per month and age criteria.

Parallely, it generates a summary tool to follow this ICARIA cohort recruitment.

This script is intended to be run daily.

20231001 Completed: As ICARIA COHORT recruitment is already finished, this script
is saved as completed and no other modifications are required. 
"""

__author__ = "Andreu Bofill"
__copyright__ = "Copyright 2023, ISGlobal Maternal, Child and Reproductive Health"
__credits__ = ["Andreu Bofill"]
__license__ = "MIT"
__version__ = "0.0.1"
__date__ = "20230227"
__maintainer__ = "Andreu Bofill"
__email__ = "andreu.bofill@isglobal.org"
__status__ = "Finished"

if __name__ == '__main__':
    print("COHORT PENDING RECRUITMENTS SCRIPT.\n")
    cohorts.pending_recruitment()

    print("\n\nCOHORT SUMMARY SCRIPT\n")
    cohorts.cohort_summary_script()

    print('\nFINISHED.\t',datetime.today())
