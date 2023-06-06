import tokens
from datetime import datetime
from datetime import date
currentMonth = datetime.now().month
# REDCap parameters
URL = tokens.URL
TRIAL_PROJECTS = tokens.REDCAP_PROJECTS_ICARIA
CHOICE_SEP = " | "
CODE_SEP = ", "
REDCAP_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
TRIAL_CHILD_FU_STATUS_EVENT = "epipenta1_v0_recru_arm_1"
COHORT_CHILD_FU_STATUS_EVENT = "ipti_1__10_weeks_r_arm_1"

# Alerts general parameters
ALERT_DATE_FORMAT = "%b %d"

COHORT_MRV2_ALERT = "MRV2"
COHORT_MRV2_ALERT_STRING = "MRV2 Pending: {birthday}"

FINALIZED_COHORT_STRING = "COH."
NON_CONT_COHORT_ALERT = "(COHORT pending)"
NON_CONT_COHORT_ALERT_STRING = NON_CONT_COHORT_ALERT

COHORT_RECRUITMENT_PATH = tokens.COHORT_RECRUITMENT_PATH
EXCEL_PATH = tokens.EXCEL_PATH + str(date.today()) + ".xlsx"
SUMMARY_PATH = tokens.SUMMARY_PATH + str(currentMonth) + ".xlsx"

# DATA DICTIONARY FIELDS USED BY THE DIFFERENT ALERTS - IMPROVE PERFORMANCE OF API CALLS

subprojects = {'HF01':['HF01.01','HF01.02'],'HF02':['HF02.01','HF02.02'],'HF08':['HF08.01','HF08.02','HF08.03'],
               'HF12':['HF12.01','HF12.02'],'HF16':['HF16.01','HF16.02','HF16.03']}
ALERT_LOGIC_FIELDS = ['record_id','study_number','ch_his_date','int_random_letter','int_sp','int_date',
                      'child_dob','death_reported_date','mig_date','hh_date']