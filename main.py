#!/usr/bin/env python

from datetime import datetime
import pandas as pd
import redcap
import params
import cohorts
import tokens

__author__ = "Andreu Bofill Pumaorla"
__copyright__ = "Copyright 2023, ISGlobal Maternal, Child and Reproductive Health"
__credits__ = ["Andreu Bofill Pumarola"]
__license__ = "MIT"
__version__ = "0.0.1"
__date__ = "20230227"
__maintainer__ = "Andreu Bofill"
__email__ = "andreu.bofill@isglobal.org"
__status__ = "Dev"

if __name__ == '__main__':
    # Read the COHORT PARTICIPATNS AND CREATE GOOGLE SHEETS
    writer = pd.ExcelWriter(params.EXCEL_PATH)
    writer_summary = pd.ExcelWriter(params.SUMMARY_PATH)
    summary_sheet = None
    for project_key in params.TRIAL_PROJECTS:
        cohort_list_df = pd.read_excel(params.COHORT_RECRUITMENT_PATH, str(datetime.now().month))
        if project_key.split(".")[0] in cohort_list_df['HF'].unique():
            project = redcap.Project(params.URL, params.TRIAL_PROJECTS[project_key])
            # Get all records for each ICARIA REDCap project (TRIAL)
            print("[{}] Getting records from the ICARIA TRIAL REDCap projects:".format(datetime.now()))
            print("[{}] Getting all records from {}...".format(datetime.now(), project_key))
            df = project.export_records(format='df', fields=params.ALERT_LOGIC_FIELDS)
            xres = df.reset_index()
            cohort_df = df[~df['ch_his_date'].isnull()].reset_index()[['record_id', 'ch_his_date']]
            cohort_ids = df[~df['ch_his_date'].isnull()].reset_index()['record_id'].unique()
            cohort_letters = xres[(xres['record_id'].isin(cohort_ids)) & (~xres['int_random_letter'].isnull())].groupby('record_id')['int_random_letter'].max()

            summary_sheet_part = cohorts.excel_creation(project_key=project_key, redcap_project=project,
                                                        redcap_project_df=df, excelwriter=writer,
                                                        summarywriter=writer_summary)
            if summary_sheet_part is not None:
                if summary_sheet is None:
                    summary_sheet = summary_sheet_part
                else:
                    summary_sheet = pd.concat([summary_sheet, summary_sheet_part])

#    summary_sheet.to_excel(writer_summary, 'SUMMARY', index=False)
    writer_summary.close()
    writer.close()
    cohorts.file_to_drive(params.EXCEL_PATH)

    """
    df = 0
    for g in tokens.group_tokens:
        for project_key in g:
            if project_key.split(".")[0] in cohort_list_df['HF'].unique():
                project = redcap.Project(params.URL, g[project_key])
                # Get all records for each ICARIA REDCap project (TRIAL)
                print("[{}] Getting records from the ICARIA TRIAL REDCap projects:".format(datetime.now()))
                print("[{}] Getting all records from {}...".format(datetime.now(), project_key))
                subdf = project.export_records(format='df', fields=params.ALERT_LOGIC_FIELDS)
                print(subdf)
                xsubdf = subdf.reset_index().reset_index()
                subdf = xsubdf.set_index(['index', 'redcap_event_name'])
                print(subdf)

                if type(df)==int:
                    df = subdf.copy()
                else:
                    df = pd.concat([df,subdf])

        xres = df.reset_index()
#        df = xres.set_index(['index','redcap_event_name'])
        cohort_df = df[~df['ch_his_date'].isnull()].reset_index()[['record_id', 'ch_his_date']]
        cohort_ids = df[~df['ch_his_date'].isnull()].reset_index()['record_id'].unique()
        cohort_letters = \
        xres[(xres['record_id'].isin(cohort_ids)) & (~xres['int_random_letter'].isnull())].groupby('record_id')[
            'int_random_letter'].max()


        summary_sheet_part = cohorts.excel_creation(project_key=project_key.split(".")[0], redcap_project=project,
                                                    redcap_project_df=df, excelwriter=writer,
                                                    summarywriter=writer_summary)
        if summary_sheet_part is not None:
            if summary_sheet is None:
                summary_sheet = summary_sheet_part
            else:
                summary_sheet = pd.concat([summary_sheet, summary_sheet_part])

    """
