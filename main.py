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
    print("COHORT PENDING RECRUITMENTS SCRIPT.\n")
    writer = pd.ExcelWriter(params.EXCEL_PATH)
    writer_summary = pd.ExcelWriter(params.SUMMARY_PATH)
    summary_sheet = None
    for project_key in params.TRIAL_PROJECTS:

        cohort_list_df = pd.read_excel(params.COHORT_RECRUITMENT_PATH, str(datetime.now().month))

        if project_key == "HF13" and str(datetime.now().month) == "6":
            new_min_age = cohort_list_df[cohort_list_df['HF']=='HF13']['min_age'].values[0]
            new_max_age = cohort_list_df[cohort_list_df['HF']=='HF13']['max_age'].values[0]
            additional_list = cohorts.additional_recruitments_from_another_hf(['HF16.01', 'HF16.02', 'HF16.03'], 'HF13',new_min_age,new_max_age)

        cohort_list_df = pd.read_excel(params.COHORT_RECRUITMENT_PATH, str(datetime.now().month))
        if project_key.split(".")[0] in cohort_list_df['HF'].unique():
            project = redcap.Project(params.URL, params.TRIAL_PROJECTS[project_key])
            # Get all records for each ICARIA REDCap project (TRIAL)
            print("[{}] Getting records from {}...".format(datetime.now(), project_key))
            df = project.export_records(format='df', fields=params.ALERT_LOGIC_FIELDS)
            xres = df.reset_index()
            cohort_df = df[~df['ch_his_date'].isnull()].reset_index()[['record_id', 'ch_his_date']]
            cohort_ids = df[~df['ch_his_date'].isnull()].reset_index()['record_id'].unique()
            cohort_letters = xres[(xres['record_id'].isin(cohort_ids)) & (~xres['int_random_letter'].isnull())].groupby('record_id')['int_random_letter'].max()

            summary_sheet_part = cohorts.excel_creation(project_key=project_key, redcap_project=project,
                                                        redcap_project_df=df, excelwriter=writer)
            if summary_sheet_part is not None:
                if summary_sheet is None:
                    summary_sheet = summary_sheet_part
                else:
                    summary_sheet = pd.concat([summary_sheet, summary_sheet_part])

#    summary_sheet.to_excel(writer_summary, 'SUMMARY', index=False)
#    writer_summary.close()
    writer.close()
    cohorts.file_to_drive(params.EXCEL_PATH)

    print("\n\nCOHORT SUMMARY SCRIPT\n")
    for month in ['10']:
        print("Getting actual cohorts for month {}".format(month))
        stop_dict = {}
        group1_df = pd.DataFrame(columns=['A', 'B', 'C', 'D', 'E', 'F'])
        month_expected,hf_per_month,nletter_list = cohorts.cohort_summary_expected(month)
        for project_key in params.TRIAL_PROJECTS:
            if project_key.split(".")[0] in hf_per_month:
                print("[{}] Getting COHORT records from {} for month {}...".format(datetime.now(), project_key,month))
                project = redcap.Project(tokens.URL, tokens.REDCAP_PROJECTS_ICARIA[project_key])

                stop_df = project.export_records(format='df', fields=params.ALERT_LOGIC_FIELDS)

                current_month = datetime.now().month
                cohort_list_df = pd.read_excel(params.COHORT_RECRUITMENT_PATH, str(current_month))
                big_project_key = project_key.split(".")[0]
                #print(big_project_key,cohort_list_df['HF'].unique())
                if big_project_key.split(".")[0] in cohort_list_df['HF'].unique():
                    min_age = cohort_list_df[cohort_list_df['HF'] == big_project_key]['min_age'].unique()[0]
                    max_age = cohort_list_df[cohort_list_df['HF'] == big_project_key]['max_age'].unique()[0]
                    min_age2 = cohort_list_df[cohort_list_df['HF'] == big_project_key]['min_age2'].unique()[0]
                    max_age2 = cohort_list_df[cohort_list_df['HF'] == big_project_key]['max_age2'].unique()[0]
                    nletter = cohort_list_df[cohort_list_df['HF'] == big_project_key]['target_letter'].unique()[0]

                if project_key == "HF13" and month=='06':
                    stop = cohorts.cohort_stopping_sistem(stop_df, nletter_list[project_key.split(".")[0]], project_key,date_="2023-" + month, min_age=min_age, max_age=max_age,additional=['HF13',['HF16.01', 'HF16.02', 'HF16.03']])
                else:
                    stop = cohorts.cohort_stopping_sistem(stop_df,nletter_list[project_key.split(".")[0]],project_key,date_="2023-"+month,min_age=min_age2,max_age=max_age2)
                if project_key.split(".")[0] not in stop_dict:
                    stop_dict[project_key.split(".")[0]] = stop
                else:
                    pass
                group1_df = cohorts.export_records_summary(project,project_key,['study_number','ch_his_date','ch_rdt_date'],"[ch_his_date]!=''",group1_df,month,min_age2,max_age2).fillna(0)
        print ("Groups Preparation . . . ")
        group =cohorts.groups_preparation(group1_df,month_expected,stop_dict)
        print(group)
        print ("Writing the drive sheet . . .")
        cohorts.file_to_drive_summary(month,group)
    print("FINISHED .")