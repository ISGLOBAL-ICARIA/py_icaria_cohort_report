from datetime import datetime
from datetime import date

import pandas as pd
from pandas import IndexSlice as idx
import tokens

from dateutil.relativedelta import relativedelta
import math
import numpy as np
import pandas
import params

import pydrive

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

gauth = GoogleAuth()
drive = GoogleDrive(gauth)

#gauth.LocalWebserverAuth()

def cohort_stopping_sistem(redcap_project,nletter,projectkey,date_='2023-05'):
    """
    :param redcap_project_df: Data frame containing all data exported from the REDCap project
    :type redcap_project_df: pandas.DataFrame

    :return: List of record ids per letter
    """

    xres = redcap_project.reset_index()
    actual_cohorts = xres[xres['redcap_event_name']=='cohort_after_mrv_2_arm_1'][['record_id','ch_his_date']]
    letters_ = xres[(xres['record_id'].isin(list(actual_cohorts['record_id'].unique())))&(xres['redcap_event_name']=='epipenta1_v0_recru_arm_1')][['record_id','int_random_letter']]
    STOP = False
    if actual_cohorts.empty:
        return STOP
    #print(actual_cohorts)
    actual_cohorts = actual_cohorts.dropna()
    records_dates_=actual_cohorts[actual_cohorts['ch_his_date'].str.contains(date_)]
    if projectkey == 'HF11' and date_=='2023-03':
        records_dates_ = (records_dates_[~records_dates_['record_id'].isin([240,239])])
    cohorts_from_this_months = pd.merge(records_dates_,letters_, on='record_id')


    #print(cohorts_from_this_months.groupby('int_random_letter').count()['record_id'])
    if len(cohorts_from_this_months.groupby('int_random_letter').count())==6 and sum(list(cohorts_from_this_months.groupby('int_random_letter').count()['record_id']>=nletter))==6: #False not in list(cohorts_from_this_months.groupby('int_random_letter').count()['record_id']>=nletter):
        STOP = True
        print ("It has been recruited all minimum participants per letter ("+str(nletter)+") and the alert for this HF needs to stop.")
    elif len(cohorts_from_this_months.groupby('int_random_letter').count())>=2:
        sum_ = 0
        for el in cohorts_from_this_months.groupby('int_random_letter').count()['record_id']:
            if el > nletter:
                el = nletter
            sum_+= el
        nletter_comp = nletter + (nletter*6 - sum_)
        if sum(list(cohorts_from_this_months.groupby('int_random_letter').count()['record_id']>=nletter_comp))>=4:
            print("It has been recruited the minimum participants per letter + compensation (" + str(nletter) + ") in, at least, 4 letters, and the alert for this HF needs to stop.")
            STOP = True
    return STOP



def get_record_ids_range_age(redcap_data,min_age,max_age,date_='2023-05-01'):
    xre = redcap_data.reset_index()
    #end_date = datetime.strptime(date_, "%Y-%m-%d").date()
    end_date = datetime.strptime("2023-0"+str(date.today().month)+"-01", "%Y-%m-%d").date()
    dob_count = 0

    dobs = list(xre[xre['redcap_event_name'] == 'epipenta1_v0_recru_arm_1']['child_dob'])
    dob_df = pd.DataFrame(index=xre.record_id.unique(), columns=['dob_diff'])

    for record_id in xre.record_id.unique():
        start_date = datetime.strptime(dobs[dob_count], "%Y-%m-%d")
        delta = relativedelta(end_date, start_date)

        res_months = delta.months + (delta.years * 12)
        if delta.days != 0:
            res_months+=1
        #print(record_id,start_date,end_date,delta,res_months,delta.months,delta.days)
        dob_df.loc[record_id]['dob_diff']= res_months
        dob_count += 1
    #print(dob_df[(dob_df['dob_diff']<= max_age) & (dob_df['dob_diff'] >= min_age)])
    return dob_df[(dob_df['dob_diff']<= max_age) & (dob_df['dob_diff'] >= min_age)].index




def get_record_ids_nc_cohort(redcap_data, max_age, min_age, nletter,projectkey):

    ## HAVING RECEIVED AT LEAST 4 DOSES OF SP

    ## 1 CRITERIA: Having received at least 4 doses of SP
    x = redcap_data
    xres = x.reset_index()

    sp_doses = xres[xres['int_sp'] == float(1)].groupby('record_id')['int_sp'].count()
    record_id_only_4_doses = xres[xres['int_sp'] == float(1)].groupby('record_id').count()[sp_doses == 4].index
    record_id_4_doses = xres[xres['int_sp'] == float(1)].groupby('record_id').count()[sp_doses > 4].index

    """ AIXÒ S'HA DE MIRAR BÉ, LO DE L'SP < 14 DIES. PERQUÈ LES XIFRES VAN VARIANT MOLT"""
    ## Calculating if last SP dose was administered  more than 14 days before
    #print(record_id_only_4_doses)

    last_SP = xres[(xres['int_sp']==1)&(xres['record_id'].isin(record_id_only_4_doses))].groupby('record_id')['int_date'].last().reset_index()

    more_14days = []
    for k,el in last_SP.T.items():
        days_from_SP = datetime.today() - datetime.strptime(el['int_date'],"%Y-%m-%d %H:%M:%S")
        if days_from_SP.days>=14:
            more_14days.append(True)
        else:
            more_14days.append(False)
    try:
        record_id_only_4_doses = last_SP[more_14days]['record_id']
    except:
        record_id_only_4_doses = []

    #print(record_id_only_4_doses)
    record_id_4_doses = list(record_id_4_doses)
    for el in list(record_id_only_4_doses):
        if el not in record_id_4_doses:
            record_id_4_doses.append(el)
#    print(record_id_4_doses)

    ## RECORDS THAT MEET THE MAX-MIN AGE RANGE CRITERIA
    records_range_age = get_record_ids_range_age(redcap_data, min_age, max_age)
    cohorts_to_be_contacted = list(set(record_id_4_doses).intersection(list(records_range_age)))
    # Find those participants deaths or migrated that can't be part of the list
    try:
        deaths = xres[(xres['redcap_event_name'] == 'end_of_fu_arm_1') & (~xres['death_reported_date'].isnull())][
            'record_id'].unique()
    except:
        deaths = []
    try:
        migrated = xres[(xres['redcap_event_name'] == 'out_of_schedule_arm_1') & (~xres['mig_date'].isnull())][
            'record_id'].unique()
    except:
        migrated = []
    # Find those participants already recruited in the COHORTS substudy that can't be part of the list
    already_cohorts = xres[(xres['redcap_event_name'] == 'cohort_after_mrv_2_arm_1') & (~xres['ch_his_date'].isnull())][
        'record_id'].unique()

    # 6 CRITERIA: Participant is not completed
    completed_participants = xres[(xres['redcap_event_name']=='hhat_18th_month_of_arm_1')&(~xres['hh_date'].isnull())]['record_id'].unique()
    #print(completed_participants)
    letters_to_be_contacted = xres[(xres['record_id'].isin(cohorts_to_be_contacted)) &
                                   (~xres['record_id'].isin(list(deaths))) &
                                   (~xres['record_id'].isin(list(migrated))) &
                                   (~xres['record_id'].isin(list(completed_participants))) &
                                   (xres['redcap_event_name'] == 'epipenta1_v0_recru_arm_1')][
        ['record_id','study_number', 'int_random_letter']]
    all = letters_to_be_contacted[['record_id','study_number','int_random_letter']].reset_index(drop=True)
    all['Recruited'] = letters_to_be_contacted['record_id'].isin(list(already_cohorts)).values
    summary = letters_to_be_contacted.groupby('int_random_letter').count().rename(columns={'record_id':'eligible'})[['eligible']]
    letters_yet_to_be_contacted = letters_to_be_contacted[~letters_to_be_contacted['record_id'].isin(list(already_cohorts))].rename(columns={'record_id':'pending'}).groupby('int_random_letter')['pending']
    #print(letters_yet_to_be_contacted.groups)
    already_cohorts_letters = xres[(xres['redcap_event_name']=='epipenta1_v0_recru_arm_1')&(xres['record_id'].isin(list(already_cohorts)))&(~xres['int_random_letter'].isnull())][['record_id','study_number','study_number','int_random_letter']].drop_duplicates().rename(columns={'record_id':'recruited'}).groupby('int_random_letter')['recruited']
    #print(already_cohorts_letters.groups)


    summary=summary.join(already_cohorts_letters.count()).join(letters_yet_to_be_contacted.count())
    stop=cohort_stopping_sistem(redcap_data,nletter=nletter,projectkey=projectkey)
    #print(stop)
    if stop == True:
        summary['pending'] = 0
        #summary.loc['Rule'] = ["The recruitment for this HF-month needs to STOP.","","",""]
    if stop==False:
        pass
    return all,summary,stop


def excel_creation(project_key,redcap_project, redcap_project_df, excelwriter, summarywriter):

    records_to_flag = []
    current_month = datetime.now().month
    cohort_list_df = pd.read_excel(params.COHORT_RECRUITMENT_PATH,str(current_month))
    big_project_key = project_key.split(".")[0]
    if big_project_key.split(".")[0] in cohort_list_df['HF'].unique():
        min_age = cohort_list_df[cohort_list_df['HF']==big_project_key]['min_age'].unique()[0]
        max_age = cohort_list_df[cohort_list_df['HF']==big_project_key]['max_age'].unique()[0]
        nletter = cohort_list_df[cohort_list_df['HF']==big_project_key]['target_letter'].unique()[0]
        #        print(current_month,project_key,min_age,max_age,nletter)

        all_to_FW, summary,stop = get_record_ids_nc_cohort(redcap_project_df, max_age=max_age, min_age=min_age,
                                                           nletter=nletter,projectkey=project_key)


        # CREATION OF THE WORKERS EXCEL
        tobe_recruited = all_to_FW[all_to_FW['Recruited']==False]
        if stop==True:
            summ = summary.reset_index()
            summ=summ.rename(columns={'eligible':'No pending'})
            summ[summ['int_random_letter']=='Rule'][['No pending']].to_excel(excelwriter,project_key,index=False)
        else:
            letter_dict = tobe_recruited.set_index('study_number').groupby('int_random_letter').groups
            biggest_size = tobe_recruited.groupby('int_random_letter')['record_id'].count().max()
            new_dict = {}
            for k,el in letter_dict.items():
                new_dict[k]=list(el)
                for i in range(biggest_size-len(el)):
                    new_dict[k].append("")
            dict_to_excel = pd.DataFrame(data=new_dict)
            dict_to_excel.to_excel(excelwriter,project_key,index=False)
        print("COHORT recruitment writen.")
        summary_sheet = summary_excel_creation(project_key,redcap_project_df,summarywriter,all_to_FW,summary,stop)
        return summary_sheet
def summary_excel_creation(project_key,redcap_project_df,summarywriter,all_to_FW,summary,stop):
    summary_sheet = pd.DataFrame(index=[project_key],columns=['HF','A-elegible','A-recruited','B-elegible','B-recruited',
        'C-elegible','C-recruited','D-elegible','D-recruited','E-elegible','E-recruited','F-elegible','F-recruited','stop'])
    list_to_summary = []
    summary[np.isnan(summary)] = 0
    list_to_summary.append(project_key)
    for k,el in summary.T.items():
        list_to_summary.append(int(el.eligible))
        list_to_summary.append(int(el.recruited))
    list_to_summary.append(stop)
    try:
        summary_sheet.loc[project_key] = list_to_summary
    except:
        return None
    letter_dict = all_to_FW[all_to_FW['Recruited']==True].set_index('study_number').groupby('int_random_letter').groups
    biggest_size = all_to_FW[all_to_FW['Recruited']==True].groupby('int_random_letter')['record_id'].count().max()
    new_dict = {}
    for letter in ['A','B','C','D','E','F']:
        if letter not in letter_dict:
            letter_dict[letter] = []
    for k, el in letter_dict.items():
        new_dict[k] = list(el)
        try:
            for i in range(biggest_size - len(el)):
                new_dict[k].append("")
        except:
            pass
        dict_to_excel = pd.DataFrame(data=new_dict)
        dict_to_excel = dict_to_excel.reindex(sorted(dict_to_excel.columns), axis=1)
        dict_to_excel.to_excel(summarywriter, project_key, index=False)

    return summary_sheet

def file_to_drive(file):
    gfile = drive.CreateFile({'title':str(date.today()) +'_pending_cohort_recruitment.xlsx' ,'parents': [{'id': tokens.cohorts_drive_folder}]})
    # Read file and set it as the content of this instance.
    gfile.SetContentFile(file)
    gfile.Upload()  # Upload the file.

