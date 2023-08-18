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
import redcap

import pydrive

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe

gauth = GoogleAuth()
drive = GoogleDrive(gauth)


#gauth.LocalWebserverAuth()


def GET_cohorts_from_this_month(redcap_project,projectkey, date_, min_age, max_age,additional=False):
    if additional:
        print("\t\tADDITIONAL: Searching for all cohort participants in the month {} and the main project/subprojects {} and all projects/subprojects on the {}".format(date_,additional[0],projectkey.split(".")[0]))
        subprojects_to_check = [additional[0]]
        for el in additional[1]:
            subprojects_to_check.append(el)
        cohorts_from_this_month = pd.DataFrame()
        for el in subprojects_to_check:
            print(el)
            project = redcap.Project(params.URL, params.TRIAL_PROJECTS[el])
            df = project.export_records(format='df', fields=params.ALERT_LOGIC_FIELDS)
            xres = df.reset_index()
            actual_cohorts = xres[xres['redcap_event_name'] == 'cohort_after_mrv_2_arm_1'][['record_id', 'ch_his_date']]
            letters_ = xres[(xres['record_id'].isin(list(actual_cohorts['record_id'].unique()))) & (
                    xres['redcap_event_name'] == 'epipenta1_v0_recru_arm_1')][['record_id', 'int_random_letter']]
            STOP = False
            actual_cohorts = actual_cohorts.dropna()
            if actual_cohorts.empty:
                pass
            else:
                records_dates_ = actual_cohorts[actual_cohorts['ch_his_date'].str.contains(date_)]
                cohorts_from_this_month_subproj = pd.merge(records_dates_, letters_, on='record_id')

                ## RECORDS THAT MEET THE MAX-MIN AGE RANGE CRITERIA
                records_range_age = get_record_ids_range_age(el,df, min_age, max_age)
                cohorts_from_this_month_subproj = cohorts_from_this_month_subproj[
                    cohorts_from_this_month_subproj['record_id'].isin(records_range_age)]


                if cohorts_from_this_month.empty:
                    cohorts_from_this_month = cohorts_from_this_month_subproj
                else:
                    cohorts_from_this_month = pd.concat([cohorts_from_this_month, cohorts_from_this_month_subproj])

        if cohorts_from_this_month.empty:
            return STOP
        return cohorts_from_this_month
    else:
        print("\t\tSearching for all cohort participants in the month {} and all subprojects on the {}".format(date_,projectkey.split(".")[0]))

        hf13 = 0
        if str(projectkey) == 'HF13' and date_ == '2023-06':
            list_subprojects = ['HF13', 'HF16.01', 'HF16.02', 'HF16.03']
            hf13 = 1

        if "." in str(projectkey):
            list_subprojects = params.subprojects[str(projectkey).split(".")[0]]

        if "." in str(projectkey) or hf13 == 1:
            cohorts_from_this_month = pd.DataFrame()
            for el in list_subprojects:
                project = redcap.Project(params.URL, params.TRIAL_PROJECTS[el])
                df = project.export_records(format='df', fields=params.ALERT_LOGIC_FIELDS)
                xres = df.reset_index()
                actual_cohorts = xres[xres['redcap_event_name'] == 'cohort_after_mrv_2_arm_1'][['record_id', 'ch_his_date']]
                letters_ = xres[(xres['record_id'].isin(list(actual_cohorts['record_id'].unique()))) & (
                            xres['redcap_event_name'] == 'epipenta1_v0_recru_arm_1')][['record_id', 'int_random_letter']]
                STOP = False
                actual_cohorts = actual_cohorts.dropna()
                if actual_cohorts.empty:
                    pass
                else:
                    records_dates_ = actual_cohorts[actual_cohorts['ch_his_date'].str.contains(date_)]
                    cohorts_from_this_month_subproj = pd.merge(records_dates_, letters_, on='record_id')

                    ## RECORDS THAT MEET THE MAX-MIN AGE RANGE CRITERIA
                    records_range_age = get_record_ids_range_age(el,df, min_age, max_age)
                    cohorts_from_this_month_subproj = cohorts_from_this_month_subproj[
                        cohorts_from_this_month_subproj['record_id'].isin(records_range_age)]


                    if cohorts_from_this_month.empty:
                        cohorts_from_this_month = cohorts_from_this_month_subproj
                    else:
                        cohorts_from_this_month = pd.concat([cohorts_from_this_month, cohorts_from_this_month_subproj])

            if cohorts_from_this_month.empty:
                return STOP
        else:
            xres = redcap_project.reset_index()
            actual_cohorts = xres[xres['redcap_event_name'] == 'cohort_after_mrv_2_arm_1'][['record_id', 'ch_his_date']]
            #print(actual_cohorts)
            letters_ = xres[(xres['record_id'].isin(list(actual_cohorts['record_id'].unique()))) & (
                        xres['redcap_event_name'] == 'epipenta1_v0_recru_arm_1')][['record_id', 'int_random_letter']]
            STOP = False
            if actual_cohorts.empty:
                return STOP
            # print(actual_cohorts)
            actual_cohorts = actual_cohorts.dropna()
            records_dates_ = actual_cohorts[actual_cohorts['ch_his_date'].str.contains(date_)]
            if projectkey == 'HF11' and date_ == '2023-03':
                records_dates_ = (records_dates_[~records_dates_['record_id'].isin([240, 239])])
            cohorts_from_this_month = pd.merge(records_dates_, letters_, on='record_id')
            #print(cohorts_from_this_month)
            ## RECORDS THAT MEET THE MAX-MIN AGE RANGE CRITERIA
            records_range_age = get_record_ids_range_age(projectkey,redcap_project, min_age, max_age)
            #print(min_age,max_age)
            #print(records_range_age)
            cohorts_from_this_month = cohorts_from_this_month[
                cohorts_from_this_month['record_id'].isin(records_range_age)]

        return cohorts_from_this_month


def cohort_stopping_sistem(redcap_project,nletter,projectkey,date_="-".join(str(date.today()).split("-")[:-1]),additional=False,max_age=False,min_age=False):
    """
    :param redcap_project_df: Data frame containing all data exported from the REDCap project
    :type redcap_project_df: pandas.DataFrame

    :return: List of record ids per letter
    """
    print("\tStopping System activated . . .")
    STOP = False
    cohorts_from_this_month = GET_cohorts_from_this_month(redcap_project,projectkey,date_,min_age,max_age,additional=additional)
    #print(cohorts_from_this_month)
    try:
        if cohorts_from_this_month==False:
            STOP = cohorts_from_this_month
            return STOP
    except:
        pass
    #print(cohorts_from_this_month.groupby('int_random_letter').count()['record_id'])
    if len(cohorts_from_this_month.groupby('int_random_letter').count())==6 and sum(list(cohorts_from_this_month.groupby('int_random_letter').count()['record_id']>=nletter))==6: #False not in list(cohorts_from_this_month.groupby('int_random_letter').count()['record_id']>=nletter):
        STOP = True
        print ("\t\tIt has been recruited all minimum participants per letter ("+str(nletter)+") and the alert for this HF needs to stop.")
    elif len(cohorts_from_this_month.groupby('int_random_letter').count())>=2:
        sum_ = 0
        for el in cohorts_from_this_month.groupby('int_random_letter').count()['record_id']:
            if el > nletter:
                el = nletter
            sum_+= el
        nletter_comp = nletter + (nletter*6 - sum_)
        if sum(list(cohorts_from_this_month.groupby('int_random_letter').count()['record_id']>=nletter_comp))>=4:
            print("\t\tIt has been recruited the minimum participants per letter + compensation (" + str(nletter) + ") in, at least, 4 letters, and the alert for this HF needs to stop.")
            STOP = True
    return STOP


def get_record_ids_range_age(project_name,redcap_data,min_age,max_age,date_='2023-05-01'):
    xre = redcap_data.reset_index()
    #end_date = datetime.strptime(date_, "%Y-%m-%d").date()
    end_date = datetime.strptime("2023-0"+str(date.today().month)+"-01", "%Y-%m-%d").date()
    dob_count = 0
    dobs = list(xre[xre['redcap_event_name'] == 'epipenta1_v0_recru_arm_1']['child_dob'])
    dobs = xre[xre['redcap_event_name'] == 'epipenta1_v0_recru_arm_1'][['record_id','child_dob']]
    dob_df = pd.DataFrame(index=xre.record_id.unique(), columns=['dob_diff'])
    for record_id in xre.record_id.unique():
        #print(dobs[dobs['record_id'] == 11010002]['child_dob'])
        try:
            start_date = datetime.strptime(list(dobs[dobs['record_id'] == record_id]['child_dob'])[0], "%Y-%m-%d")
            delta = relativedelta(end_date, start_date)

            res_months = delta.months + (delta.years * 12)

            if delta.days != 0:
                res_months+=1
            #print(record_id,start_date,end_date,delta,res_months,delta.months,delta.days)
            dob_df.loc[record_id]['dob_diff']= res_months
           # print(record_id,start_date)

        except:
            try:
                print("\t\t\tWARN:{} - {}: No dob: {}".format(project_name,record_id, dobs[dob_count]))
            except:
                print("\t\t\tWARN:{} - {}: No dob".format(project_name,record_id))

        dob_count += 1

    return dob_df[(dob_df['dob_diff']<= max_age) & (dob_df['dob_diff'] >= min_age)].index




def get_record_ids_nc_cohort(project_key,redcap_data, max_age, min_age, nletter,projectkey,max_age2=False, min_age2=False, additional=False):

    ## HAVING RECEIVED AT LEAST 4 DOSES OF SP

    ## 1 CRITERIA: Having received at least 4 doses of SP
    x = redcap_data
    xres = x.reset_index()
    print("\tGetting records from {} with age range [{}-{}] and 4th SP doses at least 15days from 4th dosis".format(projectkey,min_age,max_age))
    sp_doses = xres[xres['int_sp'] == float(1)].groupby('record_id')['int_sp'].count()
    record_id_only_4_doses = xres[xres['int_sp'] == float(1)].groupby('record_id').count()[sp_doses == 4].index
    record_id_4_doses = xres[xres['int_sp'] == float(1)].groupby('record_id').count()[sp_doses > 4].index

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

    ## RECORDS THAT MEET THE MAX-MIN AGE RANGE CRITERIA
    records_range_age1 = get_record_ids_range_age(project_key,redcap_data, min_age, max_age)

    records_range_age2 = get_record_ids_range_age(project_key,redcap_data, min_age2, max_age2)
    print("\tGetting records from {} with age range [{}-{}] and 4th SP doses at least 15days from 4th dosis".format(projectkey,min_age2,max_age2))

    records_range_age = list(records_range_age1)
    for el in list(records_range_age2):
        if el not in records_range_age:
            records_range_age.append(el)
   # print(list(records_range_age))
    cohorts_to_be_contacted = list(set(records_range_age).intersection(list(record_id_4_doses)))
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
    letters_to_be_contacted = xres[(xres['record_id'].isin(cohorts_to_be_contacted)) &
                                   (~xres['record_id'].isin(list(deaths))) &
                                   (~xres['record_id'].isin(list(migrated))) &
                                   (~xres['record_id'].isin(list(completed_participants))) &
                                   (xres['redcap_event_name'] == 'epipenta1_v0_recru_arm_1')][
        ['record_id','study_number', 'int_random_letter']]
    all = letters_to_be_contacted[['record_id','study_number','int_random_letter']].reset_index(drop=True)

    all['Recruited'] = letters_to_be_contacted['record_id'].isin(list(already_cohorts)).values
    all['firsttrue'] = all['record_id'].isin(records_range_age1)
    all = all.sort_values('firsttrue',ascending=False)
    summary = letters_to_be_contacted.groupby('int_random_letter').count().rename(columns={'record_id':'eligible'})[['eligible']]
    letters_yet_to_be_contacted = letters_to_be_contacted[~letters_to_be_contacted['record_id'].isin(list(already_cohorts))].rename(columns={'record_id':'pending'}).groupby('int_random_letter')['pending']
    already_cohorts_letters = xres[(xres['redcap_event_name']=='epipenta1_v0_recru_arm_1')&(xres['record_id'].isin(list(already_cohorts)))&(~xres['int_random_letter'].isnull())][['record_id','study_number','study_number','int_random_letter']].drop_duplicates().rename(columns={'record_id':'recruited'}).groupby('int_random_letter')['recruited']


    summary=summary.join(already_cohorts_letters.count()).join(letters_yet_to_be_contacted.count())
    stop=cohort_stopping_sistem(redcap_data,nletter=nletter,projectkey=projectkey,additional=additional,max_age=max_age2,min_age=min_age2)
    #print(stop)
    #print(summary)
    if stop == True:
        summary['pending'] = 0
        #summary.loc['Rule'] = ["The recruitment for this HF-month needs to STOP.","","",""]
    if stop==False:
        pass
    return all,summary,stop


def excel_creation(project_key,redcap_project, redcap_project_df, excelwriter,additional=False):

    records_to_flag = []
    current_month = datetime.now().month
    cohort_list_df = pd.read_excel(params.COHORT_RECRUITMENT_PATH,str(current_month))
    big_project_key = project_key.split(".")[0]
    if big_project_key.split(".")[0] in cohort_list_df['HF'].unique():
        min_age = cohort_list_df[cohort_list_df['HF']==big_project_key]['min_age'].unique()[0]
        max_age = cohort_list_df[cohort_list_df['HF']==big_project_key]['max_age'].unique()[0]
        nletter = cohort_list_df[cohort_list_df['HF']==big_project_key]['target_letter'].unique()[0]
    #   print(cohort_list_df)
        try:
            min_age2 = cohort_list_df[cohort_list_df['HF'] == big_project_key]['min_age2'].unique()[0]
            max_age2 = cohort_list_df[cohort_list_df['HF'] == big_project_key]['max_age2'].unique()[0]
        except:
            print("H")
            pass
        if additional:
            min_age = additional[2]
            max_age = additional[3]
        #        print(current_month,project_key,min_age,max_age,nletter)
            all_to_FW, summary,stop = get_record_ids_nc_cohort(project_key,redcap_project_df, max_age=max_age, min_age=min_age,
                                                           nletter=nletter,projectkey=project_key,additional=additional[:2])
        else:
            all_to_FW, summary, stop = get_record_ids_nc_cohort(project_key,redcap_project_df, max_age=max_age, min_age=min_age,
                                                                nletter=nletter, projectkey=project_key, min_age2=min_age2,
                                                                max_age2=max_age2,
                                                                additional=False)
        # CREATION OF THE WORKERSGET_cohorts_from_this_month EXCEL
#        print(all_to_FW)
        tobe_recruited = all_to_FW[all_to_FW['Recruited']==False]
#        print(tobe_recruited)

        if stop==True:
            summ = summary.reset_index()
            summ=summ.rename(columns={'eligible':'No pending'})
            summ[summ['int_random_letter']=='Rule'][['No pending']].to_excel(excelwriter,project_key,index=False)
            print("\tNO COHORT RECRUITMENT LEFT FOR THE MONTH {} ON {}\n".format(current_month,big_project_key))
            return None
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

        if additional:
            print(project_key)
            file_to_drive_summary(project_key,dict_to_excel,tokens.drive_folder_additional,tokens.drive_file_additional,index_included=False)
            print("Additional COHORT candidates added to the Google Sheet {} for sheet {}.".format(tokens.drive_file_additional,project_key))

        print("\tCOHORT recruitment sheet writen for {}\n".format(big_project_key))
        return dict_to_excel


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
        #print(summarywriter)
    return summary_sheet

def file_to_drive(file):
    gfile = drive.CreateFile({'title':str(date.today()) +'_pending_cohort_recruitment.xlsx' ,'parents': [{'id': tokens.cohorts_drive_folder}]})
    # Read file and set it as the content of this instance.
    gfile.SetContentFile(file)
    gfile.Upload()  # Upload the file.


def file_to_drive_summary(worksheet,df,drive_folder=tokens.drive_folder, title=tokens.drive_file_name,index_included=True):
    gc = gspread.oauth(tokens.path_credentials)
    sh = gc.open(title=title,folder_id=drive_folder)
    set_with_dataframe(sh.worksheet(worksheet), df,include_index=index_included)

def export_records_summary(project,project_key,fields_,filter_logic,final_df, month,min_age,max_age):
    try:
        df = project.export_records(format='df', fields=params.ALERT_LOGIC_FIELDS)
        df_cohorts = project.export_records(format='df', fields=fields_,filter_logic=filter_logic)
        df_cohorts = df_cohorts[df_cohorts['ch_his_date'].str.split("-", expand=True)[1]==month]
        records_range_age = get_record_ids_range_age(project_key,df, min_age, max_age)
        df_cohorts_xres = df_cohorts.reset_index()
        df_cohorts_xres = df_cohorts_xres[df_cohorts_xres['record_id'].isin(list(records_range_age))]
        df_cohorts = df_cohorts_xres.set_index(['record_id','redcap_event_name'])

        letters = get_letter_df(project, project_key, df_cohorts)
        final_df = pd.concat([final_df, letters.T])
    except:
        noletters =pd.DataFrame(columns=['A','B','C','D','E','F'],index=[project_key])
        noletters.loc[project_key] = [0,0,0,0,0,0]
        final_df= pd.concat([final_df,noletters])
    return final_df

def get_letter_df(project, project_key,df_):
    record_ids = df_.index.get_level_values('record_id')
    if record_ids.empty:
        noletters =pd.DataFrame(columns=['A','B','C','D','E','F'],index=[project_key])
        noletters.loc[project_key] = [0,0,0,0,0,0]
        return noletters.T
    else:
        df_letters = project.export_records(
            format='df',
            records=list(record_ids.drop_duplicates()),
            fields=["study_number", "int_random_letter"],
            filter_logic="[study_number] != ''"
        )
        records_letter = df_letters.groupby('int_random_letter')[['study_number']].count()
        records_letter = records_letter.rename(columns={'study_number': project_key.split(".")[0]})
        return records_letter


def cohort_summary_expected(month):
    expected = pd.read_excel(params.COHORT_RECRUITMENT_PATH,sheet_name=str(int(month)))
    expected = expected.set_index('HF')
    expected_index = [el+"_expected" for el in expected.index]
    final_expected = pd.DataFrame(index=expected_index, columns=['A','B','C','D','E','F','finished'])
    for k,el in expected.T.items():
        nletter = el['target_letter']
        index = k + "_expected"
        final_expected.loc[index] = [nletter,nletter,nletter,nletter,nletter,nletter,'']
    return final_expected,list(expected.index),expected['target_letter']


def groups_preparation(group,expected,finished_list):
    group = group.reset_index()
    group['index'] = group['index'].str.split(".").str[0]
    group = group.groupby('index').sum().astype(int)
    group['finished'] = finished_list.values()
    group = pd.concat([group,expected]).sort_index()[['A','B','C','D','E','F','finished']]
    return group



def additional_recruitments_from_another_hf(projects, mainproject,new_min_age,new_max_age):
    print("ADDITIONAL RECRUITMENT IN {} . . .".format(mainproject))
    print("\tNew range of age has been established based on root project {}:\n\tNew_min_age:{}\n\tNew_max_age:{}\n\tProjects added:{}\n".format(mainproject,new_min_age,new_max_age,projects))

    additional_recruitmnets_path = tokens.EXCEL_PATH+"_"+mainproject+"_additional.xlsx"
    # SPECIAL CASE FOR HF16 RANGE 17-17 AND MONTH JUNE
    writer = pd.ExcelWriter(additional_recruitmnets_path)
    summary_sheet = None
    for project_key in projects:
        project = redcap.Project(params.URL, params.TRIAL_PROJECTS[project_key])
        # Get all records for each ICARIA REDCap project (TRIAL)
        print("\tGetting records from (additional) {}...".format(project_key))
        df = project.export_records(format='df', fields=params.ALERT_LOGIC_FIELDS)
        xres = df.reset_index()
        cohort_df = df[~df['ch_his_date'].isnull()].reset_index()[['record_id', 'ch_his_date']]
        cohort_ids = df[~df['ch_his_date'].isnull()].reset_index()['record_id'].unique()
        cohort_letters = \
            xres[(xres['record_id'].isin(cohort_ids)) & (~xres['int_random_letter'].isnull())].groupby('record_id')[
                'int_random_letter'].max()

        candidates_df = excel_creation(project_key=project_key, redcap_project=project,
                                              redcap_project_df=df, excelwriter=writer, additional=[mainproject,projects,new_min_age,new_max_age])


    print("ADDITIONAL RECRUITMENTS ENDS. Candidates to "+ mainproject +" from "+", ".join(projects)+ " has been added to "+additional_recruitmnets_path+"\n")
    writer.close()
    return candidates_df