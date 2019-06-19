### CONST: THEY ARE USED IN STEP 2
CONST = {
        'K_YES': 'YES',
        'K_NO': 'NO',
        'N_ROWS': 'N_ROWS'
}

### PARAM: Parameters to tune so as to sharp the number of ouput records
### DOB is the date of birth of the given patient. Patients who are older than 89 years old 
# at any time in the database have had their date of birth shifted to obscure 
# their age and comply with HIPAA
### When Setting READ_ALL_RECORDS=YES, then LIMIT_NUM_PATIENT has no effect
# - LIMIT_NUM_CHARTEVENTS: there are 330 million records in this CHARTEVENTS, so it is good
# to limit to 10 million records for less time consuming
PARAM = {
        # 'K_YES': 'YES',
        # 'K_NO': 'NO',
        'READ_ALL_RECORDS': 'NO',
        'LIMIT_NUM_PATIENT': 20,
        #'LIMIT_NUM_CHARTEVENTS': 10000000
        'LIMIT_NUM_CHARTEVENTS': 1000000
}

CONFIG = {
        'CONST': CONST,
        'PARAM': PARAM,
}

criteria = {}

criteria[CONFIG['CONST']['N_ROWS']] = 10

if CONFIG['CONST']['N_ROWS'] in criteria:
        print(criteria[CONFIG['CONST']['N_ROWS']])


import pandas as pd

filename = '/Volumes/SSD200/Users/Shared/Mimic/Data/Output/Step2/OUT_LIMIT_NUM_EVENTS_WINSIZE_24H.csv'
#df_tbl = pd.read_csv('healthData/MIMIC/2019/OUT_LIMIT_NUM_EVENTS_WINSIZE_24H.csv'

tbl_cols =  pd.read_csv(filename, index_col=0, nrows=0).columns.tolist()

df_tbl = pd.read_csv(filename, usecols=tbl_cols)

# Yields a tuple of index label and series for each row in the datafra,e
for (index_label, row_series) in df_tbl.iterrows():
    dob = df_tbl.iloc[index_label, 'DOB']
    # Calculate the length of stay by number of days
    len_of_stay = end_chartime - start_charttime
    len_of_stay = math.ceil((end_chartime - start_charttime)/pd.offsets.Second(1))
   

# Yields a tuple of index label and series for each row in the datafra,e
for (index_label, row_series) in df_tbl.iterrows():
    phrase = df_tbl.iloc[index_label,28]
    if isinstance(phrase, str):
        from string import maketrans 
        chars_to_remove = " /``[]"
        replace_by = '-_    '
        trantab = maketrans(chars_to_remove, replace_by)
        phrase =  phrase.translate(trantab)

        df_tbl.iloc[index_label,28] = phrase

        print('Row Index label : ', index_label)
        print('Row Content as Series : ', row_series.values)
        print(phrase)
        print(df_tbl.iloc[index_label,28])

df_tbl.to_csv('healthData/MIMIC/2019/res.csv')

# cols = df_tbl.columns
# cols_ind = [df_tbl.columns.get_loc(c) for c in cols if c in df_tbl]
# print(zip(cols, cols_ind))

# # dateparse = lambda x: pd.to_datetime(str(x), format='%Y-%m-%d %H:%M:%S', errors='coerce')


#  [('Unnamed: 0', 0), ('ROW_ID', 1), ('SUBJECT_ID', 2), ('GENDER', 3), 
# ('DOB', 4), ('DOD', 5), ('DOD_HOSP', 6), ('DOD_SSN', 7), ('EXPIRE_FLAG', 8), 
# ('DOB_YEAR', 9), ('DOB_MON', 10), ('DOB_DAY', 11), ('DOB_HOUR', 12), ('DOB_MIN', 13), 
# ('DOB_SEC', 14), ('DOD_YEAR', 15), ('DOD_MON', 16), ('DOD_DAY', 17), ('DOD_HOUR', 18), 
# ('DOD_MIN', 19), ('DOD_SEC', 20), ('HADM_ROW_ID', 21), ('HADM_SUBJECT_ID', 22), 
# ('HADM_HADM_ID', 23), ('HADM_ADMITTIME', 24), ('HADM_DISCHTIME', 25), ('HADM_DEATHTIME', 26), 
# ('HADM_ADMISSION_TYPE', 27), ('HADM_ADMISSION_LOCATION', 28), ('HADM_DISCHARGE_LOCATION', 29), 
# ('HADM_INSURANCE', 30), ('HADM_LANGUAGE', 31), ('HADM_RELIGION', 32), ('HADM_MARITAL_STATUS', 33), 
# ('HADM_ETHNICITY', 34), ('HADM_EDREGTIME', 35), ('HADM_EDOUTTIME', 36), ('HADM_HOSPITAL_EXPIRE_FLAG', 37), 
# ('HADM_HAS_CHARTEVENTS_DATA', 38), ('HADM_ADM_YEAR', 39), ('HADM_ADM_MON', 40), ('HADM_ADM_DAY', 41), 
# ('HADM_ADM_HOUR', 42), ('HADM_ADM_MIN', 43), ('HADM_ADM_SEC', 44), ('HADM_DISCH_YEAR', 45), 
# ('HADM_DISCH_MON', 46), ('HADM_DISCH_DAY', 47), ('HADM_DISCH_HOUR', 48), ('HADM_DISCH_MIN', 49), 
# ('HADM_DISCH_SEC', 50), ('HADM_DEATHYEAR', 51), ('HADM_DEATHMON', 52), ('HADM_DEATHDAY', 53), ('HADM_DEATHHOUR', 54), 
# ('HADM_DEATHMIN', 55), ('HADM_DEATHSEC', 56), ('ICU_ROW_ID', 57), ('ICU_SUBJECT_ID', 58), ('ICU_HADM_ID', 59), 
# ('ICU_ICUSTAY_ID', 60), ('ICU_DBSOURCE', 61), ('ICU_FIRST_CAREUNIT', 62), ('ICU_LAST_CAREUNIT', 63), ('ICU_FIRST_WARDID', 64), 
# ('ICU_LAST_WARDID', 65), ('ICU_INTIME', 66), ('ICU_OUTTIME', 67), ('ICU_LOS', 68), ('ICU_INTTIME_YEAR', 69), 
# ('ICU_INTTIME_MON', 70), ('ICU_INTTIME_DAY', 71), ('ICU_INTTIME_HOUR', 72), ('ICU_INTTIME_MIN', 73), ('ICU_INTTIME_SEC', 74), 
# ('ICU_OUTTIME_YEAR', 75), ('ICU_OUTTIME_MON', 76), ('ICU_OUTTIME_DAY', 77), ('ICU_OUTTIME_HOUR', 78), ('ICU_OUTTIME_MIN', 79), 
# ('ICU_OUTTIME_SEC', 80), ('subject_id', 81), ('hadm_id', 82), ('icustay_id', 83), ('unit', 84), ('procedure', 85)]

