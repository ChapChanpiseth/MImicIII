# -*- coding: utf-8 -*-

import pandas as pd

from preprocessing.base import Base


class ChartEvent(Base):
    """
        Table: CHARTEVENTS
    """

    def __init__(self, **kwargs):
        Base.__init__(self, **kwargs)

    def read_csv(self, criteria=None):
        """
            Read data from table CHARTEVENTS
        """

        # CHARTEVENTS file name
        filename = self.config['FILE_DIR'] + self.config['IN_FNAME']['CHARTEVENTS']
        usecols = ['ROW_ID', 'SUBJECT_ID', 'HADM_ID', 'ICUSTAY_ID',	'ITEMID', 'CHARTTIME', 'STORETIME',\
        	'CGID', 'VALUE', 'VALUENUM', 'VALUEUOM', 'WARNING', 'ERROR', 'RESULTSTATUS', 'STOPPED']

        # Read from csv file
        df_chartevs = pd.read_csv(filename, encoding='latin1', usecols=usecols)

        # Read from csv file
        if not criteria:
            df_chartevs = pd.read_csv(filename, encoding='latin1', usecols=usecols)
        elif criteria['nrows'] is not None:
            df_chartevs = pd.read_csv(filename, encoding='latin1', usecols=usecols, nrows=criteria['nrows'])

        return df_chartevs

    def get_chartevents_by_phadmicu(self, criteria=None):
        """ Retrieve CHARTEVENTS matching the give hospital admission
        """

        # Add prefix to column's name
        df_chartevs = self.read_csv(criteria)
        df_chartevs = df_chartevs.add_prefix(self.config['PREFIX_CHEV'])

        # Filter Dataframe by SUBJECT_ID, HADM_ID and ICUSTAY_ID
        mask = (df_chartevs[self.config['PREFIX_CHEV'] + 'SUBJECT_ID'].isin(\
            criteria[self.config['PREFIX_CHEV'] + 'SUBJECT_ID'].tolist()))\
            & (df_chartevs[self.config['PREFIX_CHEV'] + 'HADM_ID'].isin(\
                criteria[self.config['PREFIX_CHEV'] + 'HADM_ID'].tolist()))
        
        df_chartevs = df_chartevs[mask]

        return df_chartevs
