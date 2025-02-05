#!/usr/bin/env python
# coding: utf-8

# To score new file, process raw data by running code in step 1 to generated score ready dataset. 

get_ipython().run_line_magic('pip', 'install catboost')

import pandas as pd
import numpy as np
import pickle
import re
import math
from catboost import CatBoostClassifier, Pool
from collections import defaultdict
import time
from datetime import datetime
import sys
sys.path.append('/home/packages')

from score_columns import score_cols, numvar_columns, catvar_columns # use score_columns.py

class INJ_SCR_PROCESSOR(object):
    """
    SCR process.
    """

    def __init__(self, data_path, score_cols, numvar_columns, catvar_columns):
        self.PATH = data_path
        #self.train_mode = train_mode
        return

    def read_scr(self, SCR):
        """
        read scr file
        :param SCR:
        :return:
        """
        self.scr = pd.read_csv(self.PATH + SCR, delimiter='|', lineterminator='\n')
        return
    
    def score_dt(self, score_cols, numvar_columns, catvar_columns):
        self.scr_new=self.scr[score_cols]
        #print('self scr_new', self.scr_new.info())
        
        self.scr_new_id=self.scr_new[['clm_id','prtpt_id']]
        self.scr_new.drop(['clm_id','prtpt_id'], axis=1, inplace=True)
        
        # Define/ensure the correct column type
        for col in numvar_columns:
            self.scr_new[col]=pd.to_numeric(self.scr_new[col], errors='coerce')
            
        for col in catvar_columns:
            self.scr_new[col]=self.scr_new[col].astype(object)
            
        for var in numvar_columns:
            self.scr_new[var].fillna(self.scr_new[var].mean(), inplace=True) 
            
        for var in catvar_columns:
            self.scr_new[var]=self.scr_new[var].fillna('X')
            
        with open('catmodel_gs_optimal_deploy.pkl', 'rb') as f:
            catmodel = pickle.load(f)
            
        # Predict the prepared file
        predictions_proba = catmodel.predict_proba(self.scr_new)
        predictions_proba=predictions_proba[:,1]
        self.scr_new['predict_proba']=predictions_proba.tolist() 
        self.scr_new['clm_id']=self.scr_new_id['clm_id']
        self.scr_new['prtpt_id']=self.scr_new_id['prtpt_id']
        
        self.scr_new=self.scr_new[['clm_id','prtpt_id','predict_proba']]

        return self.scr_new

    def main(self, SCR):
        print('reading and processing scr data..', datetime.now())
        self.read_scr(SCR)
        print(self.scr.shape)
        
        self.scr_new=self.score_dt(score_cols, numvar_columns, catvar_columns)
        return self.scr_new

if __name__ == '__main__':
    path='s3://s-m/'

    SCR='processed_2024/clm_final.csv' 
    
    score_processor = INJ_SCR_PROCESSOR(path, score_cols, numvar_columns, catvar_columns)
    s = time.time()
    score = score_processor.main(SCR)
    
    print('type of score', type(score))
    e = time.time()
    print(f'  ...data processed in {round((e - s)/60, 3)} minutes \n')

    save_path = path+'processed_2024/inj_score.csv'
     
    if save_path:
        print('saving..')
        score.to_csv(save_path, index=False, sep='|', encoding='utf-8')


# Check data
path='s3://s-m/'

save_path = path+'processed_2024/inj_score.csv'

check=pd.read_csv(save_path, delimiter='|', dtype='object',encoding='latin1',on_bad_lines='skip',
                                    lineterminator='\n') 

check.info(verbose=True)

check.head()



