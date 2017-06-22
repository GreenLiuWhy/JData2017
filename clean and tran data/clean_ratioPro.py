'''
Created on 2017年5月9日
data_tran里面的product说明：
JData_all_ratio_product.csv：所有action信息中出现的product的统计信息和转化率信息
JData_all_ratioclean_product.csv：通过对清理后的action表中的信息中出现的product的统计、转化率信息
JData_inter_product.csv：product表中的product的统计、转化率信息
@author: liu
'''

import pandas as pd
import numpy as np

#清除JData_all_ratioclean_product.csv里面的重复信息并保存
def dropDup_ratioclean_product():
    ALL_CLEANRATIO_PRODUCT='E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran\\JData_all_ratioclean_product.csv' 
    df=pd.read_csv(ALL_CLEANRATIO_PRODUCT)
    print('read over')
    df.drop_duplicates('sku_id',inplace=True)
    print('drop over')
    del df['Unnamed: 0']
    df.to_csv(ALL_CLEANRATIO_PRODUCT,index=False)
    print('over')
    return

#dropDup_ratioclean_product()

#清除JData_all_ratio_product.csv.csv里面的重复信息并保存
def dropDup_allratio_product():
    ALL_RATIO_PRODUCT='E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran\\JData_all_ratio_product.csv' 
    df=pd.read_csv(ALL_RATIO_PRODUCT)
    print('read over')
    df.drop_duplicates('sku_id',inplace=True)
    print('drop over')
    df.to_csv(ALL_RATIO_PRODUCT,index=False)
    print('over')
    return 

dropDup_allratio_product()
