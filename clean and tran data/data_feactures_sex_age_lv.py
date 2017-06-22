'''
Created on 2017年5月15日
性别的转化率特征
年龄的转化率特征
@author: liu
'''
import pandas as pd
from collections import Counter

ACTION_FILE='E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran_new\\SCleanUser_Action.csv'
USER_FILE='E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran_new\\S_S_Clean_ratio_User.csv'

SEX_FILE='E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran_new\\JData_all_ratioclean_sex.csv'
AGE_FILE='E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran_new\\JData_all_ratioclean_age.csv'
LV_FILE='E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran_new\\JData_all_ratioclean_lv.csv'


#读取数据,输入文件的地址和需要的字段  （因为内存有限，而且有的时候我们没有必要将所有字段都读取）
def get_date_needlist(fname,rowlist):
    #因为数据量较大，一次读入可能会导致内存错误，所以使用pandas的分块chunk进行读取
    reader=pd.read_csv(fname,header=0,iterator=True)#后面的iterator=True是在喊“我要分块读，你不要把我一次读完”
    chunkSize=100000
    chunks=[]
    loop=True
    while loop:
        try:
            chunk=reader.get_chunk(chunkSize)[rowlist]
            chunks.append(chunk)
        except StopIteration:
            loop=False
            print("Iteration is stopped")
    df_ac=pd.concat(chunks,ignore_index=True)
    return df_ac

#性别的转化率特征
def sex_add_type_count(gg):
    
    behavior_type=gg.type.astype(int)
    type_cnt=Counter(behavior_type) #Counter是字典的子类，目的是跟踪值出现的次数，key是索引，value是key出现的此时
    gg['sex_browse_num']=type_cnt[1]
    gg['sex_addcar_num']=type_cnt[2]
    gg['sex_delcar_num']=type_cnt[3]
    gg['sex_buy_num']=type_cnt[4]
    gg['sex_fav_num']=type_cnt[5]
    gg['sex_click_num']=type_cnt[6]
    
    return gg[['sex','sex_browse_num','sex_addcar_num','sex_delcar_num','sex_buy_num','sex_fav_num','sex_click_num']]

def allsex_countRate():
    #求count
    needlist_ac=['user_id','type']
    df_all_sex=get_date_needlist(ACTION_FILE,needlist_ac)
    needlist_user=['user_id','sex']
    df_user=get_date_needlist(USER_FILE, needlist_user)
    df_all_sex=pd.merge(df_all_sex,df_user,on='user_id',how='inner')
    del df_all_sex['user_id']
    
    df_all_sex=df_all_sex.groupby('sex',as_index=False).apply(sex_add_type_count) 
    #求比例
    df_all_sex['sex_buyBrowse_ratio']=df_all_sex['sex_buy_num']/df_all_sex['sex_browse_num']
    df_all_sex['sex_buyAddcar_ratio']=df_all_sex['sex_buy_num']/df_all_sex['sex_addcar_num']
    df_all_sex['sex_buyFav_ratio']=df_all_sex['sex_buy_num']/df_all_sex['sex_fav_num']
    df_all_sex['sex_buyClick_radio']=df_all_sex['sex_buy_num']/df_all_sex['sex_click_num']
    #当被除数为0，除数不为零时， 零
    df_all_sex.ix[(df_all_sex['sex_browse_num']==0)&(df_all_sex['sex_buy_num']!=0),'sex_buyBrowse_ratio']=0 #ix【行，列】
    df_all_sex.ix[(df_all_sex['sex_addcar_num']==0)&(df_all_sex['sex_buy_num']!=0),'sex_buyAddcar_ratio']=0
    df_all_sex.ix[(df_all_sex['sex_fav_num']==0)&(df_all_sex['sex_buy_num']!=0),'sex_buyFav_ratio']=0
    df_all_sex.ix[(df_all_sex['sex_click_num']==0)&(df_all_sex['sex_buy_num']!=0),'sex_buyClick_radio']=0
    #当被除数除数都为0，  一
    df_all_sex.ix[(df_all_sex['sex_browse_num']==0)&(df_all_sex['sex_buy_num']==0),'sex_buyBrowse_ratio']=1 #ix【行，列】
    df_all_sex.ix[(df_all_sex['sex_addcar_num']==0)&(df_all_sex['sex_buy_num']==0),'sex_buyAddcar_ratio']=1
    df_all_sex.ix[(df_all_sex['sex_fav_num']==0)&(df_all_sex['sex_buy_num']==0),'sex_buyFav_ratio']=1
    df_all_sex.ix[(df_all_sex['sex_click_num']==0)&(df_all_sex['sex_buy_num']==0),'sex_buyClick_radio']=1
    print(df_all_sex.head(5))
    #df_all_sex.to_csv(ALL_RATIO_sex,index=False) 
    df_all_sex.drop_duplicates('sex',inplace=True)
    df_all_sex.to_csv(SEX_FILE,index=False) 
    print('end')


#年龄转化率特征
def age_add_type_count(gg):
    
    behavior_type=gg.type.astype(int)
    type_cnt=Counter(behavior_type) #Counter是字典的子类，目的是跟踪值出现的次数，key是索引，value是key出现的此时
    gg['age_browse_num']=type_cnt[1]
    gg['age_addcar_num']=type_cnt[2]
    gg['age_delcar_num']=type_cnt[3]
    gg['age_buy_num']=type_cnt[4]
    gg['age_fav_num']=type_cnt[5]
    gg['age_click_num']=type_cnt[6]
    
    return gg[['age','age_browse_num','age_addcar_num','age_delcar_num','age_buy_num','age_fav_num','age_click_num']]

def allage_countRate():
    #求count
    needlist_ac=['user_id','type']
    df_all_age=get_date_needlist(ACTION_FILE,needlist_ac)
    needlist_user=['user_id','age']
    df_user=get_date_needlist(USER_FILE, needlist_user)
    df_all_age=pd.merge(df_all_age,df_user,on='user_id',how='inner')
    del df_all_age['user_id']
    
    df_all_age=df_all_age.groupby('age',as_index=False).apply(age_add_type_count) 
    #求比例
    df_all_age['age_buyBrowse_ratio']=df_all_age['age_buy_num']/df_all_age['age_browse_num']
    df_all_age['age_buyAddcar_ratio']=df_all_age['age_buy_num']/df_all_age['age_addcar_num']
    df_all_age['age_buyFav_ratio']=df_all_age['age_buy_num']/df_all_age['age_fav_num']
    df_all_age['age_buyClick_radio']=df_all_age['age_buy_num']/df_all_age['age_click_num']
    #当被除数为0，除数不为零时， 零
    df_all_age.ix[(df_all_age['age_browse_num']==0)&(df_all_age['age_buy_num']!=0),'age_buyBrowse_ratio']=0 #ix【行，列】
    df_all_age.ix[(df_all_age['age_addcar_num']==0)&(df_all_age['age_buy_num']!=0),'age_buyAddcar_ratio']=0
    df_all_age.ix[(df_all_age['age_fav_num']==0)&(df_all_age['age_buy_num']!=0),'age_buyFav_ratio']=0
    df_all_age.ix[(df_all_age['age_click_num']==0)&(df_all_age['age_buy_num']!=0),'age_buyClick_radio']=0
    #当被除数除数都为0，  一
    df_all_age.ix[(df_all_age['age_browse_num']==0)&(df_all_age['age_buy_num']==0),'age_buyBrowse_ratio']=1 #ix【行，列】
    df_all_age.ix[(df_all_age['age_addcar_num']==0)&(df_all_age['age_buy_num']==0),'age_buyAddcar_ratio']=1
    df_all_age.ix[(df_all_age['age_fav_num']==0)&(df_all_age['age_buy_num']==0),'age_buyFav_ratio']=1
    df_all_age.ix[(df_all_age['age_click_num']==0)&(df_all_age['age_buy_num']==0),'age_buyClick_radio']=1
    print(df_all_age.head(5))
    #df_all_age.to_csv(ALL_RATIO_age,index=False) 
    df_all_age.drop_duplicates('age',inplace=True)
    df_all_age.to_csv(AGE_FILE,index=False) 
    print('end')
    return


#等级转化特征
def lv_add_type_count(gg):
    
    behavior_type=gg.type.astype(int)
    type_cnt=Counter(behavior_type) #Counter是字典的子类，目的是跟踪值出现的次数，key是索引，value是key出现的此时
    gg['lv_browse_num']=type_cnt[1]
    gg['lv_addcar_num']=type_cnt[2]
    gg['lv_delcar_num']=type_cnt[3]
    gg['lv_buy_num']=type_cnt[4]
    gg['lv_fav_num']=type_cnt[5]
    gg['lv_click_num']=type_cnt[6]
    
    return gg[['user_lv_cd','lv_browse_num','lv_addcar_num','lv_delcar_num','lv_buy_num','lv_fav_num','lv_click_num']]

def lv_countRate():
    #求count
    needlist_ac=['user_id','type']
    df_all_lv=get_date_needlist(ACTION_FILE,needlist_ac)
    needlist_user=['user_id','user_lv_cd']
    df_user=get_date_needlist(USER_FILE, needlist_user)
    df_all_lv=pd.merge(df_all_lv,df_user,on='user_id',how='inner')
    del df_all_lv['user_id']
    
    df_all_lv=df_all_lv.groupby('user_lv_cd',as_index=False).apply(lv_add_type_count) 
    #求比例
    df_all_lv['lv_buyBrowse_ratio']=df_all_lv['lv_buy_num']/df_all_lv['lv_browse_num']
    df_all_lv['lv_buyAddcar_ratio']=df_all_lv['lv_buy_num']/df_all_lv['lv_addcar_num']
    df_all_lv['lv_buyFav_ratio']=df_all_lv['lv_buy_num']/df_all_lv['lv_fav_num']
    df_all_lv['lv_buyClick_radio']=df_all_lv['lv_buy_num']/df_all_lv['lv_click_num']
    #当被除数为0，除数不为零时， 零
    df_all_lv.ix[(df_all_lv['lv_browse_num']==0)&(df_all_lv['lv_buy_num']!=0),'lv_buyBrowse_ratio']=0 #ix【行，列】
    df_all_lv.ix[(df_all_lv['lv_addcar_num']==0)&(df_all_lv['lv_buy_num']!=0),'lv_buyAddcar_ratio']=0
    df_all_lv.ix[(df_all_lv['lv_fav_num']==0)&(df_all_lv['lv_buy_num']!=0),'lv_buyFav_ratio']=0
    df_all_lv.ix[(df_all_lv['lv_click_num']==0)&(df_all_lv['lv_buy_num']!=0),'lv_buyClick_radio']=0
    #当被除数除数都为0，  一
    df_all_lv.ix[(df_all_lv['lv_browse_num']==0)&(df_all_lv['lv_buy_num']==0),'lv_buyBrowse_ratio']=1 #ix【行，列】
    df_all_lv.ix[(df_all_lv['lv_addcar_num']==0)&(df_all_lv['lv_buy_num']==0),'lv_buyAddcar_ratio']=1
    df_all_lv.ix[(df_all_lv['lv_fav_num']==0)&(df_all_lv['lv_buy_num']==0),'lv_buyFav_ratio']=1
    df_all_lv.ix[(df_all_lv['lv_click_num']==0)&(df_all_lv['lv_buy_num']==0),'lv_buyClick_radio']=1
    print(df_all_lv.head(5))
    #df_all_lv.to_csv(ALL_RATIO_lv,index=False) 
    df_all_lv.drop_duplicates('user_lv_cd',inplace=True)
    df_all_lv.to_csv(LV_FILE,index=False) 
    print('end')
    return

#年龄--性别 转化率特征
def sex_age_countRate():
    return


if __name__=='__main__':
#     allsex_countRate()
#     print('sex end')
#     allage_countRate()
#     print('age end')
    lv_countRate()