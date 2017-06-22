'''
Created on 2017年5月2日
主要是针对清理的数据再对action进行一下清理
@author: liu
'''
import pandas as pd

#CLEAN_USER='E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran\\JData_clean_ratio_User.csv'
CLEAN_USER="E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran\\SClean_ratio_User.csv"
ALL_ACTION='E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran\\JData_Tran_Action.csv'
#CLEAN_USER_ACTION='E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran\\JData_CleanUser_Action.csv'
CLEAN_USER_ACTION='E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran\\SCleanUser_Action.csv'

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

#读取数据,输入文件的地址和需要的字段  （因为内存有限，而且有的时候我们没有必要将所有字段都读取）
def get_date(fname):
    #因为数据量较大，一次读入可能会导致内存错误，所以使用pandas的分块chunk进行读取
    reader=pd.read_csv(fname,header=0,iterator=True)#后面的iterator=True是在喊“我要分块读，你不要把我一次读完”
    chunkSize=100000
    chunks=[]
    loop=True
    while loop:
        try:
            chunk=reader.get_chunk(chunkSize)
            chunks.append(chunk)
        except StopIteration:
            loop=False
            print("Iteration is stopped")
    df_ac=pd.concat(chunks,ignore_index=True)
    #df_ac=df_ac.drop_duplicates() #所以原来的程序这里是不对的，应该是所有的都相同才删除吧
    #这个应该也是不要去重的
    return df_ac
#根据user的清理结果清理action
def clean_Action_BaseOnUser():
    needlist_user=['user_id']
    df_user=get_date_needlist(CLEAN_USER, needlist_user)
    df_action=get_date(ALL_ACTION)
    print('meger bagain')
    df_action=pd.merge(df_user,df_action,on='user_id',how='inner',copy=False)
    print('meger end')
    df_action.to_csv(CLEAN_USER_ACTION,index=False)
    print('end')
    return

#得到所有cate=8的数据
def cate8_action():
    df_ac=get_date("E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran\\JData_Tran_Action.csv")
    df_pro=get_date_needlist('E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_org\\JData_Product.csv',['sku_id'])
    print('meger bagain')
    df_ac=pd.merge(df_pro,df_ac,on='sku_id',how='inner',copy=False)
    print('meger end')
    df_ac.to_csv('E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran_new\\cate8Action.csv',index=False)
    print('end')
    return

def theMain():
    #clean_Action_BaseOnUser()
    cate8_action()
    
theMain()