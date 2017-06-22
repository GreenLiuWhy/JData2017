'''
Created on 2017年4月18日
内容：
一。针对 JData_User.csv
1.将age字段的字转换为  int，也就是将其离散化
2.将user_reg_tm 字段 从 年/月/日   转化为距离预测时间的天数
二。针对JData_Comment.csv
将 dt 字段   从  年/月/日  字段  转化为距离预测时间的天数
三。针对action系列表
将3各表合并成1个表，并对日期进行编号（02的是1~31,03的是32~）（其实也可以是 距离预测时间的  天数）
@author: liu
'''
import pandas as pd
from collections import Counter

ALL_USER="E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_org\\JData_User.csv"
ALL_ACTION201602="E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_org\\JData_Action_201602.csv"
ALL_ACTION201603="E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_org\\JData_Action_201603.csv"
ALL_ACTION201604="E:\\ProgrameGra2\\Python\\MyJDateVer1\data_org\\JData_Action_201604.csv" #最后要把10天时间留出来一部分作为交叉验证集，一部分作为测试集，但是在这里是画图分析数据，所以就全面考虑数据的好
ALL_COMMENT="E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_org\\JData_Comment.csv"


ALL_TRAN_USER="E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran\\JData_Tran_User.csv"
ALL_TRAN_COMMENT="E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran\\JData_Tran_Comment.csv"
ALL_TRAN_ACTION="E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran\\JData_Tran_Action.csv"
ALL_RATIO_USER="E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran\\JData_ratio_User.csv"
ALL_RATIO_PRODUCT='E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran\\JData_all_ratio_product.csv' #统计信息的所有product，不仅仅包括product表里的，而是从action表中得到的所有product信息

ALL_CLEANRATIO_PRODUCT='E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran\\JData_all_ratioclean_product.csv' 
ADING_USER='E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_org\\JData_ratio_User.csv'
ALL_CLEANRATIO_BRAND='E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran\\JData_all_ratioclean_brand.csv'

CLEAN_USER_ACTION='E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran\\JData_CleanUser_Action.csv'

timeStr="2016/4/16"
timeTran=pd.to_datetime(timeStr)#目标时间

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

#转化年龄
def convert_age(ageStr):
    if ageStr == u'-1':
        return -1
    if ageStr == u'15岁以下':
        return 0
    if ageStr == u'16-25岁':
        return 1
    if ageStr == u'26-35岁':
        return 2
    if ageStr == u'36-45岁':
        return 3
    if ageStr == u'46-55岁':
        return 4
    if ageStr == u'56岁以上':
        return 5
    else:
        return -1

    
#转换user表
def user_tran():
    df_user=pd.read_csv(ALL_USER,header=0,encoding='gbk')#这个文件不是特别大，而且是GBK编码，所以直接读它
    df_user=df_user.drop_duplicates('user_id')
    #转化年龄
    df_user['age']=df_user['age'].map(convert_age)
    #转化注册时间距离预测时间的时间差（单位是天）
    df_user['user_reg_tm']=pd.to_datetime(df_user['user_reg_tm'])
    df_user['user_reg_diff']=[i for i in (timeTran-df_user['user_reg_tm']).dt.days]
    #写入文件
    df_user.to_csv(ALL_TRAN_USER,index=False)#不加index=False会单独处理index这一行
    
#转化comment表
def comment_tran():
    df_comment=get_date(ALL_COMMENT)
    df_comment['dt']=pd.to_datetime(df_comment['dt'])
    df_comment['com_diff']=[i for i in (timeTran-df_comment['dt']).dt.days]
    df_comment.to_csv(ALL_TRAN_COMMENT,index=False)
    
#转化action表
def action_tran():
    df_action02=get_date(ALL_ACTION201602)
    df_action03=get_date(ALL_ACTION201603)
    df_action04=get_date(ALL_ACTION201604)
    #将几个表联合起来
    df_ac=pd.concat([df_action02,df_action03,df_action04],ignore_index=True)
    #df_ac=df_ac.drop_duplicates() #因为时间粒度是分钟，所以不应该去重复
    #转化时间
    df_ac['time']=pd.to_datetime(df_ac['time'])
    df_ac['action_diff']=[i for i in (timeTran-df_ac['time']).dt.days]
    df_ac.to_csv(ALL_TRAN_ACTION,index=False)

#测试action表有没有转化好，如果数据量差不多就可以
def text_action():
    ADING_ALL_ACTION='E:\\ProgrameGra2\\Python\\MyJDateVer1\\tran_By_aDing\\data_new\\JData_Action_All.csv'
    df_ac=get_date(ADING_ALL_ACTION)
    print(df_ac.shape[0])   
    #50601736
    #加起来应该是 50601736 是重复的有那么多么？

#为了方便进行数据清理工作，现对user表里加入一些统计特征和比例特征
#统计特征包括：user_browse_num.user_addcar_num,user_delcar_num,user_buy_num,user_fav_num,user_click_num
#比例特征包括：user_buyBrowse_ratio,user_buyAddcar_ratio,user_buyFav_ratio,user_buyClick_radio
def add_type_count(gg):
    
    behavior_type=gg.type.astype(int)
    type_cnt=Counter(behavior_type) #Counter是字典的子类，目的是跟踪值出现的次数，key是索引，value是key出现的此时
    gg['user_browse_num']=type_cnt[1]
    gg['user_addcar_num']=type_cnt[2]
    gg['user_delcar_num']=type_cnt[3]
    gg['user_buy_num']=type_cnt[4]
    gg['user_fav_num']=type_cnt[5]
    gg['user_click_num']=type_cnt[6]
    
    return gg[['user_id','user_browse_num','user_addcar_num','user_delcar_num','user_buy_num','user_fav_num','user_click_num']]

def userTable_sample_countRate():
    #求count
    needlist_ac=['user_id','type']
    df_user=get_date_needlist("E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran\\JData_Tran_Action.csv",needlist_ac)
    df_user=df_user.groupby('user_id',as_index=False).apply(add_type_count) 
    '''
    df_all_ac=get_date("E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran\\JData_Tran_Action.csv")
    df_all_user=get_date("E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran\\JData_Tran_User.csv")
    df_temp=df_all_ac.groupby('user_id',as_index=False).apply(add_type_count) 
    '''
    #求比例
    df_user['user_buyBrowse_ratio']=df_user['user_buy_num']/df_user['user_browse_num']
    df_user['user_buyAddcar_ratio']=df_user['user_buy_num']/df_user['user_addcar_num']
    df_user['user_buyFav_ratio']=df_user['user_buy_num']/df_user['user_fav_num']
    df_user['user_buyClick_radio']=df_user['user_buy_num']/df_user['user_click_num']
    #当被除数为0，除数不为零时， 零
    df_user.ix[(df_user['user_browse_num']==0)&(df_user['user_buy_num']!=0),'user_buyBrowse_ratio']=0 #ix【行，列】
    df_user.ix[(df_user['user_addcar_num']==0)&(df_user['user_buy_num']!=0),'user_buyAddcar_ratio']=0
    df_user.ix[(df_user['user_fav_num']==0)&(df_user['user_buy_num']!=0),'user_buyFav_ratio']=0
    df_user.ix[(df_user['user_click_num']==0)&(df_user['user_buy_num']!=0),'pro_buyClick_radio']=0
    #当被除数除数都为0，  一
    df_user.ix[(df_user['user_browse_num']==0)&(df_user['user_buy_num']==0),'user_buyBrowse_ratio']=1 #ix【行，列】
    df_user.ix[(df_user['user_addcar_num']==0)&(df_user['user_buy_num']==0),'user_buyAddcar_ratio']=1
    df_user.ix[(df_user['user_fav_num']==0)&(df_user['user_buy_num']==0),'user_buyFav_ratio']=1
    df_user.ix[(df_user['user_click_num']==0)&(df_user['user_buy_num']==0),'user_buyFav_ratio']=1    

    df_user.drop_duplicates('user_id',inplace=True)
    df_user.to_csv(ALL_RATIO_USER,index=False)  

#得到action表中出现的所有product的统计信息    
def product_add_type_count(gg):
    
    behavior_type=gg.type.astype(int)
    type_cnt=Counter(behavior_type) #Counter是字典的子类，目的是跟踪值出现的次数，key是索引，value是key出现的此时
    gg['pro_browse_num']=type_cnt[1]
    gg['pro_addcar_num']=type_cnt[2]
    gg['pro_delcar_num']=type_cnt[3]
    gg['pro_buy_num']=type_cnt[4]
    gg['pro_fav_num']=type_cnt[5]
    gg['pro_click_num']=type_cnt[6]
    
    return gg[['sku_id','pro_browse_num','pro_addcar_num','pro_delcar_num','pro_buy_num','pro_fav_num','pro_click_num']]

def allProduct_countRate():
    #求count
    needlist_ac=['sku_id','type']
    #df_all_product=get_date_needlist("E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran\\JData_Tran_Action.csv",needlist_ac)
    df_all_product=get_date_needlist(CLEAN_USER_ACTION,needlist_ac)
    df_all_product=df_all_product.groupby('sku_id',as_index=False).apply(product_add_type_count) 
    #求比例
    df_all_product['pro_buyBrowse_ratio']=df_all_product['pro_buy_num']/df_all_product['pro_browse_num']
    df_all_product['pro_buyAddcar_ratio']=df_all_product['pro_buy_num']/df_all_product['pro_addcar_num']
    df_all_product['pro_buyFav_ratio']=df_all_product['pro_buy_num']/df_all_product['pro_fav_num']
    df_all_product['pro_buyClick_radio']=df_all_product['pro_buy_num']/df_all_product['pro_click_num']
    #当被除数为0，除数不为零时， 零
    df_all_product.ix[(df_all_product['pro_browse_num']==0)&(df_all_product['pro_buy_num']!=0),'pro_buyBrowse_ratio']=0 #ix【行，列】
    df_all_product.ix[(df_all_product['pro_addcar_num']==0)&(df_all_product['pro_buy_num']!=0),'pro_buyAddcar_ratio']=0
    df_all_product.ix[(df_all_product['pro_fav_num']==0)&(df_all_product['pro_buy_num']!=0),'pro_buyFav_ratio']=0
    df_all_product.ix[(df_all_product['pro_click_num']==0)&(df_all_product['pro_buy_num']!=0),'pro_buyClick_radio']=0
    #当被除数除数都为0，  一
    df_all_product.ix[(df_all_product['pro_browse_num']==0)&(df_all_product['pro_buy_num']==0),'pro_buyBrowse_ratio']=1 #ix【行，列】
    df_all_product.ix[(df_all_product['pro_addcar_num']==0)&(df_all_product['pro_buy_num']==0),'pro_buyAddcar_ratio']=1
    df_all_product.ix[(df_all_product['pro_fav_num']==0)&(df_all_product['pro_buy_num']==0),'pro_buyFav_ratio']=1
    df_all_product.ix[(df_all_product['pro_click_num']==0)&(df_all_product['pro_buy_num']==0),'pro_buyClick_radio']=1
    print(df_all_product.head(5))
    #df_all_product.to_csv(ALL_RATIO_PRODUCT,index=False) 
    df_all_product.to_csv(ALL_CLEANRATIO_PRODUCT,index=False) 
    print('end')

#得到action表中出现的所有product的统计信息    
def brand_add_type_count(gg):
    
    behavior_type=gg.type.astype(int)
    type_cnt=Counter(behavior_type) #Counter是字典的子类，目的是跟踪值出现的次数，key是索引，value是key出现的此时
    gg['brand_browse_num']=type_cnt[1]
    gg['brand_addcar_num']=type_cnt[2]
    gg['brand_delcar_num']=type_cnt[3]
    gg['brand_buy_num']=type_cnt[4]
    gg['brand_fav_num']=type_cnt[5]
    gg['brand_click_num']=type_cnt[6]
    
    return gg[['brand','brand_browse_num','brand_addcar_num','brand_delcar_num','brand_buy_num','brand_fav_num','brand_click_num']]

def allbrand_countRate():
    #求count
    needlist_ac=['brand','type']
    #df_all_product=get_date_needlist("E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran\\JData_Tran_Action.csv",needlist_ac)
    df_all_brandduct=get_date_needlist(CLEAN_USER_ACTION,needlist_ac)
    df_all_brandduct=df_all_brandduct.groupby('brand',as_index=False).apply(brand_add_type_count) 
    #求比例
    df_all_brandduct['brand_buyBrowse_ratio']=df_all_brandduct['brand_buy_num']/df_all_brandduct['brand_browse_num']
    df_all_brandduct['brand_buyAddcar_ratio']=df_all_brandduct['brand_buy_num']/df_all_brandduct['brand_addcar_num']
    df_all_brandduct['brand_buyFav_ratio']=df_all_brandduct['brand_buy_num']/df_all_brandduct['brand_fav_num']
    df_all_brandduct['brand_buyClick_radio']=df_all_brandduct['brand_buy_num']/df_all_brandduct['brand_click_num']
    #当被除数为0，除数不为零时， 零
    df_all_brandduct.ix[(df_all_brandduct['brand_browse_num']==0)&(df_all_brandduct['brand_buy_num']!=0),'brand_buyBrowse_ratio']=0 #ix【行，列】
    df_all_brandduct.ix[(df_all_brandduct['brand_addcar_num']==0)&(df_all_brandduct['brand_buy_num']!=0),'brand_buyAddcar_ratio']=0
    df_all_brandduct.ix[(df_all_brandduct['brand_fav_num']==0)&(df_all_brandduct['brand_buy_num']!=0),'brand_buyFav_ratio']=0
    df_all_brandduct.ix[(df_all_brandduct['brand_click_num']==0)&(df_all_brandduct['brand_buy_num']!=0),'brand_buyClick_radio']=0
    #当被除数除数都为0，  一
    df_all_brandduct.ix[(df_all_brandduct['brand_browse_num']==0)&(df_all_brandduct['brand_buy_num']==0),'brand_buyBrowse_ratio']=1 #ix【行，列】
    df_all_brandduct.ix[(df_all_brandduct['brand_addcar_num']==0)&(df_all_brandduct['brand_buy_num']==0),'brand_buyAddcar_ratio']=1
    df_all_brandduct.ix[(df_all_brandduct['brand_fav_num']==0)&(df_all_brandduct['brand_buy_num']==0),'brand_buyFav_ratio']=1
    df_all_brandduct.ix[(df_all_brandduct['brand_click_num']==0)&(df_all_brandduct['brand_buy_num']==0),'brand_buyClick_radio']=1
    print(df_all_brandduct.head(5))
    #df_all_brandduct.to_csv(ALL_RATIO_brandDUCT,index=False) 
    df_all_brandduct.drop_duplicates('brand',inplace=True)
    df_all_brandduct.to_csv(ALL_CLEANRATIO_BRAND,index=False) 
    print('end')

#因为自己电脑内存不足，所以用阿丁的结果得到自己需要的结果
def ading_to_my():
    df_ading=get_date(ADING_USER)
    df_user=get_date(ALL_TRAN_USER)
    df_my=df_user.merge(df_ading,on='user_id',how='inner')
    del df_my['age_y']
    del df_my['sex_y']
    del df_my['user_lv_cd_y']
    df_my.columns=(['user_id','age','sex','user_lv_cd','user_reg_tm','user_reg_diff','user_browse_num','user_addcar_num','user_delcar_num','user_buy_num',\
                    'user_fav_num','user_click_num','user_buyAddcar_ratio','user_buyBrowse_ratio','user_buyClick_radio',\
                    'user_buyFav_ratio',])
    df_my.to_csv(ALL_RATIO_USER,index=False)
    print(df_my.head(5))
       
if __name__=="__main__":
    #主函数
#     user_tran()
#     comment_tran()
#     action_tran()
#     text_action()
    userTable_sample_countRate()
#     ading_to_my()
#     allProduct_countRate()
#     allbrand_countRate()
