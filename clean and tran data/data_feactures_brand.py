'''
Created on 2017年5月3日
1.时间尺度的统计特征 and 时间尺度的转化率特征
@author: liu
'''
import pandas as pd
import numpy as np
from collections import Counter
ACTION_TABLE="E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_choosed\\Small_Action.csv"


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

#服务于下面的统计函数
def sever_count_feacture(group):
    behavior_type = group.type.astype(int)
    type_cnt = Counter(behavior_type)

    group['timeBrand_browse_num'] = type_cnt[1]
    group['timeBrand_addcart_num'] = type_cnt[2]
    group['timeBrand_delcart_num'] = type_cnt[3]
    group['timeBrand_buy_num'] = type_cnt[4]
    group['timeBrand_favor_num'] = type_cnt[5]
    group['timeBrand_click_num'] = type_cnt[6]

    return group

#时间尺度的统计特征
def get_count_feacture(df_inwin,df_fea):
    #求统计
    df_inwin=df_inwin.drop(['user_id','sku_id','cate','action_diff'],axis=1)
    df_item = df_inwin.groupby(['brand'], as_index = False).apply(sever_count_feacture)
    del df_item['type']
    df_item=df_item.drop_duplicates('brand')
    df_fea=df_fea.merge(df_item,on='brand',how='left') 
    #求比例
    df_fea['timeBrand_buyBrowse_ratio']=df_fea['timeBrand_buy_num']/df_fea['timeBrand_browse_num']
    df_fea['timeBrand_buyAddcar_ratio']=df_fea['timeBrand_buy_num']/df_fea['timeBrand_addcart_num']
    df_fea['timeBrand_buyFav_ratio']=df_fea['timeBrand_buy_num']/df_fea['timeBrand_favor_num']
    df_fea['timeBrand_buyClick_radio']=df_fea['timeBrand_buy_num']/df_fea['timeBrand_click_num']
    #当被除数为0，除数不为零时， 零
    df_fea.ix[(df_fea['timeBrand_browse_num']==0)&(df_fea['timeBrand_buy_num']!=0),'timeBrand_buyBrowse_ratio']=0 #ix【行，列】
    df_fea.ix[(df_fea['timeBrand_addcart_num']==0)&(df_fea['timeBrand_buy_num']!=0),'timeBrand_buyAddcar_ratio']=0
    df_fea.ix[(df_fea['timeBrand_favor_num']==0)&(df_fea['timeBrand_buy_num']!=0),'timeBrand_buyFav_ratio']=0
    df_fea.ix[(df_fea['timeBrand_click_num']==0)&(df_fea['timeBrand_buy_num']!=0),'timeBrand_buyClick_radio']=0
    #当被除数除数都为0，  一
    df_fea.ix[(df_fea['timeBrand_browse_num']==0)&(df_fea['timeBrand_buy_num']==0),'timeBrand_buyBrowse_ratio']=1 #ix【行，列】
    df_fea.ix[(df_fea['timeBrand_addcart_num']==0)&(df_fea['timeBrand_buy_num']==0),'timeBrand_buyAddcar_ratio']=1
    df_fea.ix[(df_fea['timeBrand_favor_num']==0)&(df_fea['timeBrand_buy_num']==0),'timeBrand_buyFav_ratio']=1
    df_fea.ix[(df_fea['timeBrand_click_num']==0)&(df_fea['timeBrand_buy_num']==0),'timeBrand_buyClick_radio']=1
    
    df_fea.fillna(value=0,inplace=True)
    return df_fea
    
#可以被调用的主函数
def theMain(df_inwin):
    print('brand')
    #对传进来的数据进行预处理-->清除time,model_id字段（因为我没有读入这两个字段，所以此步骤不需要）
    
    #初始的特征，因为该函数仅仅是计算独立的user特征，所以其初始特征可以就是user_id
    df_fea=df_inwin['brand'].to_frame().reset_index()
    df_fea=df_fea.drop_duplicates('brand')
    
    #统计特征、转换率特征
    df_fea=get_count_feacture(df_inwin, df_fea)

    
    
    #不用变换时间，因为时间的变换在上级函数中实现
    
    #将特征返回到上一级
    del df_fea['index']
    df_fea=df_fea.drop_duplicates('brand')
    print(df_fea.head(1))
    return df_fea

#主函数
if __name__=="__main__":
    needlist_ac=['user_id','sku_id','type','brand','cate','action_diff']
    df_acc=get_date_needlist(ACTION_TABLE, needlist_ac)
    theMain(df_acc)