'''
Created on 2017年5月3日
里面的函数主要是产生和product有关的特征，在测试完成之后将
main函数
文件路径
删除，所有的函数都被通过调用来实现，转入的是在window里的action信息，返回的是sku_id+与product相关的特征

目前实现的特征有：
1.商品被重复购买的次数
2.时间尺度的转化率特征
@author: liu
'''
import pandas as pd
import numpy as np
from collections import Counter


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
   
    group['timePro_browse_num'] = type_cnt[1]
    group['timePro_addcart_num'] = type_cnt[2]
    group['timePro_delcart_num'] = type_cnt[3]
    group['timePro_buy_num'] = type_cnt[4]
    group['timePro_favor_num'] = type_cnt[5]
    group['timePro_click_num'] = type_cnt[6]

    return group

#时间尺度的统计特征
def get_count_feacture(df_inwin,df_fea):
    #求统计
    df_inwin=df_inwin.drop(['user_id','brand','cate','action_diff'],axis=1)
    df_item = df_inwin.groupby(['sku_id'], as_index = False).apply(sever_count_feacture)
    del df_item['type']
    df_item=df_item.drop_duplicates('sku_id')
    df_fea=df_fea.merge(df_item,on='sku_id',how='left') 
    #求比例
    df_fea['timePro_buyBrowse_ratio']=df_fea['timePro_buy_num']/df_fea['timePro_browse_num']
    '''
    df_fea['timePro_buyAddcar_ratio']=df_fea['timePro_buy_num']/df_fea['timePro_addcart_num']
    df_fea['timePro_buyFav_ratio']=df_fea['timePro_buy_num']/df_fea['timePro_favor_num']
    df_fea['timePro_buyClick_radio']=df_fea['timePro_buy_num']/df_fea['timePro_click_num']
    '''
    #当被除数为0，除数不为零时， 零
    df_fea.ix[(df_fea['timePro_browse_num']==0)&(df_fea['timePro_buy_num']!=0),'timePro_buyBrowse_ratio']=0 #ix【行，列】
    '''
    df_fea.ix[(df_fea['timePro_addcart_num']==0)&(df_fea['timePro_buy_num']!=0),'timePro_buyAddcar_ratio']=0
    df_fea.ix[(df_fea['timePro_favor_num']==0)&(df_fea['timePro_buy_num']!=0),'timePro_buyFav_ratio']=0
    df_fea.ix[(df_fea['timePro_click_num']==0)&(df_fea['timePro_buy_num']!=0),'timePro_buyClick_radio']=0
    '''
    #当被除数除数都为0，  一
    df_fea.ix[(df_fea['timePro_browse_num']==0)&(df_fea['timePro_buy_num']==0),'timePro_buyBrowse_ratio']=1 #ix【行，列】
    '''
    df_fea.ix[(df_fea['timePro_addcart_num']==0)&(df_fea['timePro_buy_num']==0),'timePro_buyAddcar_ratio']=1
    df_fea.ix[(df_fea['timePro_favor_num']==0)&(df_fea['timePro_buy_num']==0),'timePro_buyFav_ratio']=1
    df_fea.ix[(df_fea['timePro_click_num']==0)&(df_fea['timePro_buy_num']==0),'timePro_buyClick_radio']=1
    '''
    
    df_fea.fillna(value=0,inplace=True)
    return df_fea[['sku_id','timePro_click_num','timePro_buyBrowse_ratio']]

#商品被重复购买的次数==被购买的总次数-被多少用户购买
def get_buyNum_repeatBuy(df_inwin,df_fea): 
    df_buy=df_inwin[df_inwin['type']==4]
    #一共被买了多少次
    buyNum=df_buy.groupby(df_buy['sku_id'])['user_id'].count().to_frame().reset_index()
    buyNum.columns=(['sku_id','buyNum'])
    buyNum=buyNum.drop_duplicates('sku_id')
    #一共被多少用户购买  
    buyUser=df_buy.groupby(df_buy['sku_id'])['user_id'].nunique().to_frame().reset_index()
    buyUser.columns=(['sku_id','buyUser'])
    buyUser=buyUser.drop_duplicates('sku_id')
    #连接
    df_item=pd.merge(buyNum,buyUser,on='sku_id',how='inner')
    df_item['pro_repeatBuyNum']=df_item['buyNum']-df_item['buyUser']
    del df_item['buyNum']
    del df_item['buyUser']
    df_item=df_item.drop_duplicates('sku_id')
    df_fea=df_fea.merge(df_item,on='sku_id',how='left')   
    #因为有的商品没有被购买过，所以重复购买次数会出现空值，将空值填补为0
    df_fea.fillna(value=0,inplace=True)
    return df_fea
    
#可以被调用的主函数
def theMain(df_inwin):
    print('fea product')
    #对传进来的数据进行预处理-->清除time,model_id字段（因为我没有读入这两个字段，所以此步骤不需要）
    
    #初始的特征，因为该函数仅仅是计算独立的user特征，所以其初始特征可以就是user_id
    df_fea=df_inwin['sku_id'].to_frame().reset_index()
    df_fea=df_fea.drop_duplicates('sku_id')
    
    #统计特征、转换率特征
    df_fea=get_count_feacture(df_inwin, df_fea)
    #商品被重复购买的次数
    #df_fea=get_buyNum_repeatBuy(df_inwin, df_fea)
    
    
    #不用变换时间，因为时间的变换在上级函数中实现
    
    #将特征返回到上一级
    #del df_fea['index']
    df_fea=df_fea.drop_duplicates('sku_id')
    return df_fea


