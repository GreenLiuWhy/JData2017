'''
Created on 2017年5月4日
1.用户在交互该商品的时候，还和几个同种品类的商品做了交互
2.在5天，10天内存在几个同cate/brand里纯的商品交互日
3.用户与当前商品的交互的当天里一共点击/购买/收藏了哪些其他品牌
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

#1.用户在最近交互该商品的时候，还和几个同种品类的商品做了交互
def otherIterNum_sameCate(df_win,df_fea):
    df_win=df_win[['user_id','sku_id','cate','action_diff']]
    item=df_win.groupby(['user_id','sku_id','cate'])['action_diff'].apply(np.min).to_frame().reset_index()
    item.columns=(['user_id','sku_id','cate','lastIter_time'])
    for i in range(item.shape[0]):
        temp=df_win[(df_win['action_diff']==item.ix[i,'lastIter_time'])&(df_win['user_id']==item.ix[i,'user_id'])&(df_win['cate']==item.ix[i,'cate'])]
        item.ix[i,'cross_proPro_lastInterOtherNum_sameCate']=temp['sku_id'].nunique()-1
    #print(item.head(5))
    del item['cate']
    del item['lastIter_time']
    df_fea=pd.merge(df_fea,item,on=['user_id','sku_id'],how='left')
    df_fea.fillna(value=0,inplace=True)
    return df_fea

#1.2用户在最近交互该商品的时候，还和几个同种品牌的商品做了交互
def otherIterNum_sameBrand(df_win,df_fea):
    df_win=df_win[['user_id','sku_id','brand','action_diff']]
    item=df_win.groupby(['user_id','sku_id','brand'])['action_diff'].apply(np.min).to_frame().reset_index()
    item.columns=(['user_id','sku_id','brand','lastIter_time'])
    for i in range(item.shape[0]):
        temp=df_win[(df_win['action_diff']==item.ix[i,'lastIter_time'])&(df_win['user_id']==item.ix[i,'user_id'])&(df_win['brand']==item.ix[i,'brand'])]
        item.ix[i,'cross_proPro_lastInterOtherNum_sameBrand']=temp['sku_id'].nunique()-1
    #print(item.head(5))
    del item['brand']
    del item['lastIter_time']
    df_fea=pd.merge(df_fea,item,on=['user_id','sku_id'],how='left')
    df_fea.fillna(value=0,inplace=True)
    return df_fea

#2.在5天，10天内存在几个同cate里纯的商品交互日
def onlyIter_sameCate(df_win,df_fea):
    thelastDay=np.min(df_win['action_diff'])
    #allday=list(np.ones(10)*thelastDay+np.array([0,1,2,3,4,5,6,7,8,9]))
    for day in [thelastDay+5,thelastDay+10]:
        df_inday=df_win[df_win['action_diff']<day]
        item=df_inday.groupby(['user_id','cate','action_diff'])['sku_id'].nunique().to_frame().reset_index()
        item.columns=(['user_id','cate','action_diff','pro_num'])
        item=item[item['pro_num']==1] #得到纯交互的信息
        infor=pd.merge(df_inday,item,on=['user_id','cate','action_diff'],how='inner').drop_duplicates(['user_id','cate','action_diff'])
        infor.drop(['type','brand','cate','pro_num'],axis=1,inplace=True)
        item=infor.groupby(['user_id','sku_id'])['action_diff'].nunique().to_frame().reset_index()
        item.columns=(['user_id','sku_id','cross_proPro_'+str(day-thelastDay)+'day_onlyDayNum'])
        df_fea=pd.merge(df_fea,item,on=['user_id','sku_id'],how='left')
    df_fea.fillna(value=0,inplace=True)
    return df_fea

#3.用户与当前商品的最近第1、2、3次交互的当天里一共点击/购买/收藏其他同cate商品的数量
def thelast3Day_typeNum_sameCate(df_win,df_fea):
    return df_fea
    
#可以被调用的主函数
def theMain(df_inwin):
    print('fea proPro')
    #对传进来的数据进行预处理-->清除time,model_id字段（因为我没有读入这两个字段，所以此步骤不需要）
    
    #初始的特征，因为该函数仅仅是计算独立的user特征，所以其初始特征可以就是user_id
    df_fea=df_inwin[['user_id','sku_id']]
    df_fea=df_fea.drop_duplicates(['user_id','sku_id'])
    
    #1.用户在交互该商品的时候，还和几个同种品类的商品做了交互
    df_fea=otherIterNum_sameCate(df_inwin, df_fea)
    #1.2用户在交互该商品的时候，还和几个同种品牌的商品做了交互
    df_fea=otherIterNum_sameBrand(df_inwin, df_fea)
    #2.在5天，10天内存在几个同cate/brand里纯的商品交互日
    df_fea=onlyIter_sameCate(df_inwin, df_fea)
    #3.用户与当前商品的最近第1、2、3次交互的当天里一共点击/购买/收藏其他同cate商品的数量
    
    #不用变换时间，因为时间的变换在上级函数中实现
    
    #将特征返回到上一级
    #del df_fea['index']
    df_fea=df_fea.drop_duplicates(['user_id','sku_id'])
    print(df_fea.head(1))
    return df_fea

#主函数
if __name__=="__main__":
    needlist_ac=['user_id','sku_id','type','brand','cate','action_diff']
    df_acc=get_date_needlist(ACTION_TABLE, needlist_ac)
    theMain(df_acc)