'''
Created on 2017年5月3日
1.用户-商品的最近1,2,3,4次最近交互特征的时间 和 该时间内的点击数/购买数/收藏数/购物车数
2.用户第一次交互该商品和用户购买该商品之间的相差天数（缺失值-->11）
3.用户第一次购买该商品和下次购买该商品之间相差的天数（缺失值-->11）
4.用户最近3,6,10天内有多少天交互/收藏/加购物车了该商品，以及行为发生的数量
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


    
#1.用户-商品的最近1,2,3,4次最近交互特征的时间 和 该时间内的点击数/购买数/收藏数/购物车数
#智商有限===》好像实现不了==》===》用户-商品在1/2/3/5/7/10天内，行为数量统计 
#得到统计特征
def sever_lastIter_timeTypeNum(group):
    behavior_type = group.type.astype(int)
    type_cnt = Counter(behavior_type)

    group['time_win_user_browse_num'] = type_cnt[1]
    '''
    group['time_win_user_addcart_num'] = type_cnt[2]
    group['time_win_user_delcart_num'] = type_cnt[3]
    group['time_win_user_buy_num'] = type_cnt[4]
    group['time_win_user_favor_num'] = type_cnt[5]
    '''
    group['time_win_user_click_num'] = type_cnt[6]

    return group

def lastIter_timeTypeNum(df_win,df_fea):
    thelastDay=np.min(df_win['action_diff'])
    for day in [thelastDay+1,thelastDay+2]:#,thelastDay+3,thelastDay+5,thelastDay+7,thelastDay+10]:
        df_inday=df_win[df_win['action_diff']<day]
        item = df_inday.groupby(['user_id','sku_id'], as_index = False).apply(sever_lastIter_timeTypeNum)
        del item['brand']
        del item['cate']
        del item['action_diff']
        del item['type']
        '''
        item.columns=(['user_id','sku_id','cross_userPro_'+str(day-thelastDay)+'browseNum',\
                       'cross_userPro_'+str(day-thelastDay)+'addcarNum','cross_userPro_'+str(day-thelastDay)+'delcarNum',\
                       'cross_userPro_'+str(day-thelastDay)+'buyNum','cross_userPro_'+str(day-thelastDay)+'favNum',\
                       'cross_userPro_'+str(day-thelastDay)+'clickNum'])
                       '''
        item.columns=(['user_id','sku_id','cross_userPro_'+str(day-thelastDay)+'browseNum',\
                       'cross_userPro_'+str(day-thelastDay)+'clickNum'])
        item=item.drop_duplicates(['user_id','sku_id'])
        print(str(day))
        df_fea=df_fea.merge(item,on=['user_id','sku_id'],how='left')        
    df_fea=df_fea.fillna(0)
    return df_fea

#2.用户第一次交互该商品和用户第一次购买该商品之间的相差天数（缺失值-->11）
def interBuy_diffDay(df_win,df_fea):
    #得到交互时间
    item=df_win.groupby(['user_id','sku_id'],as_index=False)['action_diff'].apply(np.max)
    item=item.to_frame().reset_index()
    item.columns=(['user_id','sku_id','cross_userPro_firstinter_time'])
    #得到第一次购买时间
    df_buy=df_win[df_win['type']==4]
    item2=df_buy.groupby(['user_id','sku_id'],as_index=False)['action_diff'].apply(np.max)
    item2=item2.to_frame().reset_index()
    item2.columns=(['user_id','sku_id','cross_userPro_firstbuy_time'])
    #最后一次购买时间（防止出现负数）
    df_buy=df_win[df_win['type']==4]
    item3=df_buy.groupby(['user_id','sku_id'],as_index=False)['action_diff'].apply(np.min)
    item3=item3.to_frame().reset_index()
    item3.columns=(['user_id','sku_id','cross_userPro_lastbuy_time'])
    #关联起来得到差值，这时候可能出现为负数的情况或为空值的情况，这两种情况都置为11
    item=pd.merge(item,item2,on=['user_id','sku_id'],how='left')
    item=pd.merge(item,item3,on=['user_id','sku_id'],how='left')
    item['cross_userPro_iterBuy_diff']=item['cross_userPro_firstinter_time']-item['cross_userPro_firstbuy_time']
    item.ix[item['cross_userPro_iterBuy_diff']<0,'cross_userPro_iterBuy_diff']=item['cross_userPro_firstinter_time']-item['cross_userPro_lastbuy_time']
    del item['cross_userPro_firstinter_time']
    del item['cross_userPro_firstbuy_time']
    del item['cross_userPro_lastbuy_time']
    #(item['cross_userPro_iterBuy_diff']<0)|(item['cross_userPro_iterBuy_diff']==None),None不行
    item.ix[item['cross_userPro_iterBuy_diff']<0,'cross_userPro_iterBuy_diff']=11
    
    df_fea=df_fea.merge(item,on=['user_id','sku_id'],how='left')
    df_fea['cross_userPro_iterBuy_diff']=df_fea['cross_userPro_iterBuy_diff'].fillna(11)
    return df_fea 

#3.用户第一次购买该商品和下次购买该商品之间相差的天数（缺失值-->11）
def buyBuy_diffDay(df_win,df_fea):
    #得到第一次购买时间
    df_buy=df_win[df_win['type']==4]
    item2=df_buy.groupby(['user_id','sku_id'],as_index=False)['action_diff'].apply(np.max)
    item2=item2.to_frame().reset_index()
    item2.columns=(['user_id','sku_id','cross_userPro_firstbuy_time'])
    #最后一次购买时间（防止出现负数）
    df_buy=df_win[df_win['type']==4]
    item3=df_buy.groupby(['user_id','sku_id'],as_index=False)['action_diff'].apply(np.min)
    item3=item3.to_frame().reset_index()
    item3.columns=(['user_id','sku_id','cross_userPro_lastbuy_time'])
    #关联起来得到差值，这时候可能出现为负数的情况或为空值的情况，这两种情况都置为11
    item=pd.merge(item2,item3,on=['user_id','sku_id'],how='left')
    item['cross_userPro_buyBuy_diff']=item['cross_userPro_firstbuy_time']-item['cross_userPro_lastbuy_time']
    del item['cross_userPro_firstbuy_time']
    del item['cross_userPro_lastbuy_time']
    
    df_fea=df_fea.merge(item,on=['user_id','sku_id'],how='left')
    df_fea['cross_userPro_buyBuy_diff']=df_fea['cross_userPro_buyBuy_diff'].fillna(11)
    return df_fea 


#4.用户最近3,6,10天内有多少天交互/收藏/加购物车了该商品，以及行为发生的数量   
def dayNum_mostType(df_win,df_fea):
    thelastDay=np.min(df_win['action_diff'])
    df_win=df_win[['user_id','sku_id','type','action_diff']]
    for day in [3+thelastDay,6+thelastDay,10+thelastDay]:
        df_inday=df_win[df_win['action_diff']<day] #得到时间尺度内的数据
        #浏览的天数和数量
        df_type=df_inday[df_inday['type']==1]
        item=df_type.groupby(['user_id','sku_id'])['action_diff'].nunique().to_frame().reset_index()
        item.columns=['user_id','sku_id','cross_userPro_'+str(day-thelastDay)+'_browser_Daynum']
        df_fea=df_fea.merge(item,on=['user_id','sku_id'],how='left')
        item=df_type.groupby(['user_id','sku_id'])['action_diff'].count().to_frame().reset_index()
        item.columns=['user_id','sku_id','cross_userPro_'+str(day-thelastDay)+'_browser_num']
        df_fea=df_fea.merge(item,on=['user_id','sku_id'],how='left') 
        #print(str(day)+'browse end')
        #加入购物车的天数和数量
        df_type=df_inday[df_inday['type']==2]
        item=df_type.groupby(['user_id','sku_id'])['action_diff'].nunique().to_frame().reset_index()
        item.columns=['user_id','sku_id','cross_userPro_'+str(day-thelastDay)+'_addcar_Daynum']
        df_fea=df_fea.merge(item,on=['user_id','sku_id'],how='left')
        item=df_type.groupby(['user_id','sku_id'])['action_diff'].count().to_frame().reset_index()
        item.columns=['user_id','sku_id','cross_userPro_'+str(day-thelastDay)+'_addcar_num']
        df_fea=df_fea.merge(item,on=['user_id','sku_id'],how='left') 
        #print(str(day)+'addcar end')
        #下单的天数和数量
        df_type=df_inday[df_inday['type']==4]
        item=df_type.groupby(['user_id','sku_id'])['action_diff'].nunique().to_frame().reset_index()
        item.columns=['user_id','sku_id','cross_userPro_'+str(day-thelastDay)+'_buy_Daynum']
        df_fea=df_fea.merge(item,on=['user_id','sku_id'],how='left')
        item=df_type.groupby(['user_id','sku_id'])['action_diff'].count().to_frame().reset_index()
        item.columns=['user_id','sku_id','cross_userPro_'+str(day-thelastDay)+'_buy_num']
        df_fea=df_fea.merge(item,on=['user_id','sku_id'],how='left') 
        #print(str(day)+'buy end')
        #关注的天数和数量
        df_type=df_inday[df_inday['type']==5]
        item=df_type.groupby(['user_id','sku_id'])['action_diff'].nunique().to_frame().reset_index()
        item.columns=['user_id','sku_id','cross_userPro_'+str(day-thelastDay)+'_fav_Daynum']
        df_fea=df_fea.merge(item,on=['user_id','sku_id'],how='left')
        item=df_type.groupby(['user_id','sku_id'])['action_diff'].count().to_frame().reset_index()
        item.columns=['user_id','sku_id','cross_userPro_'+str(day-thelastDay)+'_fav_num']
        df_fea=df_fea.merge(item,on=['user_id','sku_id'],how='left') 
        #print(str(day)+'fav end')
        #点击的天数和数量         
        df_type=df_inday[df_inday['type']==6]
        item=df_type.groupby(['user_id','sku_id'])['action_diff'].nunique().to_frame().reset_index()
        item.columns=['user_id','sku_id','cross_userPro_'+str(day-thelastDay)+'_click_Daynum']
        df_fea=df_fea.merge(item,on=['user_id','sku_id'],how='left')
        item=df_type.groupby(['user_id','sku_id'])['action_diff'].count().to_frame().reset_index()
        item.columns=['user_id','sku_id','cross_userPro_'+str(day-thelastDay)+'_click_num']
        df_fea=df_fea.merge(item,on=['user_id','sku_id'],how='left') 
        #print(str(day)+'click end')
    df_fea=df_fea.fillna(0)
    return df_fea[['user_id','sku_id','cross_userPro_3_browser_num','cross_userPro_6_browser_num',\
                   'cross_userPro_6_click_num','cross_userPro_10_browser_Daynum',\
                   'cross_userPro_10_browser_num','cross_userPro_10_addcar_Daynum',\
                   'cross_userPro_10_fav_Daynum','cross_userPro_10_fav_num',\
                   'cross_userPro_10_click_Daynum','cross_userPro_10_click_num']]

#统计特征
#提取行为特征
def sever_countFeacture(group):
    behavior_type = group.type.astype(int)
    type_cnt = Counter(behavior_type)

    #group['cross_win_userPro_browse_num'] = type_cnt[1]
    #group['cross_win_userPro_addcart_num'] = type_cnt[2]
    group['cross_win_userPro_delcart_num'] = type_cnt[3]
    #group['cross_win_userPro_buy_num'] = type_cnt[4]
    group['cross_win_userPro_favor_num'] = type_cnt[5]
    group['cross_win_userPro_click_num'] = type_cnt[6]

    '''
    return group[['user_id','sku_id', 'cate', 'brand','model_id',
                  'browse_num', 'addcart_num','delcart_num', 'favor_num','click_num']]
    '''
    return group
def countFeacture(df_win,df_fea):
    df_inwin=df_win.drop(['brand','cate','action_diff'],axis=1)
    df_item = df_inwin.groupby(['user_id','sku_id'], as_index = False).apply(sever_countFeacture)
    del df_item['type']
    df_item=df_item.drop_duplicates(['user_id','sku_id'])
    df_fea=df_fea.merge(df_item,on=['user_id','sku_id'],how='left') 
    df_fea=df_fea.fillna(0)
    return df_fea

#可以被调用的主函数
def theMain(df_inwin):
    print('fea userPro')
    #对传进来的数据进行预处理-->清除time,model_id字段（因为我没有读入这两个字段，所以此步骤不需要）
    
    #初始的特征，因为该函数仅仅是计算独立的user特征，所以其初始特征可以就是user_id
    df_fea=df_inwin[['user_id','sku_id']]
    df_fea=df_fea.drop_duplicates(['user_id','sku_id'])
    
    #用户-商品的最近1,2,3,4次最近交互特征的时间 和 该时间内的点击数/购买数/收藏数/购物车数
    df_fea=lastIter_timeTypeNum(df_inwin, df_fea)
    #print(df_fea.shape[1])
    #.用户第一次交互该商品和用户第一次购买该商品之间的相差天数
    #df_fea=interBuy_diffDay(df_inwin, df_fea)
    #print(df_fea.shape[1])
    #用户第一次购买该商品和下次购买该商品之间相差的天数
    #df_fea=buyBuy_diffDay(df_inwin, df_fea)
    #print(df_fea.shape[1])
    #用户最近3,6,10天内有多少天交互/收藏/加购物车了该商品，以及行为发生的数量
    df_fea=dayNum_mostType(df_inwin, df_fea)
    #统计特征
    df_fea=countFeacture(df_inwin, df_fea)
    
    #不用变换时间，因为时间的变换在上级函数中实现
    
    #将特征返回到上一级
    #del df_fea['index']
    df_fea=df_fea.drop_duplicates(['user_id','sku_id'])
    print(df_fea.head(1))
    return df_fea

