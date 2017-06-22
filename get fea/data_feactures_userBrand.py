'''
Created on 2017年5月3日
1.用户-品牌在1/2/3/5/7/10天内的行为数量统计
2..用户最近3,6,10天内有多少天交互/收藏/加购物车了该品牌，以及行为发生的数量
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
        item = df_inday.groupby(['user_id','brand'], as_index = False).apply(sever_lastIter_timeTypeNum)
        del item['sku_id']
        del item['cate']
        del item['action_diff']
        del item['type']
        '''
        item.columns=(['user_id','brand','cross_userBrand_'+str(day-thelastDay)+'browseNum',\
                       'cross_userBrand_'+str(day-thelastDay)+'addcarNum','cross_userBrand_'+str(day-thelastDay)+'delcarNum',\
                       'cross_userBrand_'+str(day-thelastDay)+'buyNum','cross_userBrand_'+str(day-thelastDay)+'favNum',\
                       'cross_userBrand_'+str(day-thelastDay)+'clickNum'])
                       '''
        item.columns=(['user_id','brand','cross_userBrand_'+str(day-thelastDay)+'browseNum',\
                       'cross_userBrand_'+str(day-thelastDay)+'clickNum'])
        item=item.drop_duplicates(['user_id','brand'])
        #print(str(day))
        df_fea=df_fea.merge(item,on=['user_id','brand'],how='left')        
    df_fea=df_fea.fillna(0)
    return df_fea


#4.用户最近3,6,10天内有多少天交互/收藏/加购物车了该商品，以及行为发生的数量   
def dayNum_mostType(df_win,df_fea):
    thelastDay=np.min(df_win['action_diff'])
    df_win=df_win[['user_id','brand','type','action_diff']]
    for day in [3+thelastDay,6+thelastDay,10+thelastDay]:
        df_inday=df_win[df_win['action_diff']<day] #得到时间尺度内的数据
        #浏览的天数和数量
        df_type=df_inday[df_inday['type']==1]
        item=df_type.groupby(['user_id','brand'])['action_diff'].nunique().to_frame().reset_index()
        item.columns=['user_id','brand','cross_userBrand_'+str(day-thelastDay)+'_browser_Daynum']
        df_fea=df_fea.merge(item,on=['user_id','brand'],how='left')
        item=df_type.groupby(['user_id','brand'])['action_diff'].count().to_frame().reset_index()
        item.columns=['user_id','brand','cross_userBrand_'+str(day-thelastDay)+'_browser_num']
        df_fea=df_fea.merge(item,on=['user_id','brand'],how='left') 
        #print(str(day)+'browse end')
        #加入购物车的天数和数量
        df_type=df_inday[df_inday['type']==2]
        item=df_type.groupby(['user_id','brand'])['action_diff'].nunique().to_frame().reset_index()
        item.columns=['user_id','brand','cross_userBrand_'+str(day-thelastDay)+'_addcar_Daynum']
        df_fea=df_fea.merge(item,on=['user_id','brand'],how='left')
        item=df_type.groupby(['user_id','brand'])['action_diff'].count().to_frame().reset_index()
        item.columns=['user_id','brand','cross_userBrand_'+str(day-thelastDay)+'_addcar_num']
        df_fea=df_fea.merge(item,on=['user_id','brand'],how='left') 
        #print(str(day)+'addcar end')
        #下单的天数和数量
        df_type=df_inday[df_inday['type']==4]
        item=df_type.groupby(['user_id','brand'])['action_diff'].nunique().to_frame().reset_index()
        item.columns=['user_id','brand','cross_userBrand_'+str(day-thelastDay)+'_buy_Daynum']
        df_fea=df_fea.merge(item,on=['user_id','brand'],how='left')
        item=df_type.groupby(['user_id','brand'])['action_diff'].count().to_frame().reset_index()
        item.columns=['user_id','brand','cross_userBrand_'+str(day-thelastDay)+'_buy_num']
        df_fea=df_fea.merge(item,on=['user_id','brand'],how='left') 
        #print(str(day)+'buy end')
        #关注的天数和数量
        df_type=df_inday[df_inday['type']==5]
        item=df_type.groupby(['user_id','brand'])['action_diff'].nunique().to_frame().reset_index()
        item.columns=['user_id','brand','cross_userBrand_'+str(day-thelastDay)+'_fav_Daynum']
        df_fea=df_fea.merge(item,on=['user_id','brand'],how='left')
        item=df_type.groupby(['user_id','brand'])['action_diff'].count().to_frame().reset_index()
        item.columns=['user_id','brand','cross_userBrand_'+str(day-thelastDay)+'_fav_num']
        df_fea=df_fea.merge(item,on=['user_id','brand'],how='left') 
        #print(str(day)+'fav end')
        #点击的天数和数量         
        df_type=df_inday[df_inday['type']==6]
        item=df_type.groupby(['user_id','brand'])['action_diff'].nunique().to_frame().reset_index()
        item.columns=['user_id','brand','cross_userBrand_'+str(day-thelastDay)+'_click_Daynum']
        df_fea=df_fea.merge(item,on=['user_id','brand'],how='left')
        item=df_type.groupby(['user_id','brand'])['action_diff'].count().to_frame().reset_index()
        item.columns=['user_id','brand','cross_userBrand_'+str(day-thelastDay)+'_click_num']
        df_fea=df_fea.merge(item,on=['user_id','brand'],how='left') 
        #print(str(day)+'click end')
    df_fea=df_fea.fillna(0)
    print(df_fea.head(2))
    return df_fea[['user_id','brand','cross_userBrand_3_browser_num','cross_userBrand_3_click_num',\
                   'cross_userBrand_6_browser_num','cross_userBrand_6_click_num','cross_userBrand_10_addcar_num',\
                   'cross_userBrand_10_browser_num','cross_userBrand_10_addcar_Daynum',\
                   'cross_userBrand_10_buy_Daynum','cross_userBrand_10_fav_num','cross_userBrand_10_buy_num',\
                   'cross_userBrand_10_click_num']]


#可以被调用的主函数
def theMain(df_inwin):
    print('fea userBrand')
    #对传进来的数据进行预处理-->清除time,model_id字段（因为我没有读入这两个字段，所以此步骤不需要）
    
    #初始的特征，因为该函数仅仅是计算独立的user特征，所以其初始特征可以就是user_id
    df_fea=df_inwin[['user_id','brand']]
    df_fea=df_fea.drop_duplicates(['user_id','brand'])
    #用户-商品的最近1,2,3,4次最近交互特征的时间 和 该时间内的点击数/购买数/收藏数/购物车数
    df_fea=lastIter_timeTypeNum(df_inwin, df_fea)
    #用户最近3,6,10天内有多少天交互/收藏/加购物车了该商品，以及行为发生的数量
    df_fea=dayNum_mostType(df_inwin, df_fea)
    
    #不用变换时间，因为时间的变换在上级函数中实现
    
    #将特征返回到上一级
    #del df_fea['index']
    df_fea=df_fea.drop_duplicates(['user_id','brand'])
    print(df_fea.head(1))
    return df_fea

