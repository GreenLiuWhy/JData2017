'''
Created on 2017年4月21日
里面的函数主要是产生和user有关的特征，在测试完成之后将
main函数
文件路径
删除，所有的函数都被通过调用来实现，转入的是在window里的action信息，返回的是user_id+与user相关的特征

主要是仿照天猫的2014年清水湾的什么什么的“当前用户总体特征”
1.用户最近7/15/30天有多少天有交互/购买任意品牌
2.用户最近一次/第一次交互/购买任意品牌的时间
3.用户最近1/3/7/10/15/30天购买/点击所有品牌的总次数
4.用户最近1/3/7/10/15/30天购买/点击过的不同品牌的个数
@author: liu
'''

import pandas as pd
import numpy as np
from collections import Counter
# USER_TABLE="E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_choosed\\Small_User.csv"
# TEN_ACTION="E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_choosed\\Ten_Action.csv"
USER_FEACTURE="E:\\ProgrameGra2\\Python\\MyJDateVer1\\feactures\\user_feacture.csv"
ACTION_TABLE="E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_choosed\\Small_Action.csv"


#此函数在小数据测试阶段没有用，但是为了程序直接可以移植用于大数据，所以还是先用用它
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
    df_ac=df_ac.drop_duplicates() #所以原来的程序这里是不对的，应该是所有的都相同才删除吧
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

# #因为通过滑动窗口提取信息的时候action_diff应该是也不是原来的天数的
# def get_diffDay_baseOnSlidingWindow(diffDay):
#     return diffDay-text_start_time #就是返回了一个1~10的数   

#用户最近7/15/30天有多少天有交互/购买任意品牌---》缺失值为0
def get_daysNum_in_threeDayCoarseGrained_interactive_buy(df_inwindow,df_feactures):
    thelastDay=np.min(df_inwindow['action_diff'])
    #要把acWin_diff的特征也添加进去
    for day in [thelastDay+3,thelastDay+5,thelastDay+10]:
        #得到这几天内的数据
        df_ac_day=df_inwindow[df_inwindow["action_diff"]<day]
        #因为是统计的天数，所以同一个用户在同一天里都进行的操作就可以不管
        df_ac_day=df_ac_day.drop_duplicates(['user_id','type','action_diff'])
        #交互的天数
        df_item=df_ac_day.groupby('user_id')['action_diff'].nunique() #得到user_id---daynum
        df_item = df_item.to_frame().reset_index()#转化为dataframe
        df_item.columns=['user_id','user_'+str(day-thelastDay)+'_inter_num']
        df_feactures=df_feactures.merge(df_item,on='user_id',how='left')
        #浏览的天数
        df_bro=df_ac_day[df_ac_day['type']==1]
        df_item=df_bro.groupby('user_id')['action_diff'].nunique().to_frame().reset_index()
        df_item.columns=['user_id','user_'+str(day-thelastDay)+'_browse_num']
        df_feactures=df_feactures.merge(df_item,on='user_id',how='left')
        #加入购物车的天数
        df_car=df_ac_day[df_ac_day['type']==2]
        df_item=df_car.groupby('user_id')['action_diff'].nunique().to_frame().reset_index()
        df_item.columns=['user_id','user_'+str(day-thelastDay)+'_addcar_num']
        df_feactures=df_feactures.merge(df_item,on='user_id',how='left') 
        #得到这几内有购买的数据
        df_buy=df_ac_day[df_ac_day['type']==4]
        df_item=df_buy.groupby('user_id')['action_diff'].nunique().to_frame().reset_index()
        df_item.columns=['user_id','user_'+str(day-thelastDay)+'_buy_num']
        df_feactures=df_feactures.merge(df_item,on='user_id',how='left') 
        #关注的天数
        df_fav=df_ac_day[df_ac_day['type']==5]
        df_item=df_fav.groupby('user_id')['action_diff'].nunique().to_frame().reset_index()
        df_item.columns=['user_id','user_'+str(day-thelastDay)+'_fav_num']
        df_feactures=df_feactures.merge(df_item,on='user_id',how='left') 
        #点击的天数
        df_click=df_ac_day[df_ac_day['type']==6]
        df_item=df_click.groupby('user_id')['action_diff'].nunique().to_frame().reset_index()
        df_item.columns=['user_id','user_'+str(day-thelastDay)+'_click_num']
        df_feactures=df_feactures.merge(df_item,on='user_id',how='left') 
        
    df_feactures.fillna(value=0,inplace=True)    
    return df_feactures
        
#用户最近一次/第一次交互/购买任意品牌的时间
def  get_lastFirst_interactive_buy_time(df_inwindow,df_feactures):     
    #用户最近一次交互时间
    thelastInter=df_inwindow.groupby("user_id")["action_diff"].apply(np.min)
    thelastInter=thelastInter.to_frame().reset_index()
    thelastInter.columns=['user_id',"user_last_inter_time"]
    df_feactures=df_feactures.merge(thelastInter,on='user_id',how='left')
        
    #用户最近一次购买时间
    df_buy=df_inwindow[df_inwindow['type']==4]
    thelastInter=df_buy.groupby("user_id")["action_diff"].apply(np.min)
    thelastInter=thelastInter.to_frame().reset_index()
    thelastInter.columns=['user_id',"user_last_buy_time"]
    df_feactures=df_feactures.merge(thelastInter,on='user_id',how='left')
    
    '''  
    #用户第一次交互时间
    thefirstInter=df_inwindow.groupby("user_id")["action_diff"].apply(np.max)
    thefirstInter=thefirstInter.to_frame().reset_index()
    thefirstInter.columns=['user_id',"user_first_inter_time"]
    df_feactures=df_feactures.merge(thelastInter,on='user_id',how='left')
    #用户第一次购买时间
    thefirstInter=df_buy.groupby("user_id")["action_diff"].apply(np.max)
    thefirstInter=thefirstInter.to_frame().reset_index()
    thefirstInter.columns=['user_id',"user_first_buy_time"]
    df_feactures=df_feactures.merge(thelastInter,on='user_id',how='left')
    '''
    df_feactures.fillna(value=11,inplace=True)
    return df_feactures

     
#.用户最近1、3、7、10天购买/浏览/加购物车/收藏任意商品的总数
#与下面函数区别就是这个统计个数的时候不去重复
def get_alltype_productnum_diffCoarseGrained(df_inwindow,df_feactures):
    thelastDay=np.min(df_inwindow['action_diff'])
    #循环天数
    for day in [thelastDay+1,thelastDay+3,thelastDay+6,thelastDay+10]:
        df_inday=df_inwindow[df_inwindow['action_diff']<day]
        #浏览任意商品的总数
        df_bro=df_inday[df_inday['type']==1]
        buySer=df_bro.groupby(df_bro['user_id'])['sku_id'].count().to_frame().reset_index()
        buySer.columns=['user_id','user_'+str(day-thelastDay)+'_broProduct_num']
        df_feactures=df_feactures.merge(buySer,on='user_id',how='left')
        #加入购物车任意商品总数
        df_car=df_inday[df_inday['type']==2]
        buySer=df_car.groupby(df_car['user_id'])['sku_id'].count().to_frame().reset_index()
        buySer.columns=['user_id','user_'+str(day-thelastDay)+'_addcarProduct_num']
        df_feactures=df_feactures.merge(buySer,on='user_id',how='left')
        #购买任意商品的总数
        df_buy=df_inday[df_inday['type']==4]
        buySer=df_buy.groupby(df_buy['user_id'])['sku_id'].count().to_frame().reset_index()
        buySer.columns=['user_id','user_'+str(day-thelastDay)+'_buyProduct_num']
        df_feactures=df_feactures.merge(buySer,on='user_id',how='left')
        #关注任意商品总数
        df_buy=df_inday[df_inday['type']==5]
        buySer=df_buy.groupby(df_buy['user_id'])['sku_id'].count().to_frame().reset_index()
        buySer.columns=['user_id','user_'+str(day-thelastDay)+'_favProduct_num']
        df_feactures=df_feactures.merge(buySer,on='user_id',how='left')
        #得到点击的品牌数
        df_buy=df_inday[df_inday['type']==6]
        buySer=df_buy.groupby(df_buy['user_id'])['sku_id'].count().to_frame().reset_index()
        buySer.columns=['user_id','user_'+str(day-thelastDay)+'_clickProduct_num']
        df_feactures=df_feactures.merge(buySer,on='user_id',how='left')
    df_feactures.fillna(value=0,inplace=True)
    return df_feactures

#用户最近1、3、7、10天购买/浏览/加购物车/收藏过的不同商品个数
#与上面函数不同点就是，这个函数在计数的时候去重
def get_productNum_alltype_diffCoarseGrained(df_inwindow,df_feactures):
    '''
     #得到在此时间窗口中的数据
    idxwindow=df_ac['action_diff']<=train_start_time
    df_inwindow=df_ac[idxwindow]
    idxwindow=df_ac['action_diff']>train_end_time
    df_inwindow=df_inwindow[idxwindow]
    #对在窗口中数据的action_diff的时间重新规划为1~10
    df_inwindow['acWin_diff']=df_inwindow['action_diff'].map(get_diffDay_baseOnSlidingWindow)
    '''
    thelastDay=np.min(df_inwindow['action_diff'])
    #循环天数
    for day in [thelastDay+1,thelastDay+3,thelastDay+6,thelastDay+10]:
        df_inday=df_inwindow[df_inwindow['action_diff']<day]
        #浏览任意商品的总数
        df_bro=df_inday[df_inday['type']==1]
        buySer=df_bro.groupby(df_bro['user_id'])['sku_id'].nunique().to_frame().reset_index()
        buySer.columns=['user_id','user_'+str(day-thelastDay)+'_brodiffProduct_num']
        df_feactures=df_feactures.merge(buySer,on='user_id',how='left')
        #加入购物车任意商品总数
        df_car=df_inday[df_inday['type']==2]
        buySer=df_car.groupby(df_car['user_id'])['sku_id'].nunique().to_frame().reset_index()
        buySer.columns=['user_id','user_'+str(day-thelastDay)+'_addcardiffProduct_num']
        df_feactures=df_feactures.merge(buySer,on='user_id',how='left')
        #购买任意商品的总数
        df_buy=df_inday[df_inday['type']==4]
        buySer=df_buy.groupby(df_buy['user_id'])['sku_id'].nunique().to_frame().reset_index()
        buySer.columns=['user_id','user_'+str(day-thelastDay)+'_buydiffProduct_num']
        df_feactures=df_feactures.merge(buySer,on='user_id',how='left')
        #关注任意商品总数
        df_buy=df_inday[df_inday['type']==5]
        buySer=df_buy.groupby(df_buy['user_id'])['sku_id'].nunique().to_frame().reset_index()
        buySer.columns=['user_id','user_'+str(day-thelastDay)+'_favdiffProduct_num']
        df_feactures=df_feactures.merge(buySer,on='user_id',how='left')
        #得到点击的品牌数
        df_buy=df_inday[df_inday['type']==6]
        buySer=df_buy.groupby(df_buy['user_id'])['sku_id'].nunique().to_frame().reset_index()
        buySer.columns=['user_id','user_'+str(day-thelastDay)+'_clickdiffProduct_num']
        df_feactures=df_feactures.merge(buySer,on='user_id',how='left')   
    df_feactures.fillna(value=0,inplace=True)
    return df_feactures

#用户最近1、3、7、10天购买/浏览/加购物车/收藏过的不同商品个数
#与上面函数不同点就是，这个函数在计数的时候去重
def get_brandNum_alltype_diffCoarseGrained(df_inwindow,df_feactures):
    thelastDay=np.min(df_inwindow['action_diff'])
    #循环天数
    for day in [thelastDay+1,thelastDay+3,thelastDay+6,thelastDay+10]:
        df_inday=df_inwindow[df_inwindow['action_diff']<day]
        #浏览任意商品的总数
        df_bro=df_inday[df_inday['type']==1]
        buySer=df_bro.groupby(df_bro['user_id'])['brand'].nunique().to_frame().reset_index()
        buySer.columns=['user_id','user_'+str(day-thelastDay)+'_brodiffBrand_num']
        df_feactures=df_feactures.merge(buySer,on='user_id',how='left')
        #加入购物车任意商品总数
        df_car=df_inday[df_inday['type']==2]
        buySer=df_car.groupby(df_car['user_id'])['brand'].nunique().to_frame().reset_index()
        buySer.columns=['user_id','user_'+str(day-thelastDay)+'_addcardiffBrand_num']
        df_feactures=df_feactures.merge(buySer,on='user_id',how='left')
        #购买任意商品的总数
        df_buy=df_inday[df_inday['type']==4]
        buySer=df_buy.groupby(df_buy['user_id'])['brand'].nunique().to_frame().reset_index()
        buySer.columns=['user_id','user_'+str(day-thelastDay)+'_buydiffBrand_num']
        df_feactures=df_feactures.merge(buySer,on='user_id',how='left')
        #关注任意商品总数
        df_buy=df_inday[df_inday['type']==5]
        buySer=df_buy.groupby(df_buy['user_id'])['brand'].nunique().to_frame().reset_index()
        buySer.columns=['user_id','user_'+str(day-thelastDay)+'_favdiffBrand_num']
        df_feactures=df_feactures.merge(buySer,on='user_id',how='left')
        #得到点击的品牌数
        df_buy=df_inday[df_inday['type']==6]
        buySer=df_buy.groupby(df_buy['user_id'])['brand'].nunique().to_frame().reset_index()
        buySer.columns=['user_id','user_'+str(day-thelastDay)+'_clickdiffBrand_num']
        df_feactures=df_feactures.merge(buySer,on='user_id',how='left')  
    df_feactures.fillna(value=0,inplace=True) 
    return df_feactures

#得到统计特征
def sever_getFeatures_user(group):
    behavior_type = group.type.astype(int)
    type_cnt = Counter(behavior_type)

    group['time_win_user_browse_num'] = type_cnt[1]
    group['time_win_user_addcart_num'] = type_cnt[2]
    group['time_win_user_delcart_num'] = type_cnt[3]
    group['time_win_user_favor_num'] = type_cnt[5]
    group['time_win_user_click_num'] = type_cnt[6]

    '''
    return group[['user_id','sku_id', 'cate', 'brand','model_id',
                  'browse_num', 'addcart_num','delcart_num', 'favor_num','click_num']]
    '''
    return group

def get_countFeactures(df_inwin,df_fea):
    df_inwin=df_inwin.drop(['sku_id','brand','cate','action_diff'],axis=1)
    df_item = df_inwin.groupby(['user_id'], as_index = False).apply(sever_getFeatures_user)
    del df_item['type']
    df_item=df_item.drop_duplicates('user_id')
    df_fea=df_fea.merge(df_item,on='user_id',how='left') 
    df_fea.fillna(value=0,inplace=True)
    return df_fea

#被调用的主函数，主要用来进行一些数据预处理和使流程清晰化
#通过该函数，最终return一个 【user_id ， 用户特征 】 的dataframe用于和总的特征相互连接
def theMain(df_inwindow_ac):
    print('fea user')
    #对传进来的数据进行预处理-->清除time,model_id字段（因为我没有读入这两个字段，所以此步骤不需要）
    
    #初始的特征，因为该函数仅仅是计算独立的user特征，所以其初始特征可以就是user_id
    df_fea=df_inwindow_ac['user_id'].to_frame().reset_index()
    df_fea=df_fea.drop_duplicates('user_id')
    
    #用户最近3/5/10天有多少天交互/浏览/加购物车/下单/关注/点击任意商品
    df_fea=get_daysNum_in_threeDayCoarseGrained_interactive_buy(df_inwindow_ac,df_fea)
    #用户最近一次交互和购买品牌的时间
    df_fea=get_lastFirst_interactive_buy_time(df_inwindow_ac, df_fea)
    #用户最近1/3/6/10天内浏览/...任意商品的总数
    df_fea=get_alltype_productnum_diffCoarseGrained(df_inwindow_ac, df_fea)
    #用户最近1/3/6/10天内浏览/...过不同商品的个数
    df_fea=get_productNum_alltype_diffCoarseGrained(df_inwindow_ac, df_fea)
    #用户最近1/3/6/10天内浏览/...过不同品牌的个数
    df_fea=get_brandNum_alltype_diffCoarseGrained(df_inwindow_ac, df_fea)
    #得到统计特征
    df_fea=get_countFeactures(df_inwindow_ac, df_fea)
    
    
    #不用变换时间，因为时间的变换在上级函数中实现
    
    #将特征返回到上一级
    del df_fea['index']
    df_fea=df_fea.drop_duplicates('user_id')
    print(df_fea.head(1))
    return df_fea
    
#主函数
if __name__=="__main__":
    needlist_ac=['user_id','sku_id','type','brand','cate','action_diff']
    df_ac=get_date_needlist(ACTION_TABLE, needlist_ac)
    theMain(df_ac)


    
