'''
Created on 2017年4月21日
主要是做数据清理工作：
1.清除无收藏、购物车、购买行为的用户
2.浏览数过多但没有购买的用户
3.对商品子集无收藏、购物车、购买行为的用户
4.浏览数/购买数 比例过大的用户（经验值是》500）这个可以画图看看--->在程序中用的700
5.只有购买记录，没有其他交互记录的用户
6.最后10天没有任何交互记录的用户
7.在预测时间前2天对要测试的product集里的产品有过购买的用户
8.model_id没怎么懂，以后处理
9.完全重复的记录（这个在自己程序的上一步已经解决了）
10.将没有任何action行为的product里的产品去掉
这个9有个buge，时间是精确到分的，而在一分钟以内用户可以进行多个行为，所以不去重------------>

cpu：i5-7300HQ(4 core)
ram:8G
time:3min
complete:1,2,3,4,5,6,9
problem 1:那几个阈值是自己觉得的，没有依据，可能会出现清理不到位，或者是清理过度的情况
problem 2:其实功能4进行的不到位，没有考虑子集，但功能6其实已经等于将原来功能4完成的功能完成一大部分了

@author: liu
'''
import pandas as pd

# ACTION_TABLE="E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_choosed\\Small_Action.csv"
# USER_TABLE="E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_choosed\\Small_User.csv"

ALL_ACTION_TABLE="E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran\\JData_Tran_Action.csv"
ALL_USER_TABLE="E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran\\JData_ratio_User.csv"
# 
# CLEAN_RESULT="E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran\\JData_clean_ratio_User.csv"
# CLEAN_RESULT_NONTEN="E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran\\JData_clean_ratio_User_noten.csv"
# PRODUCT_ACTION="E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran\\JData_inter_product.csv"


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
    #df_ac=df_ac.drop_duplicates() #所以原来的程序这里是不对的，应该是所有的都相同才删除吧
    return df_ac
        
# #清除对商品子集无收藏、购物车、购买行为的用户
def clean_product_non_car_buy_fav_user(df_user):
    df_user=df_user[(df_user['user_fav_num']>0)|(df_user['user_addcar_num']>0)|(df_user['user_buy_num']>0)]
    return df_user
    
    
# #清除浏览量过多但没有购买4的用户
def clean_brower_much_non_nuy_user(df_user):
    threshold=700
    df_user=df_user[(df_user['user_buy_num']>0)|(df_user['user_browse_num']<threshold)]
    return df_user

# #清除浏览数/购买数比例过大的用户    
def clean_browseDivBuy_big(df_user):
    threshold=50000
    df_user=df_user[df_user['user_browse_num']/df_user['user_buy_num']<threshold]
    return df_user

    
#清除只有购买用户，没有其他任何交互记录的用户,与clean_product_non_car_buy_fav_user(df_user):功能相似
# def clean_just_buy_user(df_user):
#     df_user=df_user[(df_user['user_browse_num']>0)]
     
#清除最后10天没有任何交互记录的用户
def clean_lasttenday_non_type_user(df_user):
    #得到action表中最后10天有交互信息的用户
    df_ac=get_date(ALL_ACTION_TABLE)
    df_last10day_user=df_ac[df_ac['action_diff']<10]['user_id']#<=10
    
    '''
    #不管是用difftime还是time得出来的结果是相同的
    df_ac['time'] = pd.to_datetime(df_ac['time'])  
    need_time = pd.to_datetime("20160406")
    df_last10day_user=df_ac[df_ac['time']>=need_time]['user_id']
    '''
    df_last10day_user=df_last10day_user.to_frame()
    print(df_last10day_user.head(3))
    print(df_last10day_user.shape[0])
    df_last10day_user=df_last10day_user.drop_duplicates()
    print(df_last10day_user.shape[0])
    #保留所有最后10天有交互信息的用户信息，其余的删除
    df_user=df_user.merge(df_last10day_user,on='user_id',how='inner')
    print(df_user.head(3))
    print(df_last10day_user.shape[0])
    return df_user
    
     
#清除在预测时间前两天内，对product子集的商品买过的用户
#这个不可取，因为用户不买这个，可能买那个sku_id
# def clean_buied_on_ealytwoday(df_ac,df_user):
#     #得到最后两天的,购买的action表中的数据
#     df_last2day=df_ac[(df_ac['action_diff']<=2)&(df_ac['type']==4)]
#     return df_user
     
#清除完全重复记录（这个在自己写的data_format_transform.py中已经做了）
 
#清除异常天数的数据？
# def clean_outer_day():

#10.将没有任何action行为的product里的产品去掉
def cleanProduct_noAction():
    ITEM='E:\\ProgrameGra2\\Python\\MyJDateVer1\\tran_By_aDing\\item_table.csv'
    df_item=get_date(ITEM)
    print(df_item.shape[0])
    df_haveac=df_item[(df_item['browse_num']>=0)|(df_item['addcart_num']>=0)|(df_item['delcart_num']>=0)\
                      |(df_item['buy_num']>=0)|(df_item['favor_num']>=0)|(df_item['click_num']>=0)]
    df_haveac.to_csv(PRODUCT_ACTION,index=False)
    print(df_haveac.shape[0]) #[3938 rows x 19 columns]
    
if __name__=="__main__":  
    #读入数据
    
    df_user=get_date(ALL_USER_TABLE)#[999 rows x 16 columns]
    #数据清理
    #df_user=clean_product_non_car_buy_fav_user(df_user) #[694 rows x 16 columns]
    #df_user=clean_brower_much_non_nuy_user(df_user) #[691 rows x 16 columns]
    #df_user=clean_browseDivBuy_big(df_user) #[233 rows x 16 columns]
    df_user=clean_lasttenday_non_type_user( df_user) #[3 rows x 16 columns]
    df_user=df_user.drop_duplicates('user_id')
    #del df_user['index']
    #存储清理好的数据存储
    df_user.to_csv("E:\\ProgrameGra2\\Python\\MyJDateVer1\\data_tran\\S_S_Clean_ratio_User.csv",index=False) #[21122 rows x 16 columns]
    
#     print(df_user)
#     cleanProduct_noAction()
    
