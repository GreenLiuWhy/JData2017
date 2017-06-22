'''
Created on 2017年5月10日
因为生成balance的时间较长，而且每个模型都需要使用，所以将balance存入文件之间调用使用
@author: liu
'''
import pandas as pd
import numpy as np
import datetime as dt

TRAIN_FILE='result_choose_0516\\train_data.csv'
TEST_DATA='result_choose_0516\\test_data.csv'


def get_balanced_trainingSet(train,bal_rate):#传入的train是action3个月的cate=8的原始数据合并
    train_positive = train[train['buy'] == True]
    negative = train[train['buy'] == False]
    #bootstrap抽样,不一定要抽成平衡的训练集
    train_negative = pd.DataFrame()
    for i in range(0, int(bal_rate * len(train_positive))):
        j = np.random.randint(0, len(negative))
        train_negative = train_negative.append(negative.iloc[[j]], ignore_index = True)
        
    trainingData = train_positive
    trainingData = trainingData.append(train_negative, ignore_index = True)
    
    trainingData.to_csv('result_until_userPro\\balanced_1_'+str(bal_rate)+'.csv',index=False)
    print(str(bal_rate)+'  end')
    return

def complex_balanced_trainingSet(train,bal_rate,ss):#传入的train是action3个月的cate=8的原始数据合并
    train_positive = train[train['buy'] == True]
    negative = train[train['buy'] == False]
    #bootstrap抽样,不一定要抽成平衡的训练集
    train_negative = pd.DataFrame()
    for i in range(0, int(bal_rate * len(train_positive))):
        j = np.random.randint(0, len(negative))
        train_negative = train_negative.append(negative.iloc[[j]], ignore_index = True)
        
    trainingData = train_positive
    trainingData = trainingData.append(train_negative, ignore_index = True)
    
    trainingData.to_csv(ss+'_1_'+str(bal_rate)+'.csv',index=False)
    print(str(bal_rate)+'  end')
    return

def theMain():
    train_all=pd.read_csv(TRAIN_FILE)
    #test=pd.read_csv(TEST_DATA)
    #train_all=train_all.append(test,ignore_index=True)
    train_all = train_all.dropna(how = 'any', axis = 0)#如果有缺失数据则删除
    for bal in range(4,10):
        get_balanced_trainingSet(train_all, bal)

def runing0516(): 
    df=pd.read_csv(TRAIN_FILE)
    print(dt.datetime.now())
    for bal in range(7,13):
        complex_balanced_trainingSet(df, bal, 'result_choose_0516\\balance')
    print(dt.datetime.now())
    
    df_text=pd.read_csv(TEST_DATA)
    df=df.append(df_text,ignore_index=True)
    print(dt.datetime.now())
    for bal in range(7,13):
        complex_balanced_trainingSet(df, bal, 'result_choose_0516\\all_balance')
    print(dt.datetime.now())
        
runing0516()
    

    
    