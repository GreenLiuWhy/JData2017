'''
Created on 2017年5月15日

@author: liu
'''
#-*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from collections import Counter
from sklearn.ensemble import GradientBoostingClassifier
import evaluate_model as eee

TRAIN_FILE='result_choose_0516\\rain_data.csv'
PREDICT_FILE='result_choose_0516\\predict_data.csv'

#提取行为特征
def getFeatures(group):
    behavior_type = group.type.astype(int)
    type_cnt = Counter(behavior_type)

    group['browse_num'] = type_cnt[1]
    group['addcart_num'] = type_cnt[2]
    group['delcart_num'] = type_cnt[3]
    group['favor_num'] = type_cnt[5]
    group['click_num'] = type_cnt[6]
    
    return group

    
#提取label   
def getClassLabel(group):
    behavior_type = group.type.astype(int)
    type_cnt = Counter(behavior_type)

    group['buy'] = type_cnt[4] > 0

    return group[['user_id','sku_id', 'buy']]  
 

def add_type_count(group):
    behavior_type = group.type.astype(int)
    type_cnt = Counter(behavior_type)

    group['browse_num'] = type_cnt[1]
    group['addcart_num'] = type_cnt[2]
    group['delcart_num'] = type_cnt[3]
    group['buy'] = type_cnt[4] > 0
    group['favor_num'] = type_cnt[5]
    group['click_num'] = type_cnt[6]

    return group


def get_from_train_data(fname, chunk_size = 100000):
    reader = pd.read_csv(fname, header = 0, iterator=True)
    chunks = []
    loop = True
    while loop:
        try:
            chunk = reader.get_chunk(chunk_size)
            chunks.append(chunk)
        except StopIteration:
            loop = False
            print("Iteration is stopped")

    df_ac = pd.concat(chunks, ignore_index = True)        
    return df_ac
    
                   
                   
def merge_train_data():
    df_ac=pd.read_csv(TRAIN_FILE,header=0)
    
    return df_ac
    
      
    
def get_balanced_train_data(train,select_ratio):
    train_positive = train[train['buy'] == True]        
    negative = train[train['buy'] == False]
    #bootstrap抽样,不一定要抽成平衡的训练集
    train_negative = pd.DataFrame()
    #目前正类负类样本比例1:3
    for i in range(0, int(select_ratio * len(train_positive))): #3.0
        j = np.random.randint(0, len(negative))
        train_negative = train_negative.append(negative.iloc[[j]], ignore_index = True)
        
    trainingData = train_positive.copy()
    
    trainingData = trainingData.append(train_negative, ignore_index = True)
    
    return trainingData
    

def EnsembleLearning(train_data, test_data,select_ratio):
    #设定基学习器的个数，多了慢，少了不准...
    baseLearnerNum = 1 #8
    baseLearnerSet = ['learner %d' % i for i in range(0, baseLearnerNum)]
                      
    X_test = test_data.copy()
    X_test = X_test.dropna(how = 'any', axis = 0)
    #(user_id--sku_id作为索引)
    ensemblePredict = X_test.copy()
    tempPredict = X_test.copy()
    X_test.set_index(keys = ['user_id','sku_id'], inplace = True)  
    
    for baseLearner in baseLearnerSet:
        #每次得到相对平衡的训练集进行模型训练
        trainData = get_balanced_train_data(train_data,select_ratio)
        print('banlance over')
        trainData = trainData.dropna(how = 'any', axis = 0)
        
        y_train = trainData['buy']
        X_train = trainData.drop(['buy'], axis = 1, inplace = False)    
        #(user_id--sku_id作为索引)
        X_train.set_index(keys = ['user_id','sku_id'], inplace = True)
        
        
        #GBDT预测              
        gbc = GradientBoostingClassifier(learning_rate = 0.03, random_state = 10) #learning_rate = 0.02,
        #print cross_val_score(gbc, X_train, y_train,cv = 5).mean()
        
        gbc.fit(X_train, y_train)
        
        y_predict = gbc.predict(X_test)
  
        
        tempPredict[baseLearner] = y_predict  
        #ensemblePredict['finalPredict'] = ensemblePredict['finalPredict'] & tempPredict[baseLearner]             
    
    firstLearner = 'learner 0'
    lastLearner = 'learner %d' % (baseLearnerNum - 1)
    tempPredict = tempPredict.loc[:,firstLearner:lastLearner]
    
    Majority = []
    voteNum = []    
    for i in range(0,len(tempPredict)):
        #统计各个基学习器的分类结果
        count = Counter(tempPredict.iloc[i])
        #少数服从多数
        if count[True] >= count[False]:
            Majority.append(True)
            voteNum.append(count[True])
        else:
            Majority.append(False)
            voteNum.append(count[False])
   
    #最终得到的票选结果和相应的票数
    ensemblePredict['finalPredict'] = Majority
    ensemblePredict['votenum'] = voteNum

    #只输出预测为购买的用户
    ensemblePredict = ensemblePredict[ensemblePredict['finalPredict'] == True]
    print(ensemblePredict.shape[0])
    #如果一个user同时被预测买多个商品，则只输出票数最多的那个sku_id
    idx = ensemblePredict.groupby(['user_id'])['votenum'].transform(max) == ensemblePredict['votenum']
    ensemblePredict = ensemblePredict[idx]
    print(ensemblePredict.shape[0])
    return ensemblePredict
     
def oneGDBT(train_data, test_data,diedai,maxf):
    #设定基学习器的个数，多了慢，少了不准...
    baseLearnerNum = 1
    baseLearnerSet = ['learner %d' % i for i in range(0, baseLearnerNum)]
                      
    X_test = test_data.copy()
    X_test = X_test.dropna(how = 'any', axis = 0)
    #(user_id--sku_id作为索引)
    ensemblePredict = X_test.copy()
    tempPredict = X_test.copy()
    X_test.set_index(keys = ['user_id','sku_id'], inplace = True)  
    
    for baseLearner in baseLearnerSet:
        #每次得到相对平衡的训练集进行模型训练
        trainData=pd.read_csv('result_choose_0516\\all_balance_1_6.csv')#-----------改----------------
        
        print('banlance over')
        trainData = trainData.dropna(how = 'any', axis = 0)
        
        y_train = trainData['buy']
        X_train = trainData.drop(['buy'], axis = 1, inplace = False)    
        #(user_id--sku_id作为索引)
        X_train.set_index(keys = ['user_id','sku_id'], inplace = True)
        
        
        #GBDT预测              
        gbc = GradientBoostingClassifier(random_state=20,learning_rate=0.1,n_estimators=diedai,max_features=maxf)
                                        # max_depth=max_d,min_samples_split=min_ss,)
        #print cross_val_score(gbc, X_train, y_train,cv = 5).mean()
        
        gbc.fit(X_train, y_train)
        
        y_predict = gbc.predict(X_test)
        y_pro=gbc.predict_proba(X_test)
        ensemblePredict['finalPredict']=y_predict
        
        #y_predict_proba为二维array类型，每行的最大值即为对该样本分类的肯定程度
        predict_probability = np.max(y_pro, axis = 1)
        #转换为dataframe的格式，添加到X_test的最后'probability'列
        predict_probability = pd.DataFrame(predict_probability, columns = ['probability'])
        ensemblePredict['probility']=predict_probability
  

    #只输出预测为购买的用户
    ensemblePredict = ensemblePredict[ensemblePredict['finalPredict'] == True]
    print(ensemblePredict.shape[0])
    #如果一个user同时被预测买多个商品，则只输出概率大的
    idx = ensemblePredict.groupby(['user_id'])['probility'].transform(max) == ensemblePredict['probility']
    ensemblePredict = ensemblePredict[idx]
    print(ensemblePredict.shape[0])
    #因为我知道正确的是469个，所以我在这里强制输出可能性最大的1000个
#     ensemblePredict.sort('probility', axis=0, ascending=False, inplace=True)
#     ensemblePredict=ensemblePredict.reset_index()
#     del ensemblePredict['index']
#     ensemblePredict=ensemblePredict.loc[0:1200,:]
#     print(ensemblePredict.shape[0])
    return ensemblePredict   
    
def theMain():
    
    #线下测试
    #读取get_train_data.py文件生成的训练数据
    #train_data = merge_train_data()   
    train_data=[]
    
    '''
    test_data, test_label = get_test_data(ACTION_201604_FILE_CATE8)
    test_data.to_csv("D:\\Jdata\\test_data_localtest.csv", index = False)
    test_label.to_csv("D:\\Jdata\\test_label_localtest.csv", index = False)
    '''
    #线下测试使用0410-0415的数据作为测试集
    test_data=pd.read_csv(PREDICT_FILE)
    bal_over=pd.read_csv('result_choose_0516\\predict_result0518_allbal6_rate0.1_doedao260_maxf45.csv')
    test_data=pd.merge(test_data,bal_over,on=['user_id','sku_id'],how='inner')
    #test_data=test_data[test_data['cate']==8]

    #ensemble learning集成学习
    #test_predict = EnsembleLearning(train_data, test_data,select_ratio) #g-----------gai---------
    test_predict=oneGDBT(train_data, test_data, diedai=390, maxf=5)
    print(test_predict.shape[0])
    
    #本来就是cate=8的数据，这里删除得票相同的
    test_predict.drop_duplicates('user_id',inplace=True)
    #test_label=pd.merge(df_pro,test_label,on='sku_id',how='inner')
    print(test_predict.shape[0])

    test_predict=test_predict[['user_id','sku_id']]
    theZore=pd.read_csv('result//ZeroScore.csv')
    idx=test_predict['user_id'].isin(theZore['user_id'])
    test_predict=test_predict[idx==False]
    print(test_predict.shape[0])
    test_predict.drop_duplicates('user_id',inplace=True)
    print(test_predict.head(2))
    test_predict.to_csv("result_choose_0516\\predict_result0518_allbal6_rate0.1_diedai390_maxf45_5.csv",index = False)

theMain()
    
