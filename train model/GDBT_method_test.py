'''
Created on 2017年5月9日

@author: liu
'''
import pandas as pd
import numpy as np

from sklearn.ensemble import GradientBoostingClassifier
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import PolynomialFeatures

import evaluate_model as eva
'''
from collections import Counter
from sklearn.preprocessing import PolynomialFeatures
from sklearn.feature_selection import VarianceThreshold
from sklearn.feature_selection import SelectKBest
from sklearn.feature_selection import chi2
from sklearn.feature_selection import SelectFromModel
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.decomposition import PCA
'''

TRAINDATA='result_choose_0516\\train_data.csv'
TEST_FILE='result_choose_0516\\test_data.csv'


PRO='data\\JData_inter_product.csv'


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
    df_ac=pd.read_csv(TRAINDATA,header=0)
    
    return df_ac
    
 

def read_test_data():
    test_data = pd.read_csv(TEST_FILE, header = 0)
    test_data=test_data[test_data['cate']==8]
    del test_data['buy']
    bal_pre=pd.read_csv("result_choose_0516\\test_result_bal6_maxf45_rate0.1_diedai260.csv")
    test_data=pd.merge(test_data,bal_pre,on=['user_id','sku_id'],how='inner')
    test_label=pd.read_csv('result\\theA.csv')
    #test_label=pd.read_csv('result\\test_label_localtest.csv')
    return test_data, test_label
      
    
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
    

def oneGDBT(train_data, test_data,select_ratio=6,learate=0.1,diedai=220,max_d=5,min_ss=200,maxf=5):
    #设定基学习器的个数，多了慢，少了不准...
    baseLearnerNum = 1
    baseLearnerSet = ['learner %d' % i for i in range(0, baseLearnerNum)]
                      
    X_test = test_data.copy()
    X_test = X_test.dropna(how = 'any', axis = 0)
    #(user_id--sku_id作为索引)
    ensemblePredict = X_test.copy()
    tempPredict = X_test.copy()
    #X_test.set_index(keys = ['user_id','sku_id'], inplace = True) 
    X_test_new=MinMaxScaler().fit_transform(X_test) 
    X_test_new=PolynomialFeatures().fit_transform(X_test_new)
    
    for baseLearner in baseLearnerSet:
        #每次得到相对平衡的训练集进行模型训练
        trainData=pd.read_csv('result_choose_0516\\balance_1_3.csv')#-----------改----------------
        
        print('banlance over')
        trainData = trainData.dropna(how = 'any', axis = 0)
        
        y_train = trainData['buy']
        X_train = trainData.drop(['buy'], axis = 1, inplace = False)    
        #(user_id--sku_id作为索引)
        #X_train.set_index(keys = ['user_id','sku_id'], inplace = True)
        X_train_new=MinMaxScaler().fit_transform(X_train)
        X_train_new=PolynomialFeatures().fit_transform(X_train_new)
        
        
        #GBDT预测              
        gbc = GradientBoostingClassifier(random_state=20,learning_rate=learate,n_estimators=diedai\
                                         ,max_depth=max_d,min_samples_split=min_ss,max_features=maxf)
        #print cross_val_score(gbc, X_train, y_train,cv = 5).mean()
        
        gbc.fit(X_train_new, y_train)
        print(gbc.feature_importances_)
        y_predict = gbc.predict(X_test_new)
        y_pro=gbc.predict_proba(X_test_new)
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
    test_data, test_label = read_test_data()
    print('test_lable='+str(test_label.shape[0]))
    '''
    for select_ratio in [4]: #选择负样本的比例
        #print('select_ratio='+str(select_ratio))
        for diedaicishu in [220]: #迭代次数
            #print('迭代次数： '+str(diedaicishu))
            for max_d in [5]:
                print('max_deep='+str(max_d))
                for min_sam_sp in [200]:
                    print('min_sample_split='+str(min_sam_sp))
                    for maxf in range(5,86,5):
                        print('max_F='+str(maxf))
                        #ensemble learning集成学习
                        #test_predict = EnsembleLearning(train_data, test_data,select_ratio)#-----------改--------
                        test_predict=oneGDBT(train_data, test_data, select_ratio,diedaicishu,max_d,min_sam_sp,maxf) #---------------注释-----------)
                        test_predict.drop_duplicates('user_id',inplace=True)
                        print(test_predict.shape[0])
                        
                        #看得分
                        eva.report_me(test_predict[['user_id','sku_id']], test_label)
                        '''
    #learn_rate  和  迭代次数
    for learn_rate in [0.1]:
        print('learn_rate= '+str(learn_rate))
        for di in [390]:#range(180,400,10):
            print('diedai= '+str(di))
            test_predict=oneGDBT(train_data, test_data, learate=learn_rate, diedai=di,maxf=None)
            print(test_predict.shape[0])
            test_predict.drop_duplicates('user_id',inplace=True)
            eva.report_me(test_predict[['user_id','sku_id']], test_label)
            
    test_predict=test_predict[['user_id','sku_id']]
    print(test_predict.head(2))
    #test_predict.to_csv("result_choose_0516\\test_result_bal6_maxf45_rate0.1_diedai260.csv",index = False)
    
theMain() 
