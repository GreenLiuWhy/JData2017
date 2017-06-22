'''
Created on 2017年5月3日
XGB方法--在liunux上测试

@author: liu
'''
import pandas as pd
import numpy as np
import xgboost as xgb
#from sklearn.cross_validation import train_test_split #use to split train-data and test-data
from sklearn.model_selection import train_test_split

TRAIN_FILE='predict_feactures_lable.csv'
PREDICT_FILE='predict_data.csv'
RESULT="predict_result_xgboost.csv"

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
    #这个应该也是不要去重的
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

def get_balanced_trainingSet(train,bal,theseed):#传入的train是action3个月的cate=8的原始数据合并
    train_positive = train[train['buy'] == True]
    negative = train[train['buy'] == False]
    #bootstrap抽样,不一定要抽成平衡的训练集
    train_negative = pd.DataFrame()
    rand_seed=np.random.RandomState(theseed)#set the random seed
    for i in range(0, bal * len(train_positive)):
        j =rand_seed.randint(0, len(negative))
        train_negative = train_negative.append(negative.iloc[[j]], ignore_index = True)
        
    trainingData = train_positive
    trainingData = trainingData.append(train_negative, ignore_index = True)
    
    return trainingData

#训练多个xgboost模型，这些模型的正负样本比例不同，分别是1:1 ~1:9，并将这些模型存储起来
def get_xgboost_model():
    #get train-data and text-date
    alldata=get_date(TRAIN_FILE)
    alldata.dropna(how = 'any', axis = 0,inplace=True)#如果有缺失数据则删除
    print('dropna over')
    
    for bal in range(1,10):
    
        X=get_balanced_trainingSet(alldata,bal,10*bal+3)
        print('balace over')
    
        y=X['buy']
        del X['buy']
    
        #split train-data and text-data
        #也就是说我的测试集并不是最后的5天的，而是随机出来10%的数据作为测试集
        X_train,X_test,y_train,y_test=train_test_split(X,y,train_size=0.9,random_state=123) 
        print('split over')
   
        #use xgboost
        #xgboost需要转换格式，其中第一个参数是特征，第二个是label
        dtrain=xgb.DMatrix(X_train,label=y_train)
        dtext=xgb.DMatrix(X_test,label=y_test) #it really need different data to test the data
    
        #xgboost的参数，其实还有更多参数
        param={'booster':'gbtree','objective':'binary:logistic','eta':0.1,'max_depth':10,'subsample':1.0,'min_child_weight':5,\
           'colsample_bytree':0.2,'scale_pos_weight':0.1,'eval_metric':'auc','gamma':0.2,'lambda':300,'eval_metric':'error',\
           'seed':666}
        #看对于训练集和测试集的错误率
        watchlist=[(dtrain,'train'),(dtext,'val')]
    
        print('the'+str(bal)+' model is begain training')
        model=xgb.train(param,dtrain,10,evals=watchlist)#num_boost_round=1，进行训练
        print('the'+str(bal)+' model is training end')
    
        #save the model
        model.save_model('xgboost00'+str(bal)+'.model') #存储模型
        print('the'+str(bal)+'is end')
    print('end')
    

def useModelPredict():
        #get predict-data
        X_pre=get_date(PREDICT_FILE) 
        X_pre=X_pre.drop_duplicates(['user_id','sku_id'])
        X_pre = X_pre.dropna(how = 'any', axis = 0)#如果有缺失数据则删除 
        print(X_pre.shape[0])
        dpre=xgb.DMatrix(X_pre)
        
        #load the model
        bst = xgb.Booster() #init model
        bst.load_model("xgboost001.model") # l加载模型
        
        #use the model to predict
        y_pre=bst.predict(dpre) #完成预测，返回的是概率，我理解的是》0.5就是正样本，否则是负样本
        result=X_pre[['user_id','sku_id']]
        result['pro']=y_pre #buy-probability of every user-product
        result.to_csv('result_pro.csv',index=False)
        print(result.head(2))
        
        #结果只需要提交预测为购买的用户，不购买的无需提交
        result=result[result['pro']>0.5]
        #如果一个用户被同时预测会购买多种产品，则只提交最最可能购买的那个产品
        idx=result.groupby(['user_id'])['pro'].transform(max)==result['pro']
        result=result[idx]
        del result['pro']
        #此时已然会存在一个用户被预测购买多种产品的情形，即概率值相同被同时保存，智能保留一个user_id
        result.drop_duplicates(['user_id'],inplace=True)
        #使user全部是user表中的（本来就是，不用管），product全部是product表中的
        df_pro=get_date_needlist('JData_inter_product.csv', ['sku_id'])
        result=result.merge(df_pro,on='sku_id',how='inner')
        
        #save the result
        print(result.shape[0])
        result.to_csv(RESULT, index = False)
        print('end') 
        

      
