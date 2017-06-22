'''
Created on 2017年5月9日

@author: liu
'''
import pandas as pd
import numpy as np
from sklearn.cross_validation import cross_val_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.cross_validation import train_test_split
#from xgboost import XGBClassifier


TRAIN_FILE='predict_feactures_lable.csv'
PREDICT_FILE='predict_data.csv'
RESULT="predict_result_rfl.csv"


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



def get_balanced_trainingSet(train):#传入的train是action3个月的cate=8的原始数据合并
    train_positive = train[train['buy'] == True]
    negative = train[train['buy'] == False]
    #bootstrap抽样,不一定要抽成平衡的训练集
    train_negative = pd.DataFrame()
    for i in range(0, 7 * len(train_positive)):
        j = np.random.randint(0, len(negative))
        train_negative = train_negative.append(negative.iloc[[j]], ignore_index = True)
        
    trainingData = train_positive
    trainingData = trainingData.append(train_negative, ignore_index = True)
    
    return trainingData
   
    
if __name__ == "__main__":
    
    
    trainingData = pd.read_csv(TRAIN_FILE)#3个月的sample_abstract.py中得到的数据数据合并
    trainingData = get_balanced_trainingSet(trainingData)#返回要真正用来训练的正负样本集的和
    trainingData = trainingData.dropna(how = 'any', axis = 0)#如果有缺失数据则删除

    y = trainingData['buy'] #label
    X = trainingData.drop(['buy'], axis = 1, inplace = False)#feacture
    #X_train,X_test,y_train,y_test=train_test_split(X,y,train_size=0.1,random_state=11)   

    X_pre = pd.read_csv(PREDICT_FILE)
    X_pre = X_pre.dropna(how = 'any', axis = 0)

        
    #random forest预测,决策树数量暂定100,其他参数默认，可以尝试网格搜索寻优最佳参数
    print('begain')
    rfc = RandomForestClassifier(n_estimators=10, criterion='gini', max_depth=None, \
                                 min_samples_split=2,min_samples_leaf=1, min_weight_fraction_leaf=0.0, \
                                 max_features='auto', max_leaf_nodes=None, min_impurity_split=1e-07, \
                                 bootstrap=True, oob_score=False,n_jobs=-1, random_state=None, \
                                 verbose=0, warm_start=False, class_weight='balanced')
    print('end')
    #5倍交叉验证的准确率，此处可以改为线上评测指标，不再使用准确率
    #print(cross_val_score(rfc, X_train, y_train,cv = 5).mean())
     
    rfc.fit(X, y)
    #查看随机森林对各个feature的权重得分
    print(rfc.feature_importances_)
       
    #得到预测结果及预测为相应类别的肯定程度
    y_predict = rfc.predict(X_pre)
    y_predict_proba = rfc.predict_proba(X_pre)
      
      
    #y_predict_proba为二维array类型，每行的最大值即为对该样本分类的肯定程度
    predict_probability = np.max(y_predict_proba, axis = 1)
    #转换为dataframe的格式，添加到X_test的最后'probability'列
    predict_probability = pd.DataFrame(predict_probability, columns = ['probability'])
    
    predict_result = X_pre[['user_id','sku_id']]
    predict_result['buy'] = pd.Series(y_predict)
    predict_result['probability'] = predict_probability
    
    #结果只需要提交预测为购买的用户，不购买的无需提交
    predict_result = predict_result[predict_result['buy'] == True]
    #如果一个用户被同时预测会购买多种产品，则只提交最最可能购买的那个产品
    idx = predict_result.groupby(['user_id'])['probability'].transform(max) == predict_result['probability']
    
    predict_result = predict_result[idx]

    #此时已然会存在一个用户被预测购买多种产品的情形，即概率值相同被同时保存，智能保留一个user_id
    predict_result = predict_result.drop_duplicates(['user_id'])
    
    #使user全部是user表中的（本来就是，不用管），product全部是product表中的
    df_pro=get_date_needlist('JData_inter_product.csv', ['sku_id'])
    predict_result=predict_result.merge(df_pro,on='sku_id',how='inner')
    
    predict_result.to_csv(RESULT, index = False)
    #print(predict_result.head(5))
    print('end')