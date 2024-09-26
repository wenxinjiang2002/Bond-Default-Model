# -*- coding: utf-8 -*-
"""
Created on Thu Jan 31 17:09:06 2024

To Recuiter: This is the py for the final log-reg modeling/boxing for bond default scoring.
    The parameters involved are in saparate py.
计算当日主体&债券预警分数&等级

@author: Jiang
"""

import datetime
import pandas as pd
import numpy as np

# 需要补上price
def get_model_input(target_date,entity_list,s_issr,s_basic,s_news,s_price): 
    
    # 构建dataframe
    model_input=pd.DataFrame(entity_list,columns=['主体名称'])
    model_input['日期']=datetime.datetime.strptime(target_date,'%Y-%m-%d')
    
    # 拼接发行利率得分
    s_issr=s_issr[['主体名称','score_issr']]
    model_input=pd.merge(model_input,s_issr,on='主体名称',how='left')
    
    # 拼接基本面得分
    s_basic=s_basic[['主体名称','score_basic']]
    model_input=pd.merge(model_input,s_basic,on='主体名称',how='left')
    
    # 拼接舆情得分
    s_news=s_news[['主体名称','compressed_value']]
    s_news.columns=['主体名称','score_news']
    model_input=pd.merge(model_input,s_news,on='主体名称',how='left')
    
    # 拼接价格得分
    s_price=s_price[['主体名称','compressed_value']]
    s_price.columns=['主体名称','score_price']
    model_input=pd.merge(model_input,s_price,on='主体名称',how='left')

    
    return model_input

def sigmoid(z):
    a = 1/(1+np.exp(0-z))
    return a

def get_lr_pd(lr_input,coefficient):

    w=coefficient[['coef_news','coef_issr','coef_basic','coef_price']]
    b=coefficient.loc[0,'intercept']
    X=lr_input[['score_news', 'score_issr', 'score_basic','score_price']]

                    
    # 具体的分数
    X_SCORE=pd.DataFrame(sigmoid(np.dot(X,w.T) + b),columns=['pd'])
    
    lr_result=pd.concat([lr_input,X_SCORE],axis=1)
    
    return lr_result


def get_lr_level(lr_pd,hist_cut,labels_level,option):
    
    lr_level=lr_pd.copy(deep=True)
    
    hist_define=hist_cut.iloc[0].tolist()
    percentile_define=list(map(float,hist_cut.columns.tolist()))
    
    def get_final_level(hist_level,daily_level):
        if hist_level>=daily_level:
            return hist_level
        else:
            return daily_level
    
    if option==1: #initial
        # ---hist_cut
        lr_level['hist_level']=pd.cut(lr_level['pd'],hist_define,labels=labels_level)
        
        # ---daily cut
        lr_level['daily_level']=pd.qcut(lr_level['pd'].rank(method='first'),q=percentile_define,labels=labels_level)
        level_pd_t=np.percentile(lr_level['pd'], [i * 100 for i in percentile_define])
        level_pd_t=[float(i) for i in level_pd_t]     
        df_level_pd_t=pd.DataFrame(level_pd_t).T
        df_level_pd_t.columns=percentile_define
        
        lr_level['final_level']=lr_level.apply(lambda x: get_final_level(x['hist_level'],x['daily_level']),axis=1)
        return lr_level,df_level_pd_t
    
    elif option==2: # 关联关系传导后
        # ---hist_cut
        lr_level['hist_level_rela']=pd.cut(lr_level['pd_relation_mod'],hist_define,labels=labels_level)
        
        # ---daily cut
        lr_level['daily_level_rela']=pd.qcut(lr_level['pd_relation_mod'].rank(method='first'),q=percentile_define,labels=labels_level)
    
        lr_level['final_level_rela']=lr_level.apply(lambda x: get_final_level(x['hist_level_rela'],x['daily_level_rela']),axis=1)
        
        return lr_level
    
def get_lr_score(lr_level):
    
    entity_level_t=lr_level[['主体名称','日期','pd_relation_mod','final_level_rela']].copy(deep=True)
    entity_level_t.columns=['主体名称','日期','pd','预警等级']
    entity_level_t['预警得分']=entity_level_t['pd']*100
    
    return entity_level_t[['主体名称','日期','预警等级','预警得分']]


def get_bond_level(lr_pd,hist_cut,daily_cut,labels_level):
    
    lr_level=lr_pd.copy(deep=True)
    
    hist_define=hist_cut.iloc[0].tolist()
    hist_define[0]=0.0 # 债券pd最小值为0
    percentile_define=list(map(float,hist_cut.columns.tolist()))
    daily_define=daily_cut.iloc[0].tolist()
    daily_define[0]=0.0
    daily_define[-1]=1.0
    
    def get_final_level(hist_level,daily_level):
        if int(hist_level)>=int(daily_level):
            return hist_level
        else:
            return daily_level
    
    # ---hist_cut
    lr_level['hist_level']=pd.cut(lr_level['pd'],hist_define,labels=labels_level,right=False)
    
    # ---daily cut
    lr_level['daily_level']=pd.cut(lr_level['pd'],daily_define,labels=labels_level,right=False)
    
    # pd.cut设置为和qcut默认的一样左闭右开，需要手动设置pd=1的等级
    
    lr_level.loc[lr_level['pd']==1,'hist_level']='10'
    lr_level.loc[lr_level['pd']==1,'daily_level']='10'

    lr_level['final_level']=lr_level.apply(lambda x: get_final_level(x['hist_level'],x['daily_level']),axis=1)
    
    return lr_level
    

if __name__ == "__main__":
    
    starttime0 = datetime.datetime.now()

    # -------------------------------------------------------------------------

    # output path
    folder_path='C:/Users/cyjiang/Documents/债券预警模型/增量/'
    # input path dict
    input_path=folder_path+'input/'

    # output path dict
    output_dict={}
    output_dict['entity']=folder_path+'output/bond_entity_info/'
    output_dict['X_Issr']=folder_path+'output/发行利率/'
    output_dict['X_News']=folder_path+'output/舆情/'
    output_dict['X_Fdmt']=folder_path+'output/基本面/'
    output_dict['X_Price']=folder_path+'output/量价/'
    output_dict['model']=folder_path+'output/model/'
    output_dict['transform'] = folder_path+'output/transform/'
