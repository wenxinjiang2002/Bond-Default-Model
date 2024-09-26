import pandas as pd
import datetime
import numpy as np
import pymysql
import cal_lr_result

def get_df_from_db(host,user,passwd,database,port,sql):
    
    db = pymysql.connect(host=host,user=user,passwd=passwd,database=database,port=port)

    cursor = db.cursor()#使用cursor()方法获取用于执行SQL语句的游标
    cursor.execute(sql)# 执行SQL语句
    """
    使用fetchall函数以元组形式返回所有查询结果并打印出来
    fetchone()返回第一行，fetchmany(n)返回前n行
    游标执行一次后则定位在当前操作行，下一次操作从当前操作行开始
    """
    data = cursor.fetchall()

    #下面为将获取的数据转化为dataframe格式
    columnDes = cursor.description #获取连接对象的描述信息
    columnNames = [columnDes[i][0] for i in range(len(columnDes))] #获取列名
    df = pd.DataFrame([list(i) for i in data],columns=columnNames) #得到的data为二维元组，逐行取出，转化为列表，再转化为df
    
    """
    使用完成之后需关闭游标和数据库连接，减少资源占用,cursor.close(),db.close()
    db.commit()若对数据库进行了修改，需进行提交之后再关闭
    """
    cursor.close()
    db.close()

    return df

def get_Relation_Info(db_info):
    
    sql='select * from warn_relation_name wrn'
    
    relation_info=get_df_from_db(db_info.loc['host','ibond'],db_info.loc['user','ibond'],db_info.loc['passwd','ibond'],
                                 db_info.loc['db','ibond'],db_info.loc['port','ibond'],sql) 
    return relation_info

def data_proprocess(lr_level,r_info,option):

    # 需注意股东和实际控制人可能有重合
    # 剔除1.无实际控制人；2.endwith‘(疑似)’
    relation_info_filtered=r_info.loc[r_info['relation_entity_name']!='无实际控制人']
    relation_info_filtered['endwith']=relation_info_filtered['relation_entity_name'].str[-4:]
    relation_info_filtered=relation_info_filtered.loc[(relation_info_filtered['endwith']!='(疑似)')&(relation_info_filtered['endwith']!='(注销)')]
    
    relation_info_filtered=relation_info_filtered[['entity_name','relation_entity_name','ratio','relation']]
    
    # 双向传导：拼接原df和entity_name和relation_name反过来的df，去重subset=['entity_name','relation_entity_name']（因为原始数据中存在互相担保的情况）
    relation_info_direction=relation_info_filtered.copy(deep=True)
    df_back=relation_info_direction[['relation_entity_name','entity_name','ratio','relation']].copy(deep=True)
    df_back.columns=['entity_name','relation_entity_name','ratio','relation']
    relation_info_direction=pd.concat([relation_info_direction,df_back],axis=0).reset_index(drop=True)    
    relation_info_direction.drop_duplicates(subset=['entity_name','relation_entity_name','ratio','relation'],inplace=True,keep='first',ignore_index=True)
    
    # 读取主体预警等级，并对应pd和level拼接到entity_name,relation_entity_name上
    relation_info_merged=pd.merge(relation_info_direction,lr_level[['主体名称','pd','final_level']],left_on='relation_entity_name',right_on='主体名称',how='left')
    relation_info_merged=pd.merge(relation_info_merged, lr_level[['主体名称','pd','final_level']],left_on='entity_name',right_on='主体名称',how='left')
    relation_info_merged=relation_info_merged[['entity_name','relation_entity_name','ratio','relation','pd_x','final_level_x','pd_y','final_level_y']]
    relation_info_merged.columns=['entity_name','relation_entity_name','ratio','relation','relation_pd','relation_level','pd','final_level']
    
    network_other=relation_info_merged.loc[relation_info_merged['relation'].isin([5,7,8,9])].reset_index(drop=True)
    network_investor=relation_info_merged.loc[relation_info_merged['relation'].isin([1])].reset_index(drop=True)
    network_investor['ratio_new']=network_investor['ratio']/100
    
    # 计算担保，实控人，供应商，客户的ratio 
    # opt1：1/该类型的关联主体总数
    # opt2: 1/该类型的有预警等级的关联主体总数
    def ratio_func(df):
        count_all=len(df)
        df_have=df.loc[pd.isna(df['relation_level'])==False]
        count_have=len(df_have)
        ratio_all=1/count_all
        if count_have!=0:
            ratio_have=1/count_have
        else:
            ratio_have=0
        return pd.Series([count_all,ratio_all,count_have,ratio_have],index=['count_all','ratio_all','count_have','ratio_have'])
    
    df_ratio=network_other.groupby(['entity_name','relation']).apply(ratio_func).reset_index(drop=False)
    network_other=pd.merge(network_other,df_ratio[['entity_name','relation','ratio_all','ratio_have']],on=['entity_name','relation'],how='left')
    
    if option==1:
        network_other['ratio_new']=network_other['ratio_all']
    elif option==2:
        network_other['ratio_new']=network_other['ratio_have']
        
    network_res=pd.concat([network_investor,network_other[['entity_name','relation_entity_name','ratio','relation','relation_pd','relation_level','pd','final_level','ratio_new']]],axis=0).reset_index(drop=True)
        
    return network_res


def relationship_level(lr_level,hist_cut,labels_level,r_data,option):

    # effect=avg(比主体i等级高的关联主体pd*ratio)
    result_relation=lr_level.copy(deep=True).reset_index(drop=True)
    result_relation_extended=pd.merge(result_relation,r_data,left_on=['主体名称','final_level','pd'],right_on=['entity_name','final_level','pd'],how='left')
    
    def effect_func(df):
        df_higher=df.loc[df['relation_level']>df['final_level']]
        if df_higher.empty==True:
            effect=np.nan
        else:
            df_higher['effect']=df_higher['ratio_new']*df_higher['relation_pd']
            effect=df_higher['effect'].sum()
        
        return pd.Series([effect],index=['effect'])
    
    df_effect=result_relation_extended.groupby(['主体名称']).apply(effect_func).reset_index(drop=False)
    result_relation=pd.merge(result_relation,df_effect,on='主体名称',how='left')
    result_relation['effect'].fillna(value=0,inplace=True)

    # 关联主体风险传导-易感企业pd=原自身pd+index*effect
    result_relation['pd_relation']=result_relation['pd']+0.05*result_relation['effect']
    
    # -- 超过1的部分记为1
    result_relation['pd_relation_mod']=result_relation['pd_relation']
    result_relation.loc[result_relation['pd_relation']>1,'pd_relation_mod']=1
    
    result_relation_level=cal_lr_result.get_lr_level(result_relation, hist_cut, labels_level,option)
    
    return result_relation_level,result_relation_extended

if __name__ == "__main__":
         
  # -------------------------------------------------------------------------

  # output path
  folder_path='C:/Users/cyjiang/Documents/债券预警模型/data/'
  # input path dict
  input_dict={}
  input_dict['entity']=folder_path+'input/bond_entity_info/'
  input_dict['y']=folder_path+'input/y/'
  input_dict['X_Issr']=folder_path+'input/发行利率/'
  input_dict['X_News']=folder_path+'input/舆情/'
  input_dict['X_Fdmt']=folder_path+'input/基本面/'
  input_dict['X_Price']=folder_path+'input/量价/'
  input_dict['model']=folder_path+'input/model/'
  input_dict['relation']=folder_path+'input/关联关系/'

  # output path dict
  output_dict={}
  output_dict['entity']=folder_path+'output/bond_entity_info/'
  output_dict['y']=folder_path+'output/y/'
  output_dict['X_Issr']=folder_path+'output/发行利率/'
  output_dict['X_News']=folder_path+'output/舆情/'
  output_dict['X_Fdmt']=folder_path+'output/基本面/'
  output_dict['X_Price']=folder_path+'output/量价/'
  output_dict['model']=folder_path+'output/model/'
