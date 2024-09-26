# -*- coding: utf-8 -*-
"""
Created on Tue Jan 23 11:05:20 2024

基于发行利率因子.py改动实现增量跑批

@author: erlu
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Dec  4 17:19:21 2023

data_preprocess - 发行利率数据预处理，分箱

20231214 update:
    bond_info_20231214: 全部债券（未到期）

@author: erlu
"""
import datetime
import pandas as pd
import numpy as np
import pymysql
import decimal

# 定义获取函数
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

# 债券主体映射关系
def get_Mapping_Dict(db_info):
    
    sql="""select 
    we.compName '主体名称',
    wb.bond_code '债券编码',
    wb.bond_name '债券名称'
    from warn_bondinfo wb
    inner join warn_entityinfo we on wb.entity_id = we.id 
    """
    
    bond_mapping=get_df_from_db(db_info.loc['host','ibond'],db_info.loc['user','ibond'],db_info.loc['passwd','ibond'],
                                 db_info.loc['db','ibond'],db_info.loc['port','ibond'],sql)

    return bond_mapping

# 获取wind原始数据（过去一年全量）
def get_Issuing_Rate(target_date,db_info):
    dt_target_date=datetime.datetime.strptime(target_date,'%Y-%m-%d')
    ago = (datetime.date(dt_target_date.year-1,dt_target_date.month,dt_target_date.day)).strftime('%Y%m%d')
    target_date=dt_target_date.strftime('%Y%m%d')
    
    sql='select b_info_fullname,\
        s_info_name,\
        b_info_issuer,\
        b_info_listdate,\
        b_info_issueprice,\
        b_info_couponrate,\
        b_info_term_year_,\
        b_info_form \
        from cbonddescription \
        ' + 'where b_info_listdate'+'>='+ago+' and b_info_listdate'+'<='+target_date

    
    issuing_rate=get_df_from_db(db_info.loc['host','wind'],db_info.loc['user','wind'],db_info.loc['passwd','wind'],
                                 db_info.loc['db','wind'],db_info.loc['port','wind'],sql)

    return issuing_rate

def data_preprocess(target_date,df):
        
    dt_target_date=datetime.datetime.strptime(target_date,'%Y-%m-%d')
    ago = (datetime.date(dt_target_date.year-1,dt_target_date.month,dt_target_date.day)).strftime('%Y-%m-%d')
    
    df['b_info_listdate']=df['b_info_listdate'].apply(lambda x: datetime.datetime.strptime(x,'%Y%m%d'))

    df['b_info_term_year']=df['b_info_term_year_'].copy(deep=True)
    df.loc[(df['b_info_term_year']>=0.5)&(df['b_info_term_year']<1), 'b_info_term_year']= decimal.Decimal(1)
    df.loc[(df['b_info_term_year']>=0)&(df['b_info_term_year']<0.5), 'b_info_term_year']= decimal.Decimal(0.5)
    
    def cal_ratio(issue_data):
        issue_data=issue_data.drop_duplicates(keep='first',inplace=False).reset_index(drop=True) # 防止上一步有重复项被drop之后index间断            
        issue_data['b_info_couponrate'].fillna(value=np.nan,inplace=True) # 由于对None无法处理，直接读也读不出来，因为选择将None替换位NaN
        
        for i in range(0,len(issue_data)):
            print('current: ',i)
            print('total: ',len(issue_data))
            bond_type=issue_data.loc[i,'b_info_form']
            term=issue_data.loc[i,'b_info_term_year']
            # rolling过去一年
            rolling_data_region=issue_data.loc[(issue_data['b_info_form']==bond_type)&(issue_data['b_info_term_year']==term)].copy(deep=True)

            issue_data.loc[i,"benchmark_avg"] = np.nanmean(rolling_data_region['b_info_couponrate'])

                
        issue_data['issr_ratio']=(issue_data['b_info_couponrate']-issue_data['benchmark_avg'])/issue_data['benchmark_avg']

        return issue_data
    
    df_processed=cal_ratio(df)
    df_processed.sort_values(by='issr_ratio', ascending=False)
        
    # 同一天同一主体选取最大ratio
    df_entity=df_processed.drop_duplicates(['b_info_listdate','b_info_issuer'],keep='first',inplace=False,ignore_index=True)

    # 构建标准化主体时间序列
    def daily_data_process(start_d,end_d,data):
   
        entity_list=data['b_info_issuer'].unique()
                
        time_index = pd.date_range(start_d, end_d, freq='D')

        time_data = pd.DataFrame(index=time_index)
        time_data=time_data.reset_index(drop=False)
        time_data.columns=['time']
        
        entity_daily_data=pd.DataFrame()
        
        for i in range(0,len(entity_list)):
            time_data["b_info_issuer"]=entity_list[i]
            if i==0:
                entity_daily_data=time_data
            else:
                entity_daily_data=pd.concat([entity_daily_data,time_data])
            time_data=time_data.drop(columns="b_info_issuer")
        
        entity_daily_data=entity_daily_data.reset_index(drop=True)   
        
        return entity_daily_data

    df_entity_daily=daily_data_process(ago,target_date,df_entity)
    ts_issue_rate = pd.merge(df_entity_daily,df_entity,left_on=['time','b_info_issuer'],right_on = ['b_info_listdate','b_info_issuer'],how = 'left')

    # 进行前值填充
    ts_issue_rate['issr_ratio_ffilled'] = ts_issue_rate.groupby('b_info_issuer')['issr_ratio'].fillna(method='ffill')
    
    issue_rate_t=ts_issue_rate.loc[ts_issue_rate['time']==dt_target_date].reset_index(drop=True) #nan 代表一年内无新发债
    issue_rate_t['issr_ratio_ffilled']=issue_rate_t['issr_ratio_ffilled'].apply(lambda x: float(x)) #decimal类型转换为float类型才能进行分数映射
    
    return df_processed,ts_issue_rate,issue_rate_t

def concat_local(target_date,bond_mapping,df_processed):
    dt_target_date=datetime.datetime.strptime(target_date,'%Y-%m-%d')
    ago = (datetime.date(dt_target_date.year-1,dt_target_date.month,dt_target_date.day)).strftime('%Y-%m-%d')

    # ---------------1. 拼接债券 -> 拼接主体---------------
    
    # 根据债券简称和债券代码，将主体名称拼接到新发债上市首日偏离数据
    data_e_merged = pd.merge(df_processed,bond_mapping,left_on=['债券代码','债券简称'],right_on=['债券编码','债券名称'],how='left')
    data_e_merged.fillna(0,inplace=True)
    data_e_merged = data_e_merged[~(data_e_merged['主体名称'] == '0')]
    data_e_merged.drop(columns=['债券名称','债券编码','地区','省','市'],inplace=True)
    
    # 同一天同一主体选取最大ratio
    df_entity=data_e_merged.drop_duplicates(['上市日期','主体名称'],keep='first',inplace=False,ignore_index=True)

    # 构建标准化主体时间序列
    def daily_data_process(start_d,end_d,data):
   
        entity_list=data['主体名称'].unique()
                
        time_index = pd.date_range(start_d, end_d, freq='D')

        time_data = pd.DataFrame(index=time_index)
        time_data=time_data.reset_index(drop=False)
        time_data.columns=['time']
        
        entity_daily_data=pd.DataFrame()
        
        for i in range(0,len(entity_list)):
            time_data["b_info_issuer"]=entity_list[i]
            if i==0:
                entity_daily_data=time_data
            else:
                entity_daily_data=pd.concat([entity_daily_data,time_data])
            time_data=time_data.drop(columns="b_info_issuer")
        
        entity_daily_data=entity_daily_data.reset_index(drop=True)   
        
        return entity_daily_data

    df_entity_daily=daily_data_process(ago,target_date,df_entity)
    ts_issue_rate = pd.merge(df_entity_daily,df_entity,left_on=['time','b_info_issuer'],right_on = ['上市日期','主体名称'],how = 'left')
    keep = ['time','b_info_issuer','债券简称','benchmark_region_ratio']
    keep = pd.DataFrame(keep,columns=['指标'])
    keep = set(keep['指标'])
    ts_issue_rate.drop(columns = [col for col in ts_issue_rate.columns if col not in keep],inplace = True)
    # 进行前值填充
    ts_issue_rate['benchmark_region_ratio2'] = ts_issue_rate.groupby('b_info_issuer')['benchmark_region_ratio'].fillna(method='ffill')
    
    issue_rate_t=ts_issue_rate.loc[ts_issue_rate['time']==dt_target_date].reset_index(drop=True) #nan 代表一年内无新发债
    issue_rate_t['benchmark_region_ratio2']=issue_rate_t['benchmark_region_ratio2'].apply(lambda x: float(x)) #decimal类型转换为float类型才能进行分数映射
    
    return ts_issue_rate,issue_rate_t
def get_bins_mapping_result(df,bins_rule,factor_name):

    df1=df[['time','主体名称','issr_ratio_ffilled']].copy(deep=True) # return 到一个新的dataframe里
    def get_bin_intervals(rule_i):
         score=rule_i['指标分数'].tolist()
         intervals=rule_i['最小值'].tolist()
         intervals.append(rule_i['最大值'].tolist()[-1])
         
         return intervals,score   
 
    if factor_name=='score_basic':
        bin_intervals,score=get_bin_intervals(bins_rule.loc[bins_rule['指标代码']=='score_basic'].reset_index(drop=True))
        df1['score_basic'] = pd.cut(df1['评级结果'], bins=bin_intervals,labels=score)
        df1['score_basic']= df1['score_basic'].cat.codes.astype(float)
        df1.loc[df1['score_basic'] == -1, 'score_basic'] = 32.75
        
        for i in range(1,len(score)):
            df1.loc[df1['score_basic'] == i, 'score_basic'] = score[i]
            
    elif factor_name=='score_issr':
        bin_intervals,score=get_bin_intervals(bins_rule.loc[bins_rule['指标代码']==factor_name].reset_index(drop=True))
        
        df1['score_issr'] = pd.cut(df1['issr_ratio_ffilled'], bins=bin_intervals,labels=score)
        df1['score_issr']= df1['score_issr'].cat.codes.astype(float)
        df1.loc[df1['score_issr'] == -1, 'score_issr'] = 33.58
        
        for i in range(1,len(score)):
            df1.loc[df1['score_issr'] == i, 'score_issr'] = score[i]
        
    return df1  

def get_bins_mapping_result_local(df,bins_rule,factor_name):

    df1=df[['time','b_info_issuer','benchmark_region_ratio2']].copy(deep=True) # return 到一个新的dataframe里
    def get_bin_intervals(rule_i):
         score=rule_i['指标分数'].tolist()
         intervals=rule_i['最小值'].tolist()
         intervals.append(rule_i['最大值'].tolist()[-1])
         
         return intervals,score   
 
    if factor_name=='score_basic':
        bin_intervals,score=get_bin_intervals(bins_rule.loc[bins_rule['指标代码']=='score_basic'].reset_index(drop=True))
        df1['score_basic'] = pd.cut(df1['评级结果'], bins=bin_intervals,labels=score)
        df1['score_basic']= df1['score_basic'].cat.codes.astype(float)
        df1.loc[df1['score_basic'] == -1, 'score_basic'] = 32.75
        
        for i in range(1,len(score)):
            df1.loc[df1['score_basic'] == i, 'score_basic'] = score[i]
            
    elif factor_name=='score_issr':
        bin_intervals,score=get_bin_intervals(bins_rule.loc[bins_rule['指标代码']==factor_name].reset_index(drop=True))
        
        df1['score_issr'] = pd.cut(df1['benchmark_region_ratio2'], bins=bin_intervals,labels=score)
        df1['score_issr']= df1['score_issr'].cat.codes.astype(float)
        df1.loc[df1['score_issr'] == -1, 'score_issr'] = 33.58
        
        for i in range(1,len(score)):
            df1.loc[df1['score_issr'] == i, 'score_issr'] = score[i]
        
    return df1  
if __name__ == "__main__":
    
    starttime = datetime.datetime.now()
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
    
    db_info=pd.DataFrame({'ibond':['10.173.69.9','ibond_select','123456',33063,'ibond'],
                          'wind':['10.173.69.50','allsearch','allsearch~!@',3306,'wind'],
                          'ths':['10.173.69.50','allsearch','allsearch~!@',3306,'jqka'],
                          'zax':['10.173.69.50','allsearch','allsearch~!@',3306,'dp_require_data'],
                          'news':['10.173.69.52','allsearch','allsearch~!@',3306,'ibond'],
                          'price':['10.173.69.52','allsearch','allsearch~!@',3306,'ibond']
                          },
                         index=['host', 'user', 'passwd','port','db'])
    
    
    target_date=(datetime.datetime.now()- datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    # ------------------------------------------------------------------------
    #                                 发行利率
    # ------------------------------------------------------------------------
    # 1.数据读取&预处理 ->指标表
    # ib-债券主体映射关系
    bond_mapping=get_Mapping_Dict(db_info)
    bond_mapping.to_pickle(output_dict['X_Issr']+'bond_mapping.pkl')
    
    # wind 发行利率
    issuing_rate_info=get_Issuing_Rate(target_date, db_info)
    issuing_rate_info.to_pickle(output_dict['X_Issr']+'[issuing_rate_info].pkl')
    
    # 2.发行利率指标计算
    issuing_rate_info=pd.read_pickle(output_dict['X_Issr']+'issuing_rate_info.pkl')
    issr_preprocessed, issr_data_1y,issr_data=data_preprocess(target_date,issuing_rate_info)
    issr_preprocessed.to_pickle(output_dict['X_Issr']+'issr_preprocessed.pkl')
    issr_data_1y.to_pickle(output_dict['X_Issr']+'issr_data_1y.pkl')
    issr_data.to_pickle(output_dict['X_Issr']+'issr_data.pkl')
    
    # 3.发行利率指标分数计算
    issr_data=pd.read_pickle(output_dict['X_Issr']+'issr_data.pkl')
    
    # 拼接预警主体范围
    entity_res_t=pd.read_pickle(output_dict['model']+'entity_res_t.pkl')
    ib_entity_list=entity_res_t['主体名称'].unique()
    issr_data_merged=pd.merge(pd.DataFrame(ib_entity_list,columns=['主体名称']),issr_data,left_on='主体名称',right_on='b_info_issuer',how='left')
    
    bins_rule=pd.read_excel(input_path+'bins_rule.xlsx')
    score_issr=get_bins_mapping_result(issr_data_merged, bins_rule, 'score_issr')
    score_issr.to_pickle(output_dict['X_Issr']+'score_issr.pkl')
    

        
    #计算运行时间
    endtime = datetime.datetime.now()
    print('总运行时间：'+str(endtime-starttime))