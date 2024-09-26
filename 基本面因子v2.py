# -*- coding: utf-8 -*-
"""
基于基本面因子.py改动实现增量跑批
data_preprocess - 基本面数据预处理，分箱

@author: Jiang
"""
import datetime
import pandas as pd
import pymysql

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

def get_Fundamental(target_date,db_info):
    
    # 获取最新评级结果
    sql="""select a.name "主体名称", 
    a.organization_code as "统一授信编码" , 
    a.grade_score '最终得分',
    a.grade_result "评级结果",
    a.`year`,
    sd.dict_value "敞口"
    from data_cus_grade a
    inner join sys_dict sd on sd.dict_group ="industry" and a.industry_id = sd.dict_key 
    where 1=1
    and a.`year` = (select max(`year`)  from data_cus_grade)
    and (a.industry_id >=79 or a.industry_id =52);
    """
    basic_info=get_df_from_db(db_info.loc['host','ibond'],db_info.loc['user','ibond'],db_info.loc['passwd','ibond'],
                                 db_info.loc['db','ibond'],db_info.loc['port','ibond'],sql)
    
    return basic_info

def get_bins_mapping_result(df,bins_rule,factor_name):
    
    df1=df.copy(deep=True) # return 到一个新的dataframe里
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

if __name__ == "__main__":
    
    starttime = datetime.datetime.now()
    # -------------------------------------------------------------------------

    # output path
    folder_path='C:/Users/erlu/Documents/债券预警模型/P0/code整合_1204/增量/'
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
    
    db_info=pd.DataFrame({'ibond':['10.173.69.50','devops','devops@2024',3306,'ibond'],
                          'wind':['10.173.69.50','allsearch','allsearch~!@',3306,'wind'],
                          'ths':['10.173.69.50','allsearch','allsearch~!@',3306,'jqka'],
                          'zax':['10.173.69.50','allsearch','allsearch~!@',3306,'dp_require_data'],
                          'news':['10.173.69.52','allsearch','allsearch~!@',3306,'information_schema']
                          },
                         index=['host', 'user', 'passwd','port','db'])
    
    target_date=(datetime.datetime.now()- datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    
    # 1.数据读取&预处理 ->指标表
    fundamental_info=get_Fundamental(target_date, db_info)
    fundamental_info.to_pickle(output_dict['X_Fdmt']+'fundamental_info.pkl')
    
    # 2.基本面（分箱结果）赋分
    fundamental_info=pd.read_pickle(output_dict['X_Fdmt']+'fundamental_info.pkl')
    bins_rule=pd.read_excel(input_path+'bins_rule.xlsx')
    
    # 同一主体不同敞口评级结果不同，取max
    fundamental_info=fundamental_info.sort_values(by='评级结果',ascending=False).reset_index(drop=True)
    fundamental_info_unique=fundamental_info.groupby(['主体名称'])['评级结果'].max().reset_index(drop=False)

    # 拼接预警主体范围
    entity_res_t=pd.read_pickle(output_dict['model']+'entity_res_t.pkl')
    ib_entity_list=entity_res_t['主体名称'].unique()
    fundamental_info_merged=pd.merge(pd.DataFrame(ib_entity_list,columns=['主体名称']),fundamental_info_unique,on='主体名称',how='left')
    
    score_basic=get_bins_mapping_result(fundamental_info_merged, bins_rule, 'score_basic')
    score_basic.to_pickle(output_dict['X_Fdmt']+'score_basic.pkl')

    #计算运行时间
    endtime = datetime.datetime.now()
    print('总运行时间：'+str(endtime-starttime))
