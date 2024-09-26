# -*- coding: utf-8 -*-
"""
Created on Tue Jan 23 14:29:37 2024

基于舆情因子.py改动实现增量跑批

@author: erlu
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Dec  4 17:04:27 2023

data_preprocess - 舆情数据预处理，打分，衰减，分值区间缩放至[0,100]

@author: erlu
"""
import datetime
import pandas as pd
import openpyxl
import decay1127 as decay
import os
import pymysql
import re

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

# 舆情得分
def get_News_Info(target_date,db_info):
    dt_target_date=datetime.datetime.strptime(target_date,'%Y-%m-%d')
    ago = (datetime.date(dt_target_date.year-1,dt_target_date.month,dt_target_date.day)).strftime('%Y-%m-%d')
    
    sql='select d.compName,a.newsTime, c.name, a.score \
    from warn_news a \
    inner join warn_newsentity b on a.id = b.news_id \
    inner join warn_newstags c on a.id = c.newsid \
    inner join warn_entityinfo d on b.entity_id = d.id \
    where a.negative = \'是\' and b.relationtype = 1 and b.isvalid = 1 and c.`type` = 1 \
    and a.newsTime'+'>='+ '\''+ago +'\' and a.newsTime'+'<='+ '\''+target_date +'\''
    
    news_info=get_df_from_db(db_info.loc['host','news'],db_info.loc['user','news'],db_info.loc['passwd','news'],
                                  db_info.loc['db','news'],db_info.loc['port','news'],sql) 
    
    return news_info



# 舆情标签体系（新闻+公告）
def get_Tags_Info(db_info):
    
    sql='select * from review_tag_info'
    
    tags_info=get_df_from_db(db_info.loc['host','news'],db_info.loc['user','news'],db_info.loc['passwd','news'],
                                 db_info.loc['db','news'],db_info.loc['port','news'],sql)

    return tags_info

def get_Warn_Breachinfo(target_date, db_info):
    dt_target_date=datetime.datetime.strptime(target_date,'%Y-%m-%d')
    ago = (datetime.date(dt_target_date.year-1,dt_target_date.month,dt_target_date.day)).strftime('%Y-%m-%d')
    sql='select * \
        from warn_breachinfo \
        where happen_date '+'>='+ '\''+ago +'\' and happen_date'+'<='+ '\''+target_date +'\''
    
    breach_info=get_df_from_db(db_info.loc['host','ibond'],db_info.loc['user','ibond'],db_info.loc['passwd','ibond'],
                                 db_info.loc['db','ibond'],db_info.loc['port','ibond'],sql)

    return breach_info

def data_preprocess(bonds_record,panelties_record,target_date):

    def panelties_tag(panelties_record):
        panelties_record.loc[panelties_record['裁判文书'] == '√', '裁判文书'] = '_裁判文书'
        panelties_record.loc[panelties_record['破产重整'] == '√', '破产重整'] = '_破产重整'
        panelties_record.loc[panelties_record['被执行人'] == '√', '被执行人'] = '_被执行人'
        panelties_record.loc[panelties_record['失信被执行人']
                             == '√', '失信被执行人'] = '_失信被执行人'
        panelties_record.loc[panelties_record['终本案件']=='√','终本案件']='_终本案件'
        panelties_record.columns = [
            '企业名称', '时间', '裁判文书', '裁判文书时间', '裁判文书年份', '终本案件', '终本案件时间', '终本案件年份',\
             '破产重整', '破产重整时间', '破产重整年份', '被执行人', '被执行人时间', '被执行人年份', '当年净资产', \
             '执行金额占净资产比例', '失信被执行人', '失信被执行人时间', '失信被执行人年份', '案由', '文书标题', '诉讼地位']
        
        panelties_tag = pd.DataFrame()
        tag_list=['裁判文书','破产重整','被执行人','失信被执行人','终本案件']
        for t in tag_list:
            for i in range(len(panelties_record)):
                if panelties_record[t].iloc[i] == '_'+t:
                    tag=panelties_record[[t+'时间','企业名称',t]].iloc[i]
                    df_tag=pd.DataFrame(tag).T
                    df_tag.columns=['日期','主体名称','二级标签']
                    panelties_tag=pd.concat([panelties_tag,df_tag],axis=0)
                else:
                    pass
        
        panelties_tag['日期']=pd.to_datetime(panelties_tag['日期'],format="%Y-%m-%d") 
        
        # print('panelties_tag: ',panelties_tag)
        return panelties_tag  

    def extension_tag(bonds_record):
        # 同一主体在同一天可能有大于一条债券展期标签，该主体可能存在多个债券同时展期的情况
        extension_tag=pd.DataFrame()
        
        tag_list=['本息展期','触发交叉违约','触发交叉条款','担保违约','技术性违约']
        for t in tag_list:
            for i in range(len(bonds_record)):
                if bonds_record['breach_detail'].iloc[i]==t:
                    tag=bonds_record[['happen_date','entity_name']].iloc[i]
                    df_tag=pd.DataFrame(tag).T
                    df_tag.columns=['日期','主体名称']
                    if t=='展期':
                        df_tag['二级标签']='债券展期'
                    elif t=='技术性违约':
                        df_tag['二级标签']='_技术性违约'
                    elif t=='担保违约':
                        df_tag['二级标签']='_担保违约'
                    else:
                        df_tag['二级标签']=t
                    extension_tag=pd.concat([extension_tag,df_tag],axis=0)
                else:
                    pass

        return extension_tag
    
    extension_info=extension_tag(bonds_record)
    panelties_info=panelties_tag(panelties_record)
    
    # 新增舆情标签（展期情况&监管处罚）
    # 删除源文件中没有时间戳的数据
    tags_added=pd.concat([extension_info,panelties_info],axis=0).reset_index(drop=True)
    tags_added=tags_added[pd.notnull(tags_added["日期"])]
    
    # 选取最近一年的新增舆情
    dt_target_date=datetime.datetime.strptime(target_date,'%Y-%m-%d')
    dt_ago = datetime.datetime(dt_target_date.year-1,dt_target_date.month,dt_target_date.day)

    tags_added_1y=tags_added.loc[(tags_added['日期']>=dt_ago)&(tags_added['日期']<=dt_target_date)]

    return tags_added_1y

# 计算舆情得分
def get_News_Score(df,tags_added):
    # -------------原始舆情得分过大的手动赋分-------------
    df.dropna(axis=0,how='any',subset=None,inplace=True)
    df.loc[:, 'newsTime'] = df['newsTime'].apply(lambda x: x.strftime('%Y-%m-%d')) # 改为年月日,if needed
    df['score']=df['score'].apply(lambda x: float(x)) # 将decimal改为float
    # ['compName','newsTime','name','score']
    
    # score>10分的部分，改为10分
    df['score_modified']=df['score'].copy(deep=True)
    df.loc[df['score_modified']>10,'score_modified']=10
    
    # -------------新增标签手动赋分（依照已有舆情标签打分体系）-------------
    def score_adding(new_tag):
        if new_tag=='债券展期':
            return 5
        elif new_tag=='触发交叉违约':
            return 8
        elif new_tag=='_担保违约':
            return 3
        elif new_tag=='_技术性违约':
            return 8
        elif new_tag=='_破产重整':
            return 8
        elif new_tag=='_被执行人':
            return 1.001
        elif new_tag=='_失信被执行人':
            return 1.001
        elif new_tag=='_终本案件':
            return 2
        elif new_tag=='_裁判文书':
            return 2
        else:
            print(new_tag,'未赋分')
    
    # 读取手动新增数据（展期情况&监管处罚）
    # tags_added=pd.read_csv(output_path['X_News']+'tags_added.csv',encoding='gbk',index_col=False)
    # # tags_added=pd.read_csv(output_dict['X_News']+'tags_added.csv',encoding='gbk',index_col=False)
    tags_added['舆情得分'] = tags_added.apply(lambda x: score_adding(x['二级标签']), axis=1)
    # ['日期','主体名称','二级标签','舆情得分']

    # 拼接web&新增舆情得分
    df=df[['newsTime','compName','name','score_modified']]
    # 统一字段名
    df.columns=['日期','主体名称','二级标签','舆情得分']

    tags=pd.concat([df,tags_added],axis=0).reset_index(drop=True)
    
    return tags
 
# 舆情得分衰减
def get_Decayed_News(score,importances_label,w):
            
    score['日期'] = pd.to_datetime(score['日期'], format="%Y-%m-%d")
    #确保 df 已经按照日期进行了排序
    score=score.sort_values(by = '日期')
    score.columns=['日期','主体名称','二级标签','得分']
    
    # 遍历所有范围内主体进行衰减
    result_news=decay.looping_news(score,w,option='news',importances = importances_label)
        
    return result_news

def compress_News_Decayed(score_d):
    score_d_c=score_d.copy(deep=True)

    # 对衰减后得分进行压缩
    # news_factor最小值和最大值
    min_news_factor = score_d_c['最终衰减得分'].min()
    max_news_factor = 920.74
    
    # 0-100 news_factor
    score_d_c['compressed_value'] = ((score_d_c['最终衰减得分'] - min_news_factor) / (max_news_factor - min_news_factor)) * 100
    
    score_d_c.loc[score_d_c['最终衰减得分']>=920.74,'compressed_value']=100
    score_d_c.drop(columns=['衰减得分_x','未衰减得分_x','衰减得分_y','未衰减得分_y','最终衰减得分', '最终未衰减得分','最终衰减得分'],inplace=True)

    return score_d_c

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
    
    db_info=pd.DataFrame({'ibond':['10.173.69.9','ibond_select','123456',33063,'ibond'],
                          'wind':['10.173.69.50','allsearch','allsearch~!@',3306,'wind'],
                          'ths':['10.173.69.50','allsearch','allsearch~!@',3306,'jqka'],
                          'zax':['10.173.69.50','allsearch','allsearch~!@',3306,'dp_require_data'],
                          'news':['10.173.69.52','allsearch','allsearch~!@',3306,'ibond'],
                          'price':['10.173.69.52','allsearch','allsearch~!@',3306,'ibond']
                          },
                         index=['host', 'user', 'passwd','port','db'])
    
    
    target_date=(datetime.datetime.now()- datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    w_news=90
    
    # ------------------------------------------------------------------------
    #                                 舆情
    # ------------------------------------------------------------------------
    # 1.数据读取&预处理 ->指标表
    # 舆情打分
    news_info=get_News_Info(target_date, db_info)
    news_info.to_pickle(output_dict['X_News']+'news_info.pkl')
    
    # 舆情标签体系（新闻+公告）
    tags_info=get_Tags_Info(db_info)
    tags_info.to_pickle(output_dict['X_News']+'tags_info.pkl')

    # 新增-监管处罚数据&展期数据
    panelties_record = pd.read_excel(input_path+'监管处罚整理版.xlsx') 
    default_record = get_Warn_Breachinfo(target_date, db_info)
    default_record.to_pickle(output_dict['X_News']+'default_record.pkl')
    
    tags_added=data_preprocess(default_record, panelties_record,target_date)
    tags_added.to_pickle(output_dict['X_News']+'tags_added.pkl')
    
    # 2. 计算舆情得分 (新增舆情打分&原过大舆情得分重新赋分)
    news_info=pd.read_pickle(output_dict['X_News']+'news_info.pkl')
    tags_added=pd.read_pickle(output_dict['X_News']+'tags_added.pkl')
    score_news_initial=get_News_Score(news_info, tags_added)
    score_news_initial.to_pickle(output_dict['X_News']+'score_news_initial.pkl')
    
    # 3. 舆情得分衰减
    score_news_initial=pd.read_pickle(output_dict['X_News']+'score_news_initial.pkl')
    
    # 仅取预警范围内的主体进行衰减
    entity_res_t=pd.read_pickle(output_dict['model']+'entity_res_t.pkl')
    ib_entity_list=entity_res_t['主体名称'].unique()
    score_news_to_decay=score_news_initial.loc[score_news_initial['主体名称'].isin(ib_entity_list)]

    # 重要舆情标签
    importances=pd.read_excel(input_path+'importances.xlsx')                  
    score_news_d=get_Decayed_News(score_news_to_decay, importances, w_news)
    score_news_d.to_pickle(output_dict['X_News']+'score_news_d.pkl')
    
    # 4. 舆情得分衰减后压缩
    score_news_d=pd.read_pickle(output_dict['X_News']+'score_news_d.pkl')
    score_news_d_c=compress_News_Decayed(score_news_d)
    score_news_d_c.to_pickle(output_dict['X_News']+'score_news_d_c.pkl')
    
    # 5. 获取当日舆情衰减，压缩后得分
    score_news=score_news_d_c.loc[score_news_d_c['日期']==datetime.datetime.strptime(target_date,'%Y-%m-%d')].reset_index(drop=True)
    
    # 拼接预警主体范围
    score_news=pd.merge(pd.DataFrame(ib_entity_list,columns=['主体名称']),score_news,on='主体名称',how='left')
    score_news['compressed_value'].fillna(value=0,inplace=True)
    score_news.to_pickle(output_dict['X_News']+'score_news.pkl')
  
    #计算运行时间
    endtime = datetime.datetime.now()
    print('总运行时间：'+str(endtime-starttime))