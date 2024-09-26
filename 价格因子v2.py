import datetime
import pandas as pd
import numpy as np
import os
import decay1127 as decay
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

def get_Price_info(target_date,db_info):
    dt_target_date=datetime.datetime.strptime(target_date,'%Y-%m-%d')
    ago = (datetime.date(dt_target_date.year-1,dt_target_date.month,dt_target_date.day)).strftime('%Y-%m-%d')
    
    sql='select b.compName,a.newsTitle, a.newsTime \
        from warn_news a \
        inner join warn_entityinfo b \
        on a.entity_id = b.id \
        where a.showCode = \'交易监测\' \
        and a.newsTime'+'>='+ '\''+ago +'\' and a.newsTime'+'<='+ '\''+target_date +'\''
        
    price_info=get_df_from_db(db_info.loc['host','price'],db_info.loc['user','price'],db_info.loc['passwd','price'],
                                 db_info.loc['db','price'],db_info.loc['port','price'],sql)
    
    return price_info

def data_preprocess(df):
    # 识别newsTitle中带有'偏离'的记录
    keyword='偏离'
    df_contains=df[df['newsTitle'].str.contains(keyword)]
    
    df_contains[df_contains['newsTitle'].str.contains('收益率')]
    
    def split_func(row):
        
        # 1. 分离债券简称
        separated_row=re.split('：', row)
        if len(separated_row)==0: # 识别失败
            b_name=np.nan
            abnormal_type=np.nan
            value=np.nan
            amount=np.nan
        else:
            b_name=separated_row[0]
            
        # 2. 获取异常波动类型 {成交价/收益率}
            separated_type=re.findall(r'最新(.*?)偏离',separated_row[1])
            if len(separated_type)==0: # 识别失败
                abnormal_type=np.nan
                value=np.nan
                amount=np.nan
            else:
                abnormal_type=separated_type[0]
                
        # 3. 根据类型获取波动幅度
                if abnormal_type=='成交价':
                    separated=re.findall(r'约(.*?)BP',separated_row[1])
                    if len(separated)>0:
                        value=separated[0]
                    else:
                        value=np.nan
                elif abnormal_type=='收益率':
                    separated=re.findall(r'约(.*?)%',separated_row[1])
                    if len(separated)>0:
                        value=separated[0]
                    else:
                        value=np.nan
            
        # 4.获取成交金额
                separated_amount=re.findall(r'成交金额(.*?)万元',separated_row[1])
                if len(separated_amount)>0:
                    amount=separated_amount[0]
                else:
                    amount=np.nan
        
        return pd.Series([b_name,abnormal_type,value,amount],index=['债券简称','异动类型','偏离幅度','成交金额(万元)'])

    df_contains[['债券简称','异动类型','偏离幅度','成交金额(万元)']]=df_contains['newsTitle'].apply(lambda x: split_func(x))
    
    return df_contains

# 批量读取文件夹下多个excel文件
def read_File(folder_path, h=0):
    # 存储所有Excel文件的DataFrame的列表
    all_dataframes = []

    # 遍历文件夹中的所有文件
    for filename in os.listdir(folder_path):
        if filename.endswith('.xlsx'):
            file_path = os.path.join(folder_path, filename)
            # 读取Excel文件并将其添加到列表中
            df = pd.read_excel(file_path, header=h, engine='openpyxl')
            all_dataframes.append(df)

    # 合并所有DataFrame成一个大的DataFrame
    merged_df = pd.concat(all_dataframes, ignore_index=True)

    return merged_df

def get_Price(input_path):
    
    # 导入本地异常波动xlsx文件
    folder_path_price=input_path+'价格偏离昨收/'
    folder_path_return=input_path+'收益率偏离中债估值/'
    df_price=read_File(folder_path_price,h=1)
    df_return=read_File(folder_path_return,h=1) #预警通数据来源在column name行上一行，所以header=1
    
    return df_price,df_return

def get_Price_Score(df_return,df_price,w_price):
    
    # 计算异常波动最终得分
    def cal_Final(df_return,df_price):
        
        def cal_Return_Score(df):
            # 获取所有需要改为浮点数的列名
            column_names=['偏离幅度(BP)']
            # 遍历所有列，将数值字段转换为浮点数
            for column in column_names:
                df[column] = pd.to_numeric(df[column], errors='coerce')

            # 根据F列进行打分
            def assign_score(row):
                value = row['偏离幅度(BP)']
                if 100 <= value < 200:
                    return 1
                elif 200 <= value < 500:
                    return 2
                elif 500 <= value < 1000:
                    return 3
                elif value >= 1000:
                    return 4
                else:
                    return 0

            df['打分'] = df.apply(assign_score, axis=1)
            # 计算每个债项的当日交易分数
            sum_scores = df.groupby(['债券简称','成交日期','发债主体'])['打分'].max().reset_index()
            
            return sum_scores

        def cal_Price_Score(df):
            # 获取所有需要改为浮点数的列名
            column_names = ['偏离幅度(%)','前收盘价(元)','成交价(元)']

            # 遍历所有列，将数值字段转换为浮点数
            for column in column_names:
                df[column] = pd.to_numeric(df[column], errors='coerce')

            # 筛选前收盘价高于98元且成交价高于90元的净价成交，删除,并替换原始df
            df.drop(df[(df['前收盘价(元)'] > 98) & (df['成交价(元)'] > 90)].index,inplace=True) 
            
            # 根据F列进行打分
            def assign_score(row):
                value = row['偏离幅度(%)']
                if -20 < value <= -10:
                    return 1.5
                elif -35 < value <= -20:
                    return 3
                elif -50 < value <= -35:
                    return 4
                elif value <= -50:
                    return 5
                else:
                    return 0

            df['打分'] = df.apply(assign_score, axis=1)

            # 计算每个债项的当日交易分数
            sum_scores = df.groupby(['债券简称','成交日期','发债主体'])['打分'].max().reset_index()
            
            return sum_scores

        # 计算收益率偏离&价格偏离打分
        return_score=cal_Return_Score(df_return)
        price_score=cal_Price_Score(df_price)
        
        df_final=pd.merge(price_score,return_score,on=['债券简称','成交日期','发债主体'],how='outer')
        df_final['打分_x'].fillna(0,inplace=True)
        df_final['打分_y'].fillna(0,inplace=True)
        df_final['打分']=df_final['打分_x']+df_final['打分_y']

        # 设置分数上限为10分
        df_final['打分']= df_final['打分'].apply(lambda x: min(x, 10))
            
        return df_final

    df_final=cal_Final(df_return, df_price)

    # 叠加加分机制
    def score_adding(score,bond,entity,w_price):
        # start=score['成交日期']
        score = score.reset_index(drop=True)
        score = score.sort_values(by=['成交日期'],ascending=[True])
        start=score['成交日期'].iloc[0]
        end=score['成交日期'].iloc[-1]
        # 各债预处理
        df=pd.DataFrame()
        df['成交日期'] = pd.date_range(start=start, end=end, freq="D") 
        df['债券简称']=bond
        df['发债主体']=entity
        df = pd.merge(df,score,on=['债券简称','成交日期','发债主体'],how='left')
        df['打分'].fillna(0,inplace=True)
        df['最终打分']=0
        if len(df)>=w_price:
            
            for i in range(0,w_price):
                df['最终打分'][i]=df['打分'][i]
                
            for i in range(w_price,len(df)): 
                max_score=max(df['打分'][i-w_price:i-1])
                if df['打分'][i]>max_score:
                    df['最终打分'][i]=df['打分'][i]
                else:
                    pass
                
        else:
            for i in range(0,len(df)):
                df['最终打分'][i]=df['打分'][i]
            
        return df
    
    def looping(score,w_price):
        bond_list = (score['债券简称'].unique()).tolist()
        df_score=pd.DataFrame()
        for idx,b in enumerate(bond_list):
            entity=score[score['债券简称']==b]['发债主体'].reset_index(drop=True)[0]
            print('current bond:\n',b)
            print('entity name: ',entity)
            print('current: ',idx)
            print('total: ',len(bond_list))
            df_score_i=score_adding(score[score['债券简称']==b],b,entity,w_price)
            df_score=pd.concat([df_score,df_score_i],axis=0,ignore_index=True)
        return df_score

    df_final['成交日期']=pd.to_datetime(df_final['成交日期'],format='%Y-%m-%d')
    df_score=looping(df_final,w_price)
    
    return df_final,df_score

def get_Decayed_Price(score,w):
    
    score['成交日期'] = pd.to_datetime(score['成交日期'], format="%Y-%m-%d")
    score=score.sort_values(by = '成交日期')
    
    # 按日期和主体名称分组，选择每个分组中最高分数的行
    price_score_entity = score.groupby(['成交日期', '发债主体'])['最终打分'].max().reset_index()
    price_score_entity.columns = ['日期', '主体名称', '得分']
    
    # 遍历所有主体进行衰减
    result_price=decay.looping_price(price_score_entity,w,option='price')
    
    return result_price

def compress_Decayed_Price(result_price):
    
    # 对衰减后得分进行压缩
    # news_factor最小值和最大值
    min_price_factor = result_price['衰减得分'].min()
    max_price_factor = result_price['衰减得分'].max()
    # 0-100 news_factor
    result_price['compressed_value'] = ((result_price['衰减得分'] - min_price_factor) / (max_price_factor - min_price_factor)) * 100

    result_price.drop(columns=['衰减得分', '未衰减得分'],inplace=True)
    
    return result_price

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
    
    
    target_date=(datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    w_price=365
    
    # 步骤1，2的数据未来会导入库里，直接在步骤3提取即可，无需预处理，只需要修改库里数据读取以及处理成衰减前的格式
    
    # 1.数据读取&预处理 ->指标表
    # ---从数据库读取
    # price=
    # price_info=price.get_Price_info(target_date, db_info)
    # price_info.to_pickle(output_dict['X_Price']+'price_info.pkl')
    
    # price_info=pd.read_pickle(output_dict['X_Price']+'price_info.pkl')
    # price_processed=price.data_preprocess(price_info)
    # price_processed.to_pickle(output_dict['X_Price']+'price_processed.pkl')
    
    # # 2.价格得分计算
    # # ---从本地读取
    # df_price=pd.read_csv(input_path+'价格偏离昨收_total.csv',encoding='gbk',index_col=False)
    # df_return=pd.read_csv(input_path+'收益率偏离中债估值_total.csv',encoding='gbk',index_col=False)
    
    # df_final,score_price_initial=price.get_Price_Score(df_return, df_price, w_price)
    # df_final.to_pickle(output_dict['X_Price']+'成交估值打分最终结果文件.pkl')
    # score_price_initial.to_pickle(output_dict['X_Price']+'score_price_initial.pkl')
    
    # 3. 价格得分衰减
    score_price_initial=pd.read_pickle(input_path+'成交估值打分最终结果文件(new).pkl')

    # 仅取预警范围内的主体进行衰减
    entity_res_t=pd.read_pickle(output_dict['model']+'entity_res_t.pkl')
    ib_entity_list=entity_res_t['主体名称'].unique()
    score_price_to_decay=score_price_initial.loc[score_price_initial['发债主体'].isin(ib_entity_list)]

    score_price_d=get_Decayed_Price(score_price_to_decay, w_price)
    score_price_d.to_pickle(output_dict['X_Price']+'score_price_d.pkl')
    
    # 4. 价格得分衰减后rescale
    score_price_d=pd.read_pickle(output_dict['X_Price']+'score_price_d.pkl')
    score_price_d_c=compress_Decayed_Price(score_price_d)
    score_price_d_c.to_pickle(output_dict['X_Price']+'score_price_d_c.pkl')
    
    # 5. 获取当日价格衰减，压缩后得分
    score_price=score_price_d_c.loc[score_price_d_c['日期']==datetime.datetime.strptime(target_date,'%Y-%m-%d')].reset_index(drop=True)
    
    # 拼接预警主体范围
    score_price=pd.merge(pd.DataFrame(ib_entity_list,columns=['主体名称']),score_price,on='主体名称',how='left')
    score_price['compressed_value'].fillna(value=0,inplace=True)
    
    score_price.to_pickle(output_dict['X_Price']+'score_price.pkl')
        
    #计算运行时间
    endtime = datetime.datetime.now()
    print('总运行时间：'+str(endtime-starttime))
