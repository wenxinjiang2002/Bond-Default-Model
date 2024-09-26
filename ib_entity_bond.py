# -*- coding: utf-8 -*-
"""
Created on Thu Jan 25 11:30:19 2024

获取当前ib预警的主体和债券范围

@author: erlu
"""

import pandas as pd
import pymysql
import datetime

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

# 当日系统主体预警等级
def get_Entity_sys_t(target_date,db_info):

    sql='select \
        we.compName \'主体名称\',\
        ws.level_10 \'预警等级\',\
        ws.sigscore \'预警得分\'\
        from warn_entityscore ws \
        inner join warn_entityinfo we on we.id=ws.entity_id \
        where ws.carry_datetime = \''+target_date +'\' and ws.level_10 >=0'
    
    entity_res_t=get_df_from_db(db_info.loc['host','ibond'],db_info.loc['user','ibond'],db_info.loc['passwd','ibond'],
                                 db_info.loc['db','ibond'],db_info.loc['port','ibond'],sql) 
    
    return entity_res_t

# 用于等级平滑，提取target_date前180天的系统主体预警等级
def get_180Entity_sys_t(target_date,db_info):
    
    end_date = target_date    
    start_date = (datetime.datetime.strptime(target_date, "%Y-%m-%d") 
                  - datetime.timedelta(days=180)).strftime("%Y-%m-%d")

    sql='select \
        we.compName \'主体名称\',\
        ws.carry_datetime \'日期\',\
        ws.level_10 \'预警等级\',\
        ws.sigscore \'预警得分\'\
        from warn_entityscore ws \
        inner join warn_entityinfo we on we.id=ws.entity_id \
        where ws.carry_datetime >= \''+start_date +'\' \
            and ws.carry_datetime <= \''+end_date +'\'\
            and ws.level_10 >=0'
    
    entity_180res_t=get_df_from_db(db_info.loc['host','ibond'],db_info.loc['user','ibond'],db_info.loc['passwd','ibond'],
                                 db_info.loc['db','ibond'],db_info.loc['port','ibond'],sql) 
    
    return entity_180res_t

# 用于Excel分析，提取target_date前30天的系统主体预警等级
def get_30Entity_sys_t(target_date,db_info):
    
    end_date = target_date    
    start_date = (datetime.datetime.strptime(target_date, "%Y-%m-%d") 
                  - datetime.timedelta(days=30)).strftime("%Y-%m-%d")

    sql='select \
        we.compName \'主体名称\',\
        ws.carry_datetime \'日期\',\
        ws.level_10 \'预警等级\',\
        ws.sigscore \'预警得分\'\
        from warn_entityscore ws \
        inner join warn_entityinfo we on we.id=ws.entity_id \
        where ws.carry_datetime >= \''+start_date +'\' \
            and ws.carry_datetime <= \''+end_date +'\'\
            and ws.level_10 >=0'
    
    entity_180res_t=get_df_from_db(db_info.loc['host','ibond'],db_info.loc['user','ibond'],db_info.loc['passwd','ibond'],
                                 db_info.loc['db','ibond'],db_info.loc['port','ibond'],sql) 
    
    return entity_30res_t


def get_Bond_sys_t(target_date,db_info):
    
    sql='select \
        wb.bond_code \'债券编码\',\
        wb2.bond_name \'债券名称\',\
        wb.level_10 \'预警等级\',\
        wb.sigscore \'预警得分\',\
        wb2.s_info_windcode \'wind债券编码\'\
        from warn_bondsigscore wb \
        inner join warn_bondinfo wb2 on wb.bond_code = wb2.bond_code \
        where wb.carry_datetime = \''+target_date +'\''


    bond_res_t=get_df_from_db(db_info.loc['host','ibond'],db_info.loc['user','ibond'],db_info.loc['passwd','ibond'],
                                 db_info.loc['db','ibond'],db_info.loc['port','ibond'],sql) 
    
    
    return bond_res_t


def get_Bond_Entity_Dict(db_info):
    
    sql='select \
        we.compName \'主体名称\',\
        wb.bond_code \'债券编码\',\
        wb.bond_name \'债券名称\',\
        wb.type \'是否到期\' \
        from warn_bondinfo wb \
        inner join warn_entityinfo we on wb.entity_id = we.id '
    # wb.type 1：存续期 2到期
    
    b_e_dict=get_df_from_db(db_info.loc['host','ibond'],db_info.loc['user','ibond'],db_info.loc['passwd','ibond'],
                                 db_info.loc['db','ibond'],db_info.loc['port','ibond'],sql) 
    
    return b_e_dict
    