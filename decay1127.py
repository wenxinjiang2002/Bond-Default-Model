# -*- coding: utf-8 -*-
"""

衰减1103.py整合

@author: Jiang
"""
import pandas as pd
import datetime
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings("ignore")

start=datetime.datetime.now()
pd.set_option('mode.chained_assignment', None)
plt.rcParams['font.sans-serif']=['SimHei'] #用来正常显示中文标签
plt.rcParams['axes.unicode_minus']=False #用来正常显示负号

def decay_factor(t, N0, m, option):
    # 和衰减0921.py相比，exponential_decay 中已经分区间，所以这里不用再分
    # 和衰减1026.py相比，增加option参数，区分舆情和量价的衰减机制
    if option=='news':
        if N0>4:
            N_finish=0.2
            alpha = np.log(7 / N_finish) / m
            
            decay = np.exp(-alpha * (m-t-1))
            gap=np.exp(-alpha * (m-1))*N0
            decay_factor=-decay*N0+N0+gap
            
        elif N0<=4:
            N_finish=0.1
            alpha = np.log(2 / N_finish) / m
            decay = np.exp(-alpha * t)
            decay_factor=decay*N0
            
    elif option=='price':
        N_finish=0.2
        alpha = np.log(5 / N_finish) / m
        decay = np.exp(-alpha * (m-t-1))
        gap=np.exp(-alpha * (m-1))*N0
        decay_factor=-decay*N0+N0+gap

    return decay_factor

def vectorized_decay_factor(t, N0, m, option):
    # 初始化输出数组
    decay_factor = np.zeros_like(t, dtype=float)

    # 针对 'news' 选项的计算
    if option == 'news':
        alpha_high = np.log(7 / 0.2) / m
        alpha_low = np.log(2 / 0.1) / m

        high_mask = N0 > 4
        low_mask = ~high_mask

        decay_high = np.exp(-alpha_high * (m - t - 1))
        gap_high = np.exp(-alpha_high * (m - 1)) * N0
        decay_factor[high_mask] = -decay_high[high_mask] * N0[high_mask] + N0[high_mask] + gap_high[high_mask]

        decay_low = np.exp(-alpha_low * t)
        decay_factor[low_mask] = decay_low[low_mask] * N0[low_mask]

    # 针对 'price' 选项的计算
    elif option == 'price':
        alpha = np.log(5 / 0.2) / m
        decay = np.exp(-alpha * (m - t - 1))
        gap = np.exp(-alpha * (m - 1)) * N0
        decay_factor = -decay * N0 + N0 + gap

    return decay_factor


# 定义衰减公式
def compute_weighted_score(s,entity,now,window_size,option):
    # 往前推w天
    ago = now - datetime.timedelta(days=window_size-1)
    s['t'] = now - s['日期']
    s['t_days'] = s['t'].apply(lambda x: x.days)
    window = s[(s['日期'] >= ago) & (s['日期'] <= now)].reset_index(drop=True)

    # 滑动窗口内未触发（无分数）
    if window.empty:
        if option == 'news':
            # ['日期','主体名称','二级标签','得分','t','t_days']
            window.loc[0] = [now, entity, np.nan, 0, np.nan, np.nan]
        elif option == 'price':
            # ['日期','主体名称','得分','t','t_days']
            window.loc[0] = [now, entity, 0, np.nan, np.nan]
        window['decay_factor'] = np.nan
        window['decayed_score'] = 0

    else:
        window['decayed_score'] = window.apply(lambda x: decay_factor(x['t_days'], x['得分'], option=option, m=window_size), axis=1)

    return  window['decayed_score'].sum(), window['得分'].sum()

def vectorized_compute_weighted_score(s, entity, date_range, window_size, option):
    ago = date_range - pd.Timedelta(days=window_size - 1)
    date_diffs = date_range.values[:, None] - s['日期'].values[None, :]
    t_days = date_diffs / np.timedelta64(1, 'D')
    t_days = np.clip(t_days, 0, None)  # 确保天数不是负数

    N0 = np.tile(s['得分'].values, (len(date_range), 1))
    rows, cols = N0.shape
    mask = np.triu_indices(rows, k=1, m=cols)
    N0[mask] = 0
    t_days_window = np.clip(t_days, 0, window_size - 1)
    N0_window = np.where(t_days <= window_size - 1, N0, 0)

    decayed_scores = vectorized_decay_factor(t_days_window, N0_window, window_size, option)
    total_decayed_scores = decayed_scores.sum(axis=1).reshape(-1,1)
    total_undecayed_scores = N0.sum(axis=1).reshape(-1,1)

    return total_decayed_scores,total_undecayed_scores

def decay(score_i,entity_name,window_size,option):
    score_i = score_i.sort_values(by="日期").reset_index(drop=True)
    df_decay=pd.DataFrame()
    start=score_i['日期'].iloc[0]
    # 扩展一个window_size(如果最后一个时间戳有分数需要衰减)
    check = score_i['日期'].iloc[-1]
    end=score_i['日期'].iloc[-1]+datetime.timedelta(days=window_size)
    
    df_decay['日期'] = pd.date_range(start=start, end=end, freq="D")
    df_decay['主体名称']=entity_name
    df_decay[['衰减得分','未衰减得分']]=df_decay.apply(lambda x: compute_weighted_score(score_i, entity_name, x['日期'], window_size,option), axis=1, result_type="expand")

    # print(df_decay)
    return df_decay    

def vectorized_decay(score_i, entity_name, window_size, option):
    score_i = score_i.sort_values(by="日期").reset_index(drop=True)
    start = score_i['日期'].iloc[0]
    end = score_i['日期'].iloc[-1] + datetime.timedelta(days=window_size)
    date_range = pd.date_range(start=start, end=end, freq="D")

    full_timeline = pd.DataFrame({'日期': date_range})
    score_i_full = full_timeline.merge(score_i, on='日期', how='left')
    score_i_full['得分'].fillna(0, inplace=True)
    score_i_full['主体名称'].fillna(entity_name, inplace=True)
    score_i_full = score_i_full.sort_values(by="日期").reset_index(drop=True)

    # 调用向量化函数来计算衰减得分和未衰减得分
    decay_scores,un_decay_scores = vectorized_compute_weighted_score(score_i_full, entity_name, date_range, window_size, option)
    decay_scores = decay_scores.ravel()
    un_decay_scores = un_decay_scores.ravel()
    # 构造结果 DataFrame
    df_decay = pd.DataFrame({
        '日期': date_range,
        '主体名称': entity_name,
        '衰减得分': decay_scores,
        '未衰减得分': un_decay_scores
    })

    return df_decay

# 遍历所有主体(舆情及司法，展期捕捉到的主体)
def looping_news(score,w,option,importances):
    entity_list = (score['主体名称'].unique()).tolist()

    # 创建一个空的DataFrame来存储结果
    result_df = pd.DataFrame()  
    
    for idx,e in enumerate(entity_list):
        print('current entity:\n',e)
        print('current: ',idx+1)
        print('total: ',len(entity_list))
        score_i = score[score['主体名称'] == e].reset_index(drop=True)
        important_tags = set(importances['标签'])
        important_data = score_i[score_i['二级标签'].isin(important_tags)]
        unimportant_data = score_i[~score_i['二级标签'].isin(important_tags)]

        if not important_data.empty:
            w1 = 365
            entity_decay_important = decay(important_data, e, w1, option)
        else:
            entity_decay_important = pd.DataFrame(columns=['日期', '主体名称', '衰减得分', '未衰减得分'])  # 指定你的列名
            entity_decay_important = entity_decay_important.applymap(lambda x: None)

        if not unimportant_data.empty:
            w1 = w
            entity_decay_unimportant = decay(unimportant_data, e, w1, option)
        else:

            entity_decay_unimportant = pd.DataFrame(columns=['日期', '主体名称', '衰减得分', '未衰减得分'])  # 指定你的列名
            entity_decay_unimportant = entity_decay_unimportant.applymap(lambda x: None)
        entity_decay = pd.merge(entity_decay_important,entity_decay_unimportant,on=['日期', '主体名称'],how = 'outer')
        entity_decay.fillna(0,inplace=True)
        entity_decay['最终衰减得分'] = entity_decay.apply(lambda row: row['衰减得分_x']+row['衰减得分_y'],axis=1)
        entity_decay['最终未衰减得分'] = entity_decay.apply(lambda row:row['未衰减得分_x']+row['未衰减得分_y'],axis=1)
        result_df = pd.concat([result_df, entity_decay], axis=0, ignore_index=True)

    return result_df
    
def looping_price(score,w,option):
    entity_list = (score['主体名称'].unique()).tolist()
    
    # 创建一个空的DataFrame来存储结果
    result_df = pd.DataFrame()  
    
    for idx,e in enumerate(entity_list):
        print('current entity:\n',e)
        print('current: ',idx+1)
        print('total: ',len(entity_list))
        score_i = score[score['主体名称'] == e].reset_index(drop=True)
        entity_decay = vectorized_decay(score_i, e, w,option)
        result_df = pd.concat([result_df, entity_decay],axis=0,ignore_index=True)
    
    
    return result_df
