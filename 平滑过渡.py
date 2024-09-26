# -*- coding: utf-8 -*-
"""
Created on Thu Feb  8 19:51:41 2024

@author: Jiang
"""
import numpy as np
import pandas as pd
from datetime import timedelta
from datetime import datetime
import os

# start_date = datetime(2024, 2, 25)
# transition_period = 180
# today = datetime(2024, 2, 26)
# transition_day = (today - start_date).days

# 旧模型的分数区间
def score_old(entity_res_t):
    if '预警等级' in entity_res_t.columns and '预警得分' in entity_res_t.columns:
        # 计算每个预警等级的分数区间
        score_ranges = entity_res_t.groupby(
            '预警等级')['预警得分'].agg(['min', 'max']).sort_index()
        print(score_ranges)
    else:
        print("预警等级或分数列不存在于数据中。")
    score_ranges = score_ranges.reset_index()
    score_ranges['预警等级'] = score_ranges['预警等级'].astype(int)
    score_ranges = score_ranges.set_index('预警等级').sort_index()
    score_ranges = score_ranges.sort_index()
    score_ranges['min_sys'] = score_ranges['min'].astype(float)
    score_ranges['max_sys'] = score_ranges['max'].astype(float)
    score_ranges_dict = {level: (row['min_sys'], row['max_sys'])
                         for level, row in score_ranges.iterrows()}
    return score_ranges_dict


# 新模型的分数区间
def score_new(entity_t):
    
    # 将日期列去除
    warn_entity=entity_t.copy(deep=True)
    warn_entity = warn_entity[['主体名称','预警等级','预警得分']]
    
    if '预警等级' in warn_entity.columns and '预警得分' in warn_entity.columns:
        # 计算每个预警等级的分数区间
        score_ranges = warn_entity.groupby(
            '预警等级')['预警得分'].agg(['min', 'max']).sort_index()
        print(score_ranges)
    else:
        print("预警等级或分数列不存在于数据中。")
    score_ranges = score_ranges.reset_index()
    score_ranges['预警等级'] = score_ranges['预警等级'].astype(int)
    score_ranges = score_ranges.set_index('预警等级').sort_index()
    score_ranges = score_ranges.sort_index()
    score_ranges['min_new'] = score_ranges['min'].astype(float)
    score_ranges['max_new'] = score_ranges['max'].astype(float)
    labeled_intervals = {level: (row['min_new'], row['max_new'])
                         for level, row in score_ranges.iterrows()}
    return labeled_intervals


def score_trans(trans_amount):
    if '预警等级_trans' in trans_amount.columns and '预警得分_trans' in trans_amount.columns:
        # 计算每个预警等级的分数区间
        score_ranges = trans_amount.groupby(
            '预警等级_trans')['预警得分_trans'].agg(['min', 'max']).sort_index()
        print(score_ranges)
    else:
        print("预警等级或分数列不存在于数据中。")
    score_ranges = score_ranges.reset_index()
    score_ranges['预警等级_trans'] = score_ranges['预警等级_trans'].astype(int)
    score_ranges = score_ranges.set_index('预警等级_trans').sort_index()
    score_ranges = score_ranges.sort_index()
    score_ranges['min_trans'] = score_ranges['min'].astype(float)
    score_ranges['max_trans'] = score_ranges['max'].astype(float)
    trans_ranges_dict = {level: (row['min_trans'], row['max_trans'])
                         for level, row in score_ranges.iterrows()}
    return trans_ranges_dict

def score_oldnew(trans_amount):
    if '旧模型预警等级' in trans_amount.columns and '旧模型新分数' in trans_amount.columns:
        # 计算每个预警等级的分数区间
        score_ranges = trans_amount.groupby(
            '旧模型预警等级')['旧模型新分数'].agg(['min', 'max']).sort_index()
        print(score_ranges)
    else:
        print("预警等级或分数列不存在于数据中。")
    score_ranges = score_ranges.reset_index()
    score_ranges['旧模型预警等级'] = score_ranges['旧模型预警等级'].astype(int)
    score_ranges = score_ranges.set_index('旧模型预警等级').sort_index()
    score_ranges = score_ranges.sort_index()
    score_ranges['min_sys'] = score_ranges['min'].astype(float)
    score_ranges['max_sys'] = score_ranges['max'].astype(float)
    score_ranges_dict = {level: (row['min_sys'], row['max_sys'])
                         for level, row in score_ranges.iterrows()}
    return score_ranges_dict


# 对每个主体的分数进行缩放 就新旧模型之间的映射
def mapping_relations(entity_t, entity_res_t, score_ranges_dict, labeled_intervals):
    def scale_score(old_score, old_range, new_range):
        old_min, old_max = old_range
        new_min, new_max = new_range
        # 计算新旧区间的比例
        scale = (new_max - new_min) / (old_max - old_min)
        # 缩放分数
        return new_min + (old_score - old_min) * scale

    entity_res_t['预警等级'] = entity_res_t['预警等级'].astype(int)
    entity_res_t['预警得分'] = entity_res_t['预警得分'].astype(float)
    result = pd.DataFrame()
    # entities = entity_res_t['主体名称'].unique()
    for index, row in entity_res_t.iterrows():
        level = row['预警等级']
        old_score = row['预警得分']
        new_score = scale_score(
            old_score, score_ranges_dict[level], labeled_intervals[level])
        row['旧模型新分数'] = new_score
        row = row.to_frame().transpose()
        result = pd.concat([result, row], axis=0)
    result.rename(columns={'预警得分': '旧模型分数', '预警等级': '旧模型预警等级'}, inplace=True)
    df_full = pd.merge(entity_t, result, left_on='主体名称',
                       right_on='主体名称', how='left')
    df_full.rename(columns={'预警得分': '新模型分数'}, inplace=True)
    df_full['旧模型预警等级'] = df_full['旧模型预警等级'].astype('Int64')
    df_full['预警等级'] = df_full['预警等级'].astype('Int64')
    df_full['新旧模型等级变动量'] = abs(df_full['旧模型预警等级'] - df_full['预警等级'])
    return df_full

# df_full.drop(columns='旧模型预警等级',inplace=True)

 
def trans(df_full, entity_t, transition_day):

    # 定义计算基量和衰减天数的函数
    def calculate_base_amount(row):
        level_change = abs(row['旧模型预警等级'] - row['预警等级'])
        if level_change < 3:
            decay_days = 30
        elif 3 <= level_change <= 5:
            decay_days = 90
        else:
            decay_days = 180
        base_amount = row['旧模型新分数'] - row['新模型分数']
        return pd.Series([base_amount, decay_days], index=['base_amount', 'decay_days'])

    # 计算调整后的新模型分数
    def calculate_adjustment(row, transition_day):
        if transition_day <= row['decay_days']:
            daily_add = row['base_amount'] / row['decay_days']
            updated_score = row['旧模型新分数'] - daily_add * transition_day # 对鸭鸭的代码改动了，应该问题不大
        else:
            updated_score = row['新模型分数']
        return updated_score

    if transition_day == 0:
        new_columns = df_full.apply(calculate_base_amount, axis=1)
        df_full = pd.concat([df_full, new_columns], axis=1)
        df_full.to_pickle(
            'C:/Users/cyjiang/Documents/债券预警模型/增量/output/base_amount.pkl')
        df_cal = pd.merge(entity_t, df_full, left_on='主体名称',
                          right_on='主体名称', how='left')
        df_cal['updated_score'] = df_cal.apply(
            lambda row: calculate_adjustment(row, transition_day), axis=1)
        df_cal.to_pickle(
            'C:/Users/cyjiang/Documents/债券预警模型/增量/output/trans_amount.pkl')

    else:
        base_amount = pd.read_pickle(
            'C:/Users/cyjiang/Documents/债券预警模型/增量/output/base_amount.pkl')
        df_cal = pd.merge(entity_t, base_amount, left_on='主体名称',
                          right_on='主体名称', how='left')
        df_cal['updated_score'] = df_cal.apply(
            lambda row: calculate_adjustment(row, transition_day), axis=1)
        df_cal.to_pickle(
            'C:/Users/cyjiang/Documents/债券预警模型/增量/output/trans_amount.pkl')
    return
