# -*- coding: utf-8 -*-
"""
Created on Fri Jan 26 13:35:40 2024

主体等级平滑

@author: erlu
"""
import pandas as pd
import numpy as np

def smooth(hist_lr_level,lr_level,downgrade_window):
    
    lr_level1=lr_level.copy(deep=True)
    lr_level1.columns=['主体名称','日期','预警等级','预警得分']
    # hist_lr_level 为读取的历史等级['主体名称','日期','预警等级','预警得分']，字段应与最新一日的保持一致
    hist_lr_level.columns=['主体名称','日期','预警等级','预警得分']
    lr_level_concated=pd.concat([hist_lr_level,lr_level1],axis=0).reset_index(drop=True)
    
    level_interval=pd.DataFrame({'interval':['0~3','0~3','0~3','0~3','4~5','4~5','6~7','6~7','8~9','8~9','10']})
    
    def get_smoothed_res_v1(res_i):
        res_i['smoothed_level']=np.nan
        res_i['smoothed_level'][0]=res_i['预警等级'][0]
        
        for i in range(1,len(res_i)):
            # 等级不变
            if int(res_i['预警等级'][i])==int(res_i['smoothed_level'][i-1]):
                res_i['smoothed_level'][i]=res_i['smoothed_level'][i-1]
                
            # 等级上调
            elif int(res_i['预警等级'][i])>int(res_i['smoothed_level'][i-1]):
        
                if int(res_i['预警等级'][i])==10:
                    res_i['smoothed_level'][i]=res_i['预警等级'][i]
                elif int(res_i['预警等级'][i])<=3:
                    res_i['smoothed_level'][i]=res_i['预警等级'][i]
                else: 
                    window=res_i.iloc[i-10:i]
                    if len(set(window['smoothed_level']))==1:
                        res_i['smoothed_level'][i]=str(int(res_i['smoothed_level'][i-1])+1)
                    else:
                        res_i['smoothed_level'][i]=res_i['smoothed_level'][i-1]
            # 等级下降
            else:
                # 同类别
                if  (level_interval.iloc[int(res_i['预警等级'].iloc[i])]==level_interval.iloc[int(res_i['smoothed_level'].iloc[i-1])]).all():
                    w=np.array(downgrade_window.loc[downgrade_window['level']==res_i['smoothed_level'].iloc[i-1]]['downgrade_window'])[0]
                    window=res_i['smoothed_level'].iloc[i-w:i]
                    if len(set(window))==1:
                        res_i['smoothed_level'][i]=str(int(res_i['smoothed_level'][i-1])-1)
                    else:
                        res_i['smoothed_level'][i]=res_i['smoothed_level'][i-1]
                # 跨类别
                else:
                    if res_i['smoothed_level'][i-1]=='10': # 10级向下本来就会跨等级
                        w=np.array(downgrade_window.loc[downgrade_window['level']==res_i['smoothed_level'].iloc[i-1]]['downgrade_window'])[0]
                    
                    elif res_i['smoothed_level'][i-1] in ['8','9']: # 8~9级跨类别向下调整多观察14天
                        w=14+np.array(downgrade_window.loc[downgrade_window['level']==res_i['smoothed_level'].iloc[i-1]]['downgrade_window'])[0]
                        
                    elif res_i['smoothed_level'][i-1] in ['4','5','6','7']:# 4~7级跨类别向下调整多观察7天
                        w=7+np.array(downgrade_window.loc[downgrade_window['level']==res_i['smoothed_level'].iloc[i-1]]['downgrade_window'])[0]
                    
                    else:
                        print('error: 没有跨类别')
                        
                    window=res_i.iloc[i-w:i]
                    if len(set(window['smoothed_level']))==1:
                        res_i['smoothed_level'][i]=str(int(res_i['smoothed_level'][i-1])-1)
                    else:
                        res_i['smoothed_level'][i]=res_i['smoothed_level'][i-1]
               
        return res_i
        
    entity_list=lr_level_concated['主体名称'].unique()
    
    res_smooth=pd.DataFrame()
    for idx,e in enumerate(entity_list):
        print('current entity:\n',e)
        print('current: ',idx+1)
        print('total: ',len(entity_list))
        res_i = lr_level_concated[lr_level_concated['主体名称'] == e].reset_index(drop=True)
        res_i=res_i.sort_values(by='日期')
        res_smooth_i=get_smoothed_res_v1(res_i)
        res_smooth=pd.concat([res_smooth,res_smooth_i],axis=0)
    
    return res_smooth[['主体名称','日期','预警等级','smoothed_level','预警得分']]

