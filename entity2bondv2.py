import datetime
import numpy as np
import pandas as pd

def data_preprocess(b_e_dict,bond_res_t,score_price_initial,target_date,w):

    # bond_res_t中有重复条的数据
    bond_list=bond_res_t.drop_duplicates(subset=['债券编码','债券名称'], keep='first', inplace=False, ignore_index=False)
    
    res_bond=pd.merge(bond_list,b_e_dict,on=['债券编码','债券名称'],how='left')
    # wb.type '1':存续期 '2':到期 ，已到期债券在系统中无预警等级
    res_bond_expired=res_bond.loc[res_bond['是否到期']=='2']
    res_bond_unexpired=res_bond.loc[res_bond['是否到期']=='1']
    
    # 拼接窗口期内需要的各债价格波动
    dt_target_date=datetime.datetime.strptime(target_date,'%Y-%m-%d')
    ago = dt_target_date - datetime.timedelta(days=w-1)    
    
    score_price_initial_t=score_price_initial.loc[(score_price_initial['成交日期']>=ago)&(score_price_initial['成交日期']<=dt_target_date)].reset_index(drop=True)
    
    res_bond_unexpired=pd.merge(res_bond_unexpired,score_price_initial_t,left_on='债券名称',right_on='债券简称',how='left')    
    
    return res_bond_unexpired[['债券编码','债券名称','主体名称','是否到期','成交日期','最终打分']],res_bond_expired

def linear_decay(s,now,window_size):
    # 往前推w天
    s1=s.copy(deep=True)
    ago = now - datetime.timedelta(days=window_size-1)
    s1['t'] = now - s1['成交日期']
    s1['t_days'] = s1['t'].apply(lambda x: x.days)
    window = s1[(s1['成交日期'] >= ago) & (s1['成交日期'] <= now)].reset_index(drop=True)
    
    # # linear decay
    # def linear_decay()
    window['decay_factor'] = 1 - window['t_days'] / (window_size - 1)
    window['decayed_value'] = window['最终打分'] * window['decay_factor']

    return np.sum(window['decayed_value'])

def get_Decayed_Bond_Price_Score(b_unexpired,target_date,w):
    dt_target_date=datetime.datetime.strptime(target_date,'%Y-%m-%d')
    
    # looping - 计算债券价格波动衰减后得分
    bond_list=b_unexpired['债券名称'].unique()
    res_decayed_b_score=pd.DataFrame()

    for idx,b in enumerate(bond_list):
        print('current bond:\n',b)
        print('current: ',idx+1)
        print('total: ',len(bond_list))
        res_i = b_unexpired[b_unexpired['债券名称'] == b].reset_index(drop=True)
        res_i_t=res_i.loc[res_i['成交日期']==dt_target_date][res_i.columns[:-1]].copy(deep=True)
        if res_i_t.empty==True: 
            #该债券在过去一年无价格异动，拼接来的成交日期为NaT，res_i_t取出来的当日window为空，需要赋当日信息，否则只return decay_sum
            res_i_t=res_i[res_i.columns[:-2]].drop_duplicates(keep='first',inplace=False, ignore_index=False)
            res_i_t['成交日期']=dt_target_date
        else:
            pass
        # res_i_t['decay_sum']= res_i_t.apply(lambda row: linear_decay(res_i,dt_target_date,w), axis=1)
        res_i_t['decay_sum']= linear_decay(res_i,dt_target_date,w)
        
        res_decayed_b_score=pd.concat([res_decayed_b_score,res_i_t],axis=0).reset_index(drop=True)
    
    return res_decayed_b_score

def compress_Decayed_Bond_Price_Score(result_price):
    
    # 对衰减后得分进行压缩    
    # looping - 主体下各债异动衰减后得分最大/最小值
    entity_list=result_price['主体名称'].unique()
    res_compressed=pd.DataFrame()

    for idx,e in enumerate(entity_list):
        print('current entity:\n',e)
        print('current: ',idx+1)
        print('total: ',len(entity_list))
        res_i = result_price[result_price['主体名称'] == e].reset_index(drop=True)
        
        min_price_factor = res_i['decay_sum'].min()
        max_price_factor = res_i['decay_sum'].max()
        
        # rescale to 0-100
        res_i['compressed_value'] = ((res_i['decay_sum'] - min_price_factor) / (max_price_factor - min_price_factor)) * 100
        
        res_compressed=pd.concat([res_compressed,res_i],axis=0)

    return res_compressed


def get_Bond_Score(b_unexpired_d_c,entity_t,score_price_d_c,target_date,alpha1,alpha2):
    dt_target_date=datetime.datetime.strptime(target_date,'%Y-%m-%d')
    score_price_d_c_t=score_price_d_c.loc[score_price_d_c['日期']==dt_target_date].reset_index(drop=True)

    # 拼接主体当日衰减、压缩后价格异动得分
    res_bond_score=pd.merge(b_unexpired_d_c,score_price_d_c_t,left_on=['主体名称','成交日期'],right_on=['主体名称','日期'],how='left')
    res_bond_score=pd.merge(res_bond_score,entity_t,left_on=['主体名称','成交日期'],right_on=['主体名称','日期'],how='left')
    
    res_bond_score=res_bond_score[['债券编码','债券名称','主体名称','是否到期','成交日期','decay_sum','compressed_value_x','compressed_value_y','预警等级','预警得分']]
    res_bond_score.columns=['债券编码','债券名称','主体名称','是否到期','成交日期','decay_sum','compressed_value-各债','compressed_value-主体','主体预警等级','主体预警得分']
    
    
    def func_bond_score(bond_d_c,entity_d_c,entity_score,alpha1,alpha2,entity_level):
        if pd.isna(entity_level)==True:  #该主体不在预警范围，且目前系统中无债券预警等级及分数，债券信息中‘是否到期’字段数据有误
            return np.nan
        else:
            
            if entity_d_c==0: # 主体价格得分=0，其债券一定=0
                return entity_score #债券和主体相同预警分数
            else: 
                if pd.isna(bond_d_c)==True: #主体下各债均无波动
                    bond_d_c=0 

                if int(entity_level)<=7:
                    return max(entity_score-alpha1*(100-bond_d_c),0)
                else:
                    return max(entity_score-alpha2*(100-bond_d_c),0) 
            
    res_bond_score['债券预警得分']=res_bond_score.apply(lambda x: func_bond_score(x['compressed_value-各债'],x['compressed_value-主体'],x['主体预警得分'],alpha1,alpha2,x['主体预警等级']),axis=1)

    return res_bond_score
