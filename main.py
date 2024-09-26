# -*- coding: utf-8 -*-
"""
Created on Mon Jan 31 10:18:37 2024

增量

@author: Jiang
"""
import datetime
import numpy as np
import pandas as pd
import 基本面因子v2 as fdmt
import 发行利率因子v2 as issr
import 价格因子v2 as price
import 舆情因子v2 as news
import ib_entity_bond as ib
import cal_lr_result
import entity_relationship_v4 as rela
import smooth_level
import entity2bondv2
import 平滑过渡 as transfer

if __name__ == "__main__":

    starttime0 = datetime.datetime.now()

    # -------------------------------------------------------------------------

    # output path
    folder_path = 'C:/Users/cyjiang/Documents/债券预警模型/增量/'
    output_path = folder_path
    # input path dict
    input_path = folder_path+'input/'

    # output path dict
    output_dict = {}
    output_dict['entity'] = output_path+'output/bond_entity_info/'
    output_dict['X_Issr'] = output_path+'output/发行利率/'
    output_dict['X_News'] = output_path+'output/舆情/'
    output_dict['X_Fdmt'] = output_path+'output/基本面/'
    output_dict['X_Price'] = output_path+'output/量价/'
    output_dict['model'] = output_path+'output/model/'
    output_dict['transform'] = output_path+'output/transform/'


    db_info = pd.DataFrame({'ibond': ['10.173.69.52', 'devops', 'devops@2024', 3306, 'ibond'],
                            'wind': ['10.173.69.50', 'allsearch', 'allsearch~!@', 3306, 'wind'],
                            'ths': ['10.173.69.50', 'allsearch', 'allsearch~!@', 3306, 'jqka'],
                            'zax': ['10.173.69.50', 'allsearch', 'allsearch~!@', 3306, 'dp_require_data'],
                            'news': ['10.173.69.52', 'allsearch', 'allsearch~!@', 3306, 'ibond'],
                            'price': ['10.173.69.52', 'allsearch', 'allsearch~!@', 3306, 'ibond']
                            },
                           index=['host', 'user', 'passwd', 'port', 'db'])

    # target_date='2023-11-10'
    # target_date=(datetime.datetime.now()- datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    target_date = datetime.datetime(2024, 3, 3).strftime("%Y-%m-%d")
    
    w_price = 365
    w_news = 90
    w_bond = 14  # 主体-债券传导的回看期
    # labels_level = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10']
    labels_level = [0,1,2,3,4,5,6,7,8,9,10]

    # ------------------------------------------------------------------------
    #                        Step 0: ib主体&债券信息
    # ------------------------------------------------------------------------
    # 1.当日系统主体预警等级&得分
    entity_res_t = ib.get_Entity_sys_t(target_date, db_info)
    # entity_res_t = entity_res_t[entity_res_t['主体名称']=='青岛海发国有资本投资运营集团有限公司']
    entity_res_t.to_pickle(output_dict['model']+'entity_res_t.pkl')

# =============================================================================
#     # 2.当日系统债券预警等级&得分
#     bond_res_t=ib.get_Bond_sys_t(target_date, db_info)
#     bond_res_t.to_pickle(output_dict['model']+'bond_res_t.pkl')
#
#     # 3. 债券主体映射关系
#     b_e_dict=ib.get_Bond_Entity_Dict(db_info)
#     b_e_dict.to_pickle(output_dict['entity']+'b_e_dict.pkl')
# =============================================================================

    # ========================================================================
    #                            Step 1: 因子处理
    # ========================================================================
    print('---------------------------------------------------------')
    print('Step 1: 因子处理')
    # ------------------------------------------------------------------------
    #                                 基本面
    # ------------------------------------------------------------------------
    starttime = datetime.datetime.now()

    # 1.数据读取&预处理 ->指标表
    fundamental_info = fdmt.get_Fundamental(target_date, db_info)
    fundamental_info.to_pickle(output_dict['X_Fdmt']+'fundamental_info.pkl')

    # 2.基本面（分箱结果）赋分
    fundamental_info = pd.read_pickle(
        output_dict['X_Fdmt']+'fundamental_info.pkl')
    bins_rule = pd.read_excel(input_path+'bins_rule.xlsx')

    # 同一主体不同敞口评级结果不同，取max
    fundamental_info = fundamental_info.sort_values(
        by='评级结果', ascending=False).reset_index(drop=True)
    fundamental_info_unique = fundamental_info.groupby(
        ['主体名称'])['评级结果'].max().reset_index(drop=False)

    # 拼接预警主体范围
    entity_res_t = pd.read_pickle(output_dict['model']+'entity_res_t.pkl')
    ib_entity_list = entity_res_t['主体名称'].unique()
    fundamental_info_merged = pd.merge(pd.DataFrame(ib_entity_list, columns=[
                                       '主体名称']), fundamental_info_unique, on='主体名称', how='left')

    score_basic = fdmt.get_bins_mapping_result(
        fundamental_info_merged, bins_rule, 'score_basic')
    score_basic.to_pickle(output_dict['X_Fdmt']+'score_basic.pkl')

    # 计算运行时间
    endtime = datetime.datetime.now()
    # 2024-01-29运行时间：0:00:03.559611
    print('基本面因子处理完成，运行时间: '+str(endtime-starttime))

    # ------------------------------------------------------------------------
    #                                 发行利率
    # ------------------------------------------------------------------------
    starttime = datetime.datetime.now()
# =============================================================================
#     # 1.数据读取&预处理 ->指标表
#     # ib-债券主体映射关系
#     bond_mapping=issr.get_Mapping_Dict(db_info)
#     bond_mapping.to_pickle(output_dict['X_Issr']+'bond_mapping.pkl')
#
#     # wind 发行利率
#     issuing_rate_info=issr.get_Issuing_Rate(target_date, db_info)
#     issuing_rate_info.to_pickle(output_dict['X_Issr']+'issuing_rate_info.pkl')
#
#     # 2.发行利率指标计算
#     issuing_rate_info=pd.read_pickle(output_dict['X_Issr']+'issuing_rate_info.pkl')
#     issr_preprocessed, issr_data_1y,issr_data=issr.data_preprocess(target_date,issuing_rate_info)
#     issr_preprocessed.to_pickle(output_dict['X_Issr']+'issr_preprocessed.pkl')
#     issr_data_1y.to_pickle(output_dict['X_Issr']+'issr_data_1y.pkl')
#     issr_data.to_pickle(output_dict['X_Issr']+'issr_data.pkl')
#
#     # 3.发行利率指标分数计算
#     issr_data=pd.read_pickle(output_dict['X_Issr']+'issr_data.pkl')
#     # 拼接预警主体范围
#     entity_res_t=pd.read_pickle(output_dict['model']+'entity_res_t.pkl')
#     ib_entity_list=entity_res_t['主体名称'].unique()
#     issr_data_merged=pd.merge(pd.DataFrame(ib_entity_list,columns=['主体名称']),issr_data,left_on='主体名称',right_on='b_info_issuer',how='left')
#
#     score_issr=issr.get_bins_mapping_result(issr_data_merged, bins_rule, 'score_issr')
#     score_issr.to_pickle(output_dict['X_Issr']+'score_issr.pkl')
# =============================================================================

    bond_mapping=issr.get_Mapping_Dict(db_info)
    bond_mapping.to_pickle(output_dict['X_Issr']+'bond_mapping.pkl')
    # 每周跑p更新 issue_data.pkl
    issr_data = pd.read_pickle(input_path+'issue_data.pkl')
    issr_data_1y,issr_data = issr.concat_local(target_date,bond_mapping,issr_data)
    issr_data_1y.to_pickle(output_dict['X_Issr']+'issr_data_1y.pkl')
    issr_data.to_pickle(output_dict['X_Issr']+'issr_data.pkl')

    # 拼接预警主体范围
    entity_res_t = pd.read_pickle(output_dict['model']+'entity_res_t.pkl')
    ib_entity_list = entity_res_t['主体名称'].unique()
    issr_data_merged = pd.merge(pd.DataFrame(ib_entity_list, columns=[
                                '主体名称']), issr_data, left_on='主体名称', right_on='b_info_issuer', how='left')

    score_issr = issr.get_bins_mapping_result_local(
        issr_data_merged, bins_rule, 'score_issr')
    score_issr.to_pickle(output_dict['X_Issr']+'score_issr.pkl')

    # 计算运行时间
    endtime = datetime.datetime.now()
    # 2024-01-29运行时间：0:45:23.261485
    print('发行利率因子处理完成，运行时间: '+str(endtime-starttime))

    # ------------------------------------------------------------------------
    #                                 价格异常波动
    # ------------------------------------------------------------------------
    starttime = datetime.datetime.now()
    # 步骤1，2的数据未来会导入库里，直接在步骤3提取即可，无需预处理，只需要修改库里数据读取以及处理成衰减前的格式

    # 1.数据读取&预处理 ->指标表
    # ---从数据库读取
    # price_info=price.get_Price_info(target_date, db_info)
    # price_info.to_pickle(output_dict['X_Price']+'price_info.pkl')

    # price_info=pd.read_pickle(output_dict['X_Price']+'price_info.pkl')
    # price_processed=price.data_preprocess(price_info)
    # price_processed.to_pickle(output_dict['X_Price']+'price_processed.pkl')

    # 2.价格得分计算
    # ---从本地读取
    # df_price=pd.read_csv(input_path+'价格偏离昨收_total.csv',encoding='gbk',index_col=False)
    # df_return=pd.read_csv(input_path+'收益率偏离中债估值_total.csv',encoding='gbk',index_col=False)

    # df_final,score_price_initial=price.get_Price_Score(df_return, df_price, w_price)
    # df_final.to_pickle(output_dict['X_Price']+'成交估值打分最终结果文件.pkl')
    # score_price_initial.to_pickle(output_dict['X_Price']+'score_price_initial.pkl')

    # 3. 价格得分衰减
    score_price_initial = pd.read_pickle(input_path+'成交估值打分最终结果文件(new).pkl')

    # 仅取预警范围内的主体进行衰减
    entity_res_t = pd.read_pickle(output_dict['model']+'entity_res_t.pkl')
    ib_entity_list = entity_res_t['主体名称'].unique()
    score_price_to_decay = score_price_initial.loc[score_price_initial['发债主体'].isin(
        ib_entity_list)]

    score_price_d = price.get_Decayed_Price(score_price_to_decay, w_price)
    score_price_d.to_pickle(output_dict['X_Price']+'score_price_d.pkl')

    # 4. 价格得分衰减后rescale
    score_price_d = pd.read_pickle(output_dict['X_Price']+'score_price_d.pkl')
    score_price_d_c = price.compress_Decayed_Price(score_price_d)
    score_price_d_c.to_pickle(output_dict['X_Price']+'score_price_d_c.pkl')

    # 5. 获取当日价格衰减，压缩后得分
    score_price = score_price_d_c.loc[score_price_d_c['日期'] == datetime.datetime.strptime(
        target_date, '%Y-%m-%d')].reset_index(drop=True)

    # 拼接预警主体范围
    score_price = pd.merge(pd.DataFrame(ib_entity_list, columns=[
                           '主体名称']), score_price, on='主体名称', how='left')
    score_price['compressed_value'].fillna(value=0, inplace=True)

    score_price.to_pickle(output_dict['X_Price']+'score_price.pkl')

    # 计算运行时间
    endtime = datetime.datetime.now()
    print('价格异动因子处理完成，运行时间: '+str(endtime-starttime))

    # ------------------------------------------------------------------------
    #                                 舆情
    # ------------------------------------------------------------------------
    starttime = datetime.datetime.now()

    # 1.数据读取&预处理 ->指标表
    # 舆情打分
    news_info = news.get_News_Info(target_date, db_info)
    news_info.to_pickle(output_dict['X_News']+'news_info.pkl')

    # 舆情标签体系（新闻+公告）
    tags_info = news.get_Tags_Info(db_info)
    tags_info.to_pickle(output_dict['X_News']+'tags_info.pkl')

    # 新增-监管处罚数据&展期数据
    panelties_record = pd.read_excel(input_path+'监管处罚整理版.xlsx')
    default_record = news.get_Warn_Breachinfo(target_date, db_info)
    default_record.to_pickle(output_dict['X_News']+'default_record.pkl')

    tags_added = news.data_preprocess(
        default_record, panelties_record, target_date)
    tags_added.to_pickle(output_dict['X_News']+'tags_added.pkl')

    # 2. 计算舆情得分 (新增舆情打分&原过大舆情得分重新赋分)
    news_info = pd.read_pickle(output_dict['X_News']+'news_info.pkl')
    tags_added = pd.read_pickle(output_dict['X_News']+'tags_added.pkl')
    score_news_initial = news.get_News_Score(news_info, tags_added)
    score_news_initial.to_pickle(
        output_dict['X_News']+'score_news_initial.pkl')

    # 3. 舆情得分衰减
    score_news_initial = pd.read_pickle(
        output_dict['X_News']+'score_news_initial.pkl')

    # 仅取预警范围内的主体进行衰减
    entity_res_t = pd.read_pickle(output_dict['model']+'entity_res_t.pkl')
    ib_entity_list = entity_res_t['主体名称'].unique()
    score_news_to_decay = score_news_initial.loc[score_news_initial['主体名称'].isin(
        ib_entity_list)]

    # 重要舆情标签
    importances = pd.read_excel(input_path+'importances.xlsx')
    score_news_d = news.get_Decayed_News(
        score_news_to_decay, importances, w_news)
    score_news_d.to_pickle(output_dict['X_News']+'score_news_d.pkl')

    # 4. 舆情得分衰减后压缩
    score_news_d = pd.read_pickle(output_dict['X_News']+'score_news_d.pkl')
    score_news_d_c = news.compress_News_Decayed(score_news_d)
    score_news_d_c.to_pickle(output_dict['X_News']+'score_news_d_c.pkl')

    # 5. 获取当日舆情衰减，压缩后得分
    score_news = score_news_d_c.loc[score_news_d_c['日期'] == datetime.datetime.strptime(
        target_date, '%Y-%m-%d')].reset_index(drop=True)

    # 拼接预警主体范围
    score_news = pd.merge(pd.DataFrame(ib_entity_list, columns=[
                          '主体名称']), score_news, on='主体名称', how='left')
    score_news['compressed_value'].fillna(value=0, inplace=True)
    score_news.to_pickle(output_dict['X_News']+'score_news.pkl')

    # 计算运行时间
    endtime = datetime.datetime.now()
    print('舆情因子处理完成，运行时间: '+str(endtime-starttime))

    # ========================================================================
    #                            Step 2: 预警分数、等级计算
    # ========================================================================
    print('---------------------------------------------------------')
    print('Step 2: 预警分数、等级计算')
    # ------------------------------------------------------------------------
    #                                 主体pd&等级计算
    # ------------------------------------------------------------------------
    starttime = datetime.datetime.now()

    # 1.数据读取&拼接input dataframe
    score_news = pd.read_pickle(output_dict['X_News']+'score_news.pkl')
    score_issr = pd.read_pickle(output_dict['X_Issr']+'score_issr.pkl')
    score_basic = pd.read_pickle(output_dict['X_Fdmt']+'score_basic.pkl')
    score_price = pd.read_pickle(output_dict['X_Price']+'score_price.pkl')

    score_issr.rename(columns={'b_info_issuer':'主体名称'},inplace=True)


    model_input = cal_lr_result.get_model_input(
        target_date, ib_entity_list, score_issr, score_basic, score_news, score_price)
    model_input.to_pickle(output_dict['model']+'model_input.pkl')

    # 2.代入系数计算主体pd
    coef = pd.read_excel(input_path+'coef.xlsx')
    model_input = pd.read_pickle(output_dict['model']+'model_input.pkl')

    # lr_pd_score = cal_lr_result.get_lr_pd(model_input, coef)
    # lr_pd_score.to_pickle(output_dict['model']+'lr_pd_score.pkl')

    # 3.计算initial预警等级
    lr_pd_score = pd.read_pickle(output_dict['model']+'lr_pd_score0303.pkl')
    hist_level_pd = pd.read_csv(
        input_path+'hist_level_pd江.csv', encoding='gbk', index_col=False)
    
    # lr_pd_level, hist_level_pd 去null值
    lr_pd_score.fillna(0, inplace=True)
    # hist_level_pd = hist_level_pd.applymap(lambda x: pd.to_numeric(x,errors='ignore'))
    # labels_level = labels_level.applymap(lambda x: pd.to_numeric(x,errors='ignore'))

    lr_pd_level, daily_level_pd = cal_lr_result.get_lr_level(
        lr_pd_score, hist_level_pd, labels_level, option=1)
    lr_pd_level.to_pickle(output_dict['model']+'lr_pd_level.pkl')
    lr_pd_level.to_excel(output_dict['model']+'lr_pd_level.xlsx',index = False)
    daily_level_pd.to_pickle(output_dict['model']+'daily_level_pd.pkl')
    # daily_level_pd.to_excel(output_dict['model']+'daily_level_pd.xlsx')

    # 计算运行时间
    endtime = datetime.datetime.now()
    print('主体预警得分，等级计算完成，运行时间: '+str(endtime-starttime))

    # ------------------------------------------------------------------------
    #                                 主体关联关系传导
    # ------------------------------------------------------------------------
    # 根据initial预警等级进行关联关系传导
    starttime = datetime.datetime.now()
    # 1. 数据读取&预处理
    lr_pd_level = pd.read_pickle(output_dict['model']+'lr_pd_level.pkl')
    relation_info = rela.get_Relation_Info(db_info)
    # 计算担保，实控人，供应商，客户的ratio option={1：1/该类型的关联主体总数; 2: 1/该类型的有预警等级的关联主体总数}
    relation_data = rela.data_proprocess(lr_pd_level, relation_info, option=2)
    relation_data.to_pickle(output_dict['model']+'relation_data.pkl')

    # 2. 进行传导
    relation_data = pd.read_pickle(output_dict['model']+'relation_data.pkl')
    rela_pd_level, rela_res_detail = rela.relationship_level(
        lr_pd_level, hist_level_pd, labels_level, relation_data, option=2)
    
    
    rela_pd_level.to_pickle(output_dict['model']+'rela_pd_level.pkl')
    rela_res_detail.to_pickle(output_dict['model']+'rela_res_detail.pkl')

    # 计算运行时间
    endtime = datetime.datetime.now()
    print('主体关联关系传导完成，运行时间: '+str(endtime-starttime))

    # ------------------------------------------------------------------------
    #                                 主体预警等级平滑
    # ------------------------------------------------------------------------
    starttime = datetime.datetime.now()
    downgrade_window = pd.read_excel(input_path+'downgrade_window.xlsx')
    rela_pd_level = pd.read_pickle(output_dict['model']+'rela_pd_level.pkl')

    # 主体预警分数转换
    rela_level_score = cal_lr_result.get_lr_score(rela_pd_level)

    # # 读取近180天预警分数与等级 -需补充代码?
    # start_date = '2023-08-29'
    # end_date = '2024-02-25'    
    # hist_level_score= ib.get_180Entity_sys_t(start_date,end_date,db_info)
    # hist_level_score.to_pickle(output_dict['model'] + 'hist_level_score.pkl')
    
    # # 处理数据格式
    # hist_level_score['日期'] = pd.to_datetime(hist_level_score['日期'])
    # hist_level_score['日期'] = hist_level_score['日期'].astype('int64')
    # rela_level_score['日期'] = pd.to_datetime(rela_level_score['日期'])
    # rela_level_score['日期'] = rela_level_score['日期'].astype('int64')
    
    # #开始平滑
    # smoothed_level=smooth_level.smooth(hist_level_score, rela_level_score, downgrade_window)
    # smoothed_level.to_pickle(output_dict['model']+'smoothed_level.pkl')

    # 计算运行时间
    endtime = datetime.datetime.now()
    print('主体预警等级平滑完成，运行时间: '+str(endtime-starttime))

    # ------------------------------------------------------------------------
    #                                 输出当日主体预警分数与得分到指定pkl
    # ------------------------------------------------------------------------
    # ---------------并行测试开始后后有历史记录--使用这段代码
    # smoothed_level=pd.read_pickle(output_dict['model']+'smoothed_level.pkl')
    # entity_t=smoothed_level.loc[smoothed_level['日期']==target_date][['主体名称','日期','smoothed_level','预警得分']].reset_index(drop=True) #nan 代表一年内无新发债
    # entity_t.columns=['主体名称','日期','预警等级','预警得分']

    # 调试测试代码先使用以下代码替代
    entity_t = rela_level_score.copy(deep=True)

    # # 并行测试首日（无完整output）运行以下代码：
    # entity_t.to_pickle(output_dict['model']+'warn_entityscore.pkl')
    # entity_t.to_csv(
    #     output_dict['model']+'warn_entityscore.csv', encoding='gbk', index=False)
    # entity_t = pd.read_csv(output_dict['model']+'warn_entityscore.csv', encoding='gbk', index=False)

    # ----------- 并行测试第二日起运行以下代码：
    # 现在已有0225,0226,0227,0228,0229,0301,0302,0303的新数据了
    warn_entityscore=pd.read_pickle(output_dict['model']+'warn_entityscore.pkl')
    warn_entityscore=pd.concat([warn_entityscore,entity_t],axis=0)
    warn_entityscore.to_pickle(output_dict['model']+'warn_entityscore.pkl')

    # ------------------------------------------------------------------------
    #                                 主体平滑过度
    # ------------------------------------------------------------------------
    # 旧模型的分数区间
    # start_date = datetime(2024, 2, 25)
    # transition_period = 180
    # today = datetime(2024, 2, 25)
    transition_day = 7

    if transition_day == 0:
        entity_res_t.to_pickle(
            'C:/Users/cyjiang/Documents/债券预警模型/增量/output/transform/entity_res_t.pkl')
    # 目前是从2.25号开始过渡的，这里的entity_res_t是2.25的系统分数
    else:
        entity_res_t = pd.read_pickle(
            'C:/Users/cyjiang/Documents/债券预警模型/增量/output/transform/entity_res_t0225.pkl')
    
    # entity_t = warn_entityscore[warn_entityscore['日期']=='2024-02-26']
    # import 平滑过渡 as transfer

    score_ranges_dict = transfer.score_old(entity_res_t)
    labeled_intervals = transfer.score_new(entity_t)
    
    df_full = transfer.mapping_relations(
        entity_t, entity_res_t, score_ranges_dict, labeled_intervals)
    # entity_t.fillna(0,inplace=True)
    df_full.fillna(0,inplace=True)
    # import cal_lr_result
    transfer.trans(df_full, entity_t,transition_day)
   
    trans_amount = pd.read_pickle('C:/Users/cyjiang/Documents/债券预警模型/增量/output/trans_amount.pkl')
    
    # ------------------------------------------------------------------------

    trans_amount = trans_amount.rename(columns={'updated_score':'预警得分_trans'})
    
    def assign_warning_level(score):
        for level, (min_val, max_val) in labeled_intervals.items():
            if min_val <= score <= max_val:
                return level
        return None  # 如果分数不在任何区间内，返回None

    trans_amount["预警等级_trans"] = trans_amount["预警得分_trans"].apply(assign_warning_level)

   
    trans_amount.to_pickle(output_dict['transform']+'trans_amount.pkl')
    # base_amount = pd.read_pickle(
    #     'C:/Users/cyjiang/Documents/债券预警模型/增量/output/base_amount.pkl')

    print('主体平滑过渡完成')

    # ------------------------------------------------------------------------
    #                     划档区间 excel
    # ------------------------------------------------------------------------
    
    # 系统分数区间
    sys_intervals = pd.DataFrame(data = score_ranges_dict)
    sys_intervals_df=(sys_intervals.T)
    sysmaxnum = sys_intervals_df[1].iloc[-1]
    sys_intervals_df.loc[len(sys_intervals_df)]=[None, None]
    sys_intervals_df.iloc[-1] = sysmaxnum
    sys_intervals_df = sys_intervals_df[[0]]
    sys_intervals_df = sys_intervals_df.rename(columns={0:'系统分数区间'})

    # 训练模型的滑档区间 
    hist_interval = (hist_level_pd.T)
    hist_interval = hist_interval.reset_index()
    hist_interval = hist_interval[[0]]
    hist_interval = hist_interval.rename(columns={0:'训练模型分数区间'})

    # 新模型每日现在滑档的百分比 
    # 新模型的分数区间 
    daily_interval = (daily_level_pd.T)
    daily_interval = daily_interval.reset_index()
    daily_interval = daily_interval.rename(columns={'index':'新模型每日分数的百分比',0:'新模型分数区间'})
    
    # 关联关系传导后新模型的分数区间 
    new_intervals = pd.DataFrame(data = labeled_intervals)
    new_intervals_df=(new_intervals.T)
    newmaxnum = new_intervals_df[1].iloc[-1]
    new_intervals_df.loc[len(new_intervals_df)]=[None, None]
    new_intervals_df.iloc[-1] = newmaxnum
    new_intervals_df = new_intervals_df[[0]]
    new_intervals_df = new_intervals_df.rename(columns={0:'关联关系传导后新模型分数区间'})
    
    # 过渡模型滑档区间
    trans_intervals = transfer.score_trans(trans_amount)
    trans_intervals = pd.DataFrame(data = trans_intervals)
    trans_intervals_df=(trans_intervals.T)
    transmaxnum = trans_intervals_df[1].iloc[-1]
    trans_intervals_df.loc[len(trans_intervals_df)]=[None, None]
    trans_intervals_df.iloc[-1] = transmaxnum
    trans_intervals_df = trans_intervals_df[[0]]
    trans_intervals_df = trans_intervals_df.rename(columns={0:'过渡模型分数区间'})
    
    # 索引列'
    # index = pd.DataFrame({"预警等级":[0,1,2,3,4,5,6,7,8,9,10]})
    
    all_intervals = pd.concat([
        hist_interval,daily_interval,new_intervals_df,sys_intervals_df,trans_intervals_df
        ],axis=1)
    # all_intervals = all_intervals[:-1]
    all_intervals.index.name = '预警等级'  
    
    all_intervals.to_pickle(output_dict['model']+'all_intervals.pkl')
    # ------------------------------------------------------------------------
    #                     新模型划档区间
    # ------------------------------------------------------------------------
    # 如果之后跑出来的预警分数为10的数量有大波动可能需要再调一下
    # intervals_final = pd.DataFrame()
    # intervals_final['前-新模型划档百分位区间'] = [0,0.15,0.3,0.45,0.6,0.725,0.85,0.9,0.95,0.97,0.997]
    # intervals_final['现-新模型划档百分位区间'] = [0,0.15,0.3,0.45,0.6,0.725,0.85,0.9,0.95,0.97,0.98]
    # intervals_final.index.name = '预警等级'    
    
    # intervals_final.to_pickle(output_dict['model']+'aintervals_final.pkl')

    # ------------------------------------------------------------------------
    #                             债券预警得分&等级计算
    # ------------------------------------------------------------------------
    # starttime = datetime.datetime.now()
    # # 1. 数据获取&预处理
    # warn_entityscore = pd.read_pickle(
    #    'C:/Users/cyjiang/Documents/WeChat Files/wxid_4114231136012/FileStorage/File/2024-02/' +'warn_entityscore.pkl')
    
    # b_e_dict = pd.read_pickle(output_dict['entity']+'b_e_dict.pkl')

    # # 获取预警债券范围
    # bond_res_t = pd.read_pickle(output_dict['model']+'bond_res_t.pkl')

    # # -------important：需要确认当前获取的系统债券等级有重复的且预警分数不同的以哪一条为准，然后修改下述代码进行去重
    # # bond_res_t.sort_values(by=['预警得分'],inplace=True,ascending=False)
    # # bond_res_t=bond_res_t.drop_duplicates(subset=['债券编码','债券名称'], keep='first', inplace=False, ignore_index=True)

    # score_price_initial = pd.read_pickle(
    #     input_path+'成交估值打分最终结果文件(new).pkl')  # 各债波动

    # b_unexpired, b_expired = entity2bondv2.data_preprocess(
    #     b_e_dict, bond_res_t, score_price_initial, target_date, w_price)
    # b_unexpired.to_pickle(output_dict['model']+'b_unexpired.pkl')
    # b_expired.to_pickle(output_dict['model']+'b_expired.pkl')

    # # 2. 获取当日各债价格异动线性衰减后得分- 无价格波动的债项（日期为NaT，已填为当日）
    # b_unexpired = pd.read_pickle(output_dict['model']+'b_unexpired.pkl')
    # b_unexpired_d = entity2bondv2.get_Decayed_Bond_Price_Score(
    #     b_unexpired, target_date, w_price)
    # b_unexpired_d.to_pickle(output_dict['model']+'b_unexpired_d.pkl')

    # # 3. 衰减后压缩 compressed_value=nan为该主体下各债均无波动
    # b_unexpired_d = pd.read_pickle(output_dict['model']+'b_unexpired_d.pkl')
    # b_unexpired_d_c = entity2bondv2.compress_Decayed_Bond_Price_Score(
    #     b_unexpired_d)
    # b_unexpired_d_c.to_pickle(output_dict['model']+'b_unexpired_d_c.pkl')

    # # 4.对存续债进行价格传导——传导到预警分数
    # b_unexpired_d_c = pd.read_pickle(
    #     output_dict['model']+'b_unexpired_d_c.pkl')

    # # 当日主体预警分数 entity_t
    # score_price_d_c = pd.read_pickle(
    #     output_dict['X_Price']+'score_price_d_c.pkl')  # 主体波动

    # # 尝试分段设置系数 --需要分析合理性
    # alpha1 = 0.005
    # alpha2 = 0.05

    # b_unexpired_score = entity2bondv2.get_Bond_Score(
    #     b_unexpired_d_c, entity_t, score_price_d_c, target_date, alpha1, alpha2)  # 存续债预警分数
    # b_unexpired_score['pd'] = b_unexpired_score['债券预警得分']/100

    # # 7. 获取债券预警等级（已到期债券等级设置为None）
    # # 存续债：
    # # 读取主体百分位pd划分区间,用于债券等级划分
    # daily_level_pd = pd.read_pickle(output_dict['model']+'daily_level_pd.pkl')
    # hist_level_pd = pd.read_csv(
    #     input_path+'hist_level_pd.csv', encoding='gbk', index_col=False)

    # # 标识为未到期的债券有一部分实际已到期（系统中无等级）
    # bond_unscored = b_unexpired_score.loc[pd.isna(
    #     b_unexpired_score['债券预警得分']) == True].reset_index(drop=True)
    # bond_unscored_level = bond_unscored.copy(deep=True)
    # bond_unscored_level['债券预警等级'] = np.nan

    # bond_scored = b_unexpired_score.loc[pd.isna(
    #     b_unexpired_score['债券预警得分']) == False].reset_index(drop=True)
    # bond_scored_level = cal_lr_result.get_bond_level(
    #     bond_scored, hist_level_pd, daily_level_pd, labels_level)

    # # 已到期债券：
    # b_expired_level = b_expired.copy(deep=True)
    # b_expired_level['债券预警等级'] = np.nan
    # b_expired_level['债券预警得分'] = np.nan

    # # 接下来的步骤：
    # # 1.b_unexpired_level_score和b_expired_level调整成一样的格式['债券编码','债券名称','预警等级','预警得分']（模型得出的预警等级与得分）
    # b_expired_level_final = b_expired_level[[
    #     '债券编码', '债券名称', '债券预警等级', '债券预警得分']].copy(deep=True)
    # b_expired_level_final.columns = ['债券编码', '债券名称', '预警等级', '预警得分']
    # bond_scored_level_final = bond_scored_level[[
    #     '债券编码', '债券名称', 'final_level', '债券预警得分']].copy(deep=True)
    # bond_scored_level_final.columns = ['债券编码', '债券名称', '预警等级', '预警得分']
    # bond_unscored_level_final = bond_unscored_level[[
    #     '债券编码', '债券名称', '债券预警等级', '债券预警得分']].copy(deep=True)
    # bond_unscored_level_final.columns = ['债券编码', '债券名称', '预警等级', '预警得分']

    # # 2.将上述三个dataframe通过pd.concat([df1,df2],axis=0).reset_index(drop=True)上下拼接成最终债券预警分数与等级的dataframe
    # bond_t = pd.concat([bond_scored_level_final,
    #                    bond_unscored_level_final], axis=0).reset_index(drop=True)
    # bond_t = pd.concat([bond_t, b_expired_level_final],
    #                    axis=0).reset_index(drop=True)

    # bond_t.to_pickle(output_dict['model']+'bond_t.pkl')

    # # 计算运行时间
    # endtime = datetime.datetime.now()
    # print('债券预警得分&等级计算完成，运行时间: '+str(endtime-starttime))

    # # ------------------------------------------------------------------------
    # #                                 输出当日债券预警分数与得分到指定pkl
    # # ------------------------------------------------------------------------

    # # 并行测试首日（无完整output）运行以下代码：
    # bond_t.to_pickle(output_dict['model']+'warn_bondscore.pkl')
    # bond_t.to_csv(output_dict['model'] +
    #               'warn_bondscore.csv', encoding='gbk', index=False)

    # # ----------- 并行测试第二日起运行以下代码：
    # warn_bondscore=pd.read_pickle(output_dict['model']+'warn_bondscore.pkl')
    # # warn_bondscore=pd.concat([warn_bondscore,bond_t],axis=0)
    # # warn_bondscore.to_pickle(output_dict['model']+'warn_bondscore.pkl')

    # # 计算总运行时间
    # endtime = datetime.datetime.now()
    # print('运行时间总计: '+str(endtime-starttime0))
