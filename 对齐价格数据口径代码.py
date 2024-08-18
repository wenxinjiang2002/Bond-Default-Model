import pandas as pd
import numpy as np
import datetime

# 读取app版数据（Excel文件
price_df = pd.read_excel('/Users/wenxinjiang/Desktop/德勤/债权价格取数对齐/pythonProject/[手机版]成交净价偏离20240108-20240201.xlsx')
rate_df = pd.read_excel('/Users/wenxinjiang/Desktop/德勤/债权价格取数对齐/pythonProject/[手机版]收益率偏高中债估值20240129-20240201.xlsx')

# 价格昨离偏收 columns names
price_dev_columns = ['序号','债券代码','债券简称','成交日期','成交时间',
                     '偏离幅度（%）','成交价（元）','前收盘价（元）','基准日期','成交量（手）',
                     '债券类型','上市市场','上市日期','到期日期','债项评级','主体评级','发债主体',
                     '地区（省级）','地区（地级）','地区（区县级）','行业（一级）','行业（二级）','企业性质','是否城投']

# 收益率偏高中债 columns names
rate_dev_columns = ['序号','债券代码','债券简称','成交日期','成交时间',
                    '偏离幅度（BP）','成交到期收益率（%）','估值收益率（%）','对比类型','估值日','成交量（手）',
                    '债券类型','上市市场','上市日期','到期日期','债项评级','主体评级','发债主体',
                    '地区（省级）','地区（地级）','地区（区县级）','行业（一级）','行业（二级）','企业性质','是否城投']

# 多列日期需要修改，列名放在一个列表中
date_columns = ['成交日期', '上市日期', '到期日期']  # 请替换为你的列名

# 遍历每个日期列，将日期从“yyyy/mm/dd”格式转换为“yyyy-mm-dd”格式
for col in date_columns:
    price_df[col] = pd.to_datetime(price_df[col], format='%Y/%m/%d').dt.strftime('%Y-%m-%d')
    rate_df[col] = pd.to_datetime(rate_df[col], format='%Y/%m/%d').dt.strftime('%Y-%m-%d')

# 将所属地区改为‘地区（省级））’，将所属行业改为‘行业（一级）’
price_df.rename(columns={'所属地区': '地区（省级）', '所属行业': '行业（一级）'}, inplace=True)
rate_df.rename(columns={'所属地区': '地区（省级）', '所属行业': '行业（一级）'}, inplace=True)

# 添加新列：地区（市级），地区（区县级），行业（二级）；所有设为null值
price_df['地区（地级）'] = np.nan
price_df['地区（区县级）'] = np.nan
price_df['行业（二级）'] = np.nan
rate_df['地区（地级）'] = np.nan
rate_df['地区（区县级）'] = np.nan
rate_df['行业（二级）'] = np.nan

# # 确保所有新列都在df中，如果某些列不存在，则创建并填充为NaN
# for col in price_dev_columns:
#     if col not in price_df.columns:
#         price_df[col] = np.nan
# for col in rate_dev_columns:
#     if col not in rate_df.columns:
#         rate_df[col] = np.nan

# 重新排序DataFrame以匹配新的列顺序
price_df = price_df.reindex(columns=price_dev_columns)
rate_df = rate_df.reindex(columns=rate_dev_columns)

# # 将“成交量（手）”列的每个值除以10，并将列名更改为“成交面额（万）”
# df['成交面额（万）'] = df['成交量（手）'] / 10
# # 删除原始的“成交量（手）”列
# df.drop(columns=['成交量（手）'], inplace=True)

# 保存修改成电脑版格式的价格文件
price_df.to_excel('/Users/wenxinjiang/Desktop/德勤/债权价格取数对齐/pythonProject/价格偏离昨收20240108-20240201.xlsx', index=False)
rate_df.to_excel('/Users/wenxinjiang/Desktop/德勤/债权价格取数对齐/pythonProject/收益率偏离中债估值20240129-20240201.xlsx', index=False)