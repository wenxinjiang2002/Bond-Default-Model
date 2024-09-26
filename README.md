# Bond Default Risk Scoring Model (债券违约风险评分模型)

### Project Description (项目描述)

This repository contains the code for replicating a bond default risk scoring model, which I developed during my previous internship. The goal of this model is to evaluate the default risk of bonds based on four key factors:

- Fundamental Analysis: Analyzing companies' financial health through financial statement analysis. 
- News Announcements: Conducting sentiment analysis on company announcements and relevant news articles.
- Primary Market Information: Using data from bond issuance events to gauge the risk.
- Secondary Market Prices: Analyzing bond price movements in the secondary market.

The model uses a logistic regression approach to automatically classify a default risk level for all bond on each analysis date. The final score is smoothed to ensure consistency over time and for better use for the clients.

# License (许可证)

Copyright [2024] [wenxinjiang2002].

Most data sources and partial codes are not uploaded to Github due to confidentiality, this repository is only for showcase [wenxinjiang2002] 's data analysis/modeling experience.

All rights reserved. This code is proprietary and confidential. You may not use, copy, modify, or distribute this code without explicit permission from the author.
