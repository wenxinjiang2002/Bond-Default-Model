# Public Bond Default Risk Scoring Model (债券违约风险评分模型)

## 1. Summary (Must read)

This repository contains the code for replicating a bond default risk scoring model, which I developed during my previous internship. The goal of this model is to evaluate the default risk of public bonds. Most data sources and partial codes are not uploaded to Github due to confidentiality, this repository is only for showcase [wenxinjiang2002] 's data analysis/modeling experience.

The model uses a logistic regression approach to automatically classify a default risk level for all bond on each analysis date. The final score is smoothed to ensure consistency over time and for better use for the clients.

All rights reserved. This code is proprietary and confidential. You may not use, copy, modify, or distribute this code without explicit permission from the author.

## 2. Overview of the Public Bond Warning Model Process

The warning model process is divided into the government level, industry level, issuer level, and bond level for model building. Among them, the issuer level primarily focuses on sentiment analysis, market dimensions, fundamentals, industry dimensions, government dimensions, and relational dimensions across six aspects to conduct a pressure index evaluation on bond issuers.

The general process of the warning model is as follows:

### Sentiment Analysis

Daily news is crawled through Python, and the sentiment score is calculated through sentiment algorithms and manually filtered to derive the final sentiment score.

### Market Dimension Calculation

Currently, the primary market data uses issuance information, and the secondary market data uses holding changes and bond prices, which are influenced by three factors:

a. **Issuance Information**: Obtained from Wind, which includes data on successful and failed bond issuances, and calculates the issuer’s historical issuance rate.

b. **Changes in Institutional Holdings**: Data from Adm covering changes in institutional holdings of various bonds from March 1, 2020, to June 1, 2023, is used to calculate the holding change rate for each bond.

c. **Bond Prices**: The highest deviation between the current interest rate and the valuation of bonds from the past day is calculated, along with trading volume and bias ranges, to derive a comprehensive score. For the selected issuer, the highest score on the bond with the greatest deviation is taken.

### Fundamentals Calculation

Decision trees are used to categorize the results from fundamental rating boxes into 1 to 5 levels.

### Pressure Index Calculation

Logistic regression and decision tree algorithms are used to calculate the weight of the issuer's overall score, and a backtest is performed to ensure the validity and accuracy of the model's calculation.

### Pre-warning Index and Warning Score

The warning score is calculated based on pre-set thresholds, currently focusing on continuous or normal warning scores.

### Issuer Pressure Index and Warning Levels Calculation

The issuer's bond-level pressure index is derived, along with the highest score and the respective warning grade. The highest score serves as the issuer’s pressure index, warning level, or alert score.

## 3. Data Scource
internal data source 

## 4. issuer level
### 4.1 Sentiment Dimension

### Data Retrieval:
- **Python Web Scraping**:
  - Retrieves news and announcements based on two paths: news direction (`新闻方向`) and announcement direction (`公告方向`).

### News Direction Calculation (`新闻计算`):
1. The news content is classified based on company name, bond name, issuer name, and announcement content.
2. The sentiment score is calculated based on the proportion of positive and negative content in the news article.
3. Daily sentiment scores for each event are calculated. Scores are adjusted by assigning a weight of 0.3 to recent events (events within 9 days) and 0.1 to older events (over 30 days ago).
4. The final sentiment score for the day is calculated using the following formula:
    - \( \text{sig\_news} = \sqrt{0.5 * \sum \text{(news sentiment score)} * 0.5 * \sum \text{(historical news score)} + 1} \)

### Announcement Direction Calculation (`公告计算`):
1. Similar to the news direction, company name, bond name, and issuer name are identified, and announcements are analyzed for sentiment.
2. A sentiment score for each announcement is derived.
3. The score is influenced by historical sentiment, calculated as:
    - \( \text{sig\_event} = \sqrt{\text{(sentiment score per announcement)}^2 + \text{historical sig\_event}^2 * 20.999} \)

### Final Calculation:
- **News Score**: `sig_news`
- **Announcement Score**: `sig_event`

#### 6.1.1 Retrieving Sentiment Data

Sentiment data is retrieved using a Python web crawler, which scrapes real-time financial news and regulatory announcements from designated websites 24 hours a day. The crawler downloads PDF files of announcements while also storing the local copies of the PDFs. The retrieved data includes time, title, content, and links (announcements do not have content). The following table shows the sources of sentiment data from both news and announcements:

