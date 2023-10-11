# AI-Enabled Stock Prediction With Social Sensing, Technical Analysis and Forecasting Techniques
 AI-Enabled Stock Prediction With Social Sensing, Technical Analysis and Forecasting Techniques


The application of both Machine Learning (ML) and sentiment analysis from 
microblogging services has become a common approach for stock market prediction. In 
this thesis, we analyzed the stock movements of three companies, namely Amazon, 
Microsoft, Apple and Tesla using both historical and sentiment big data. Specifically, 
we collected 19,790,818 tweets from Twitter covering the period from 31-11-2018 to 
31-12-2021. These tweets were collected with queries regarding either the company 
ticker or the company CEO. We also mined historical data from the Yahoo Finance 
website for the same period. The sentiment analysis of social media data was conducted 
using two specialized pre-trained models from Hugging Face: Twitter XLM-roBERTa 
and an alternative roBERTa model fine-tuned with data taken from Stocktwits. Also, 
multiple technical analysis indicators were created from historical data to aid with the 
final prediction. Finally, we used multiple forecasting algorithms to identify the best 
model to forecast the final prediction of price movement. We implemented multiple ML 
models, including KNN, SVM, Logistic Regression, Naïve Bayes, Decision Tree, 
Random Forest, and MLP. Our results indicate that when using tweets from Twitter 
with both sentiment models as the sentiment analysis tools, LGBM is the ML algorithm 
that gives the highest f-score of 62 % and an Area Under Curve (AUC) of 62%.
