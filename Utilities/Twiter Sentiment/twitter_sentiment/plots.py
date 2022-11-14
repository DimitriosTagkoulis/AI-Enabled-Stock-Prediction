# -*- coding: utf-8 -*-
"""

@author: Paschalis
"""

import matplotlib.pyplot as plt
import matplotlib as mpl
import seaborn as sns
import pandas as pd
import mysql.connector
import time
import  settings
import numpy as np

def Count(df, list, x, y):
    word1 = 0
    word2 = 0
    word3 = 0
  

    for i in df:
        for z in list:
            if z == settings.TRACK_WORDS[0]:
                word1 += i
            elif z == settings.TRACK_WORDS[1]:
                word2 += i
            elif z == settings.TRACK_WORDS[2]:
                word3 += i
            

    data = [(settings.TRACK_WORDS[0], word1), (settings.TRACK_WORDS[1], word2), (settings.TRACK_WORDS[2], word3)]
    freq = pd.DataFrame(data, columns=[x, y])
    print(freq)
    return freq


def NumofTweets(df, list, x, y):
    word1 = 0
    word2 = 0
    word3 = 0
    
    for z in l:
        if z == settings.TRACK_WORDS[0]:
            word1 += 1
        elif z == settings.TRACK_WORDS[1]:
            word2 += 1
        elif z == settings.TRACK_WORDS[2]:
            word3 += 1
        

    data = [(settings.TRACK_WORDS[0], word1), (settings.TRACK_WORDS[1], word2), (settings.TRACK_WORDS[2], word3)]
    freq = pd.DataFrame(data, columns=[x, y])
    print(freq)
    return freq

def Timeseries(df, track):
    # UTC for date time at default
    df['created_at'] = pd.to_datetime(df['created_at'])
    print("Candidates Negative Tweets Monitor: ")
    for index, tweets in df[df['sentiment'] == 'NEGATIVE'].iterrows():
        print("  " + str(tweets[2]) + "  " + tweets[1])

    # Clean and transform data to enable time series
    result = df.groupby([pd.Grouper(key='created_at', freq='5min'), 'sentiment']).count() \
        .unstack(fill_value=0).stack().reset_index()
    result['created_at'] = pd.to_datetime(result['created_at']).apply(lambda x: x.strftime('%m-%d %H:%M'))

    # Plot Line Chart for monitoring enterprise awareness on Twitter
    mpl.rcParams['figure.dpi'] = 200
    plt.figure(figsize=(16, 6))
    sns.set(style="darkgrid")
    ax = sns.lineplot(x="created_at", y="id_str", hue='sentiment', data=result, \
                      palette=sns.color_palette(["#FF5A5F", "#484848", "#767676"]))
    ax.set(xlabel='Time Series in UTC', ylabel="Number of '{}' mentions".format(track))
    plt.legend(title='Attitude towards enterprise', loc='upper left', labels=['Negative', 'Normal', 'Positive'])
    sns.set(rc={"lines.linewidth": 1})
    plt.show()

def Densityplots(data1, data2, data3):
    d = sns.kdeplot(data1, shade=True, color="r")
    d = sns.kdeplot(data2, shade=True, color="b")
    d = sns.kdeplot(data3, shade=True, color="g")
    plt.show()

def BarPlot(freq, x, y, color):
    height = freq[y]
    bars = freq[x]
    y_pos = np.arange(len(bars))
    plt.bar(y_pos, height, color=color)
    plt.xticks(y_pos, bars)
    plt.show()

def CategoricalSentiment(n, s):
        names = ['NEGATIVE', 'NEUTRAL', 'POSITIVE']
        values = n
        plt.figure(figsize=(10, 3))
        plt.subplot(131)
        plt.bar(names, values)
        plt.subplot(132)
        plt.scatter(names, values)
        plt.subplot(133)
        plt.plot(names, values)
        plt.suptitle(s)
        plt.show()

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="",
    database="main"

)
#Take values from the database
df_amazon = pd.read_sql("SELECT id_str, text, created_at, sentiment, user_location, track FROM {} WHERE track = 'AMAZON'".format(settings.TABLE_NAME), con=mydb)
df_microsoft = pd.read_sql("SELECT id_str, text, created_at, sentiment, user_location, track FROM {} WHERE track = 'MICROSOFT'".format(settings.TABLE_NAME), con=mydb)
df_google = pd.read_sql("SELECT id_str, text, created_at, sentiment, user_location, track FROM {} WHERE track = 'GOOGLE'".format(settings.TABLE_NAME), con=mydb)

followers = pd.read_sql('SELECT user_followers_count FROM {}'.format(settings.TABLE_NAME), con=mydb)
tweet_counter = pd.read_sql('SELECT tweet_count FROM {}'.format(settings.TABLE_NAME), con=mydb)
theme = pd.read_sql('SELECT track FROM {}'.format(settings.TABLE_NAME), con=mydb)
l = []
for i in theme.values:
    for j in i:
        l.append(j)

pol_amazon = pd.read_sql("SELECT  polarity as polarity_amazon FROM {} WHERE track = 'AMAZON'".format(settings.TABLE_NAME), con=mydb)
pol_microsoft = pd.read_sql("SELECT  polarity as polarity_microsoft FROM {} WHERE track = 'MICROSOFT'".format(settings.TABLE_NAME), con=mydb)
pol_google = pd.read_sql("SELECT  polarity as polarity_google FROM {} WHERE track = 'GOOGLE'".format(settings.TABLE_NAME), con=mydb)
subj_amazon = pd.read_sql("SELECT  subjectivity as subjectivity_amazon FROM {} WHERE track = 'AMAZON'".format(settings.TABLE_NAME), con=mydb)
subj_microsoft = pd.read_sql("SELECT  subjectivity as subjectivity_microsoft FROM {} WHERE track = 'MICROSOFT'".format(settings.TABLE_NAME), con=mydb)
subj_google = pd.read_sql("SELECT  subjectivity as subjectivity_google FROM {} WHERE track = 'GOOGLE'".format(settings.TABLE_NAME), con=mydb)

#Tables with the number of attributes for each track word
followers = Count(followers['user_followers_count'], l, 'Enterprise', 'Number of followers')
UserTweets = Count(tweet_counter['tweet_count'], l, 'Enterprise', 'Number of users tweets')
tweets_no = NumofTweets(theme['track'], l, 'Enterprise', 'Number of tweets')

#Bar plots that shows the distribution of observations at each track word
BarPlot(followers, 'Enterprise', 'Number of followers', ('#ff4554', '#ff6714', '#a12222'))
BarPlot(UserTweets, 'Enterprise', 'Number of users tweets',  ('#ff8000', '#dc6900', '#ffce26'))
BarPlot(tweets_no, 'Enterprise', 'Number of tweets',  ('#00c3e3', '#7170ff', '#8afccf'))

#Time series graph for the flactuation of the sentiment through time
Timeseries(df_amazon, settings.TRACK_WORDS[0])
Timeseries(df_microsoft, settings.TRACK_WORDS[1])
Timeseries(df_google, settings.TRACK_WORDS[2])

#Density plots for the sentiment analysis at polarity and subjectivity for the track words
Densityplots(pol_amazon["polarity_amazon"], pol_microsoft["polarity_microsoft"], pol_google["polarity_google"])
Densityplots(subj_amazon["subjectivity_amazon"], subj_microsoft["subjectivity_microsoft"], subj_google["subjectivity_google"])

#Count the sentiment from each category
cursor = mydb.cursor()
cursor.execute("select sentiment,count(sentiment) from main.main group by sentiment;")
rows = cursor.fetchall()
cursor.execute("select count(sentiment) from main.main where track = 'AMAZON' group by sentiment;")
amazon = [item[0] for item in cursor.fetchall()]
cursor.execute("select count(sentiment) from main.main where track = 'MICROSOFT' group by sentiment;")
microsoft = [item[0] for item in cursor.fetchall()]
cursor.execute("select count(sentiment) from main.main where track = 'GOOGLE' group by sentiment;")
google = [item[0] for item in cursor.fetchall()]

CategoricalSentiment(amazon, 'Categorical sentiment for Amazon')
CategoricalSentiment(microsoft, 'Categorical sentiment for Microsoft')
CategoricalSentiment(google, 'Categorical sentiment for Google')