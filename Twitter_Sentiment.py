# Application to pull streaming data from twitter and determine the sentiment of them.
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
import sys
import webbrowser
import codecs
import csv
from string import punctuation
import matplotlib.pyplot as plt
import time
import IPython.display from IPython

class tweetlistener(StreamListener):


    def on_status(self,status):
        global counter,Total_tweet_count,outfile,search_words_list,indiv,outfile
        counter += 1
        if counter >= Total_tweet_count:
            search_words_list.pop(0)
            outfile.close()
            senti1 = Sentiment()
            senti1.sentiment_analysis()
            #time.sleep(15)
            search_tweets()

        try:
            print "----------NEW TWEET RECEIVED!-----------"
            print "Tweet Text : %s" % status.text
            outfile.write(status.text)
            outfile.write(str("\n"))
            print "Author's Screen name : %s" % status.author.screen_name
            print "Time of creation : %s" % status.created_at
            print "Source of Tweet : %s" % status.source
        except UnicodeEncodeError:
            print "Skipping a tweet"

    def on_error(self, status):
        drawing()
        print "Too soon reconnected . Will terminate the program"
        print status
        sys.exit()

class Sentiment():
    def sentiment_analysis(self):
        global file2,indiv,outfile,labels,colors,all_figs
        pos_sent = open("positive_words.txt").read()
        positive_words = pos_sent.split('\n')
        positive_counts = []
        neg_sent = open('negative_words.txt').read()
        negative_words = neg_sent.split('\n')
        outfile.close()
        negative_counts = []
        conclusion = []
        tweets_list = []
        tot_pos = 0
        tot_neu = 0
        tot_neg = 0
        all_total = 0
        #print file2
        tweets = codecs.open(file2, 'r', "utf-8").read()
        tweet_list_dup = []

        tweets_list = tweets.split('\n')
        #print tweets_list

        for tweet in tweets_list:
            positive_counter = 0
            negative_counter = 0
            tweet = tweet.encode("utf-8")
            tweet_list_dup.append(tweet)
            tweet_processed = tweet.lower()

            for p in list(punctuation):
                tweet_processed = tweet_processed.replace(p, '')

            words = tweet_processed.split(' ')
            word_count = len(words)
            for word in words:
                if word in positive_words:
                    positive_counter = positive_counter + 1
                elif word in negative_words:
                    negative_counter = negative_counter + 1

            positive_counts.append(positive_counter)
            negative_counts.append(negative_counter)
            if positive_counter > negative_counter:
                conclusion.append("Positive")
                tot_pos += 1
            elif positive_counter == negative_counter:
                conclusion.append("Neutral")
                tot_neu += 0.5
            else:
                conclusion.append("Negative")
                tot_neg +=1

        #print len(positive_counts)
        output = zip(tweet_list_dup, positive_counts, negative_counts,conclusion)
        #output = output.encode('utf-8')

        print "******** Overall Sentiment Analysis **************"


        if tot_pos > tot_neg and tot_pos > tot_neu:
            print "Overall Sentiment - Positive"
        elif tot_neg > tot_pos and tot_neg > tot_neu:
            print "Overall Sentiment - Negative"
        elif tot_neg == tot_neu and tot_neg > tot_pos:
            print "Overall Sentiment - Negative"
        elif tot_pos + tot_neg < tot_neu:
            print "Overall Sentiment - Semi Positive "
        else:
            print "Overall Sentiment - Neutral"


        print "%%%%%%%%%%%% End of stream - " + indiv + "   %%%%%%%%%%%%%%%%%%%%%"

        file1 = 'tweet_sentiment_' + indiv + '.csv'
        writer = csv.writer(open(file1, 'wb'))
        writer.writerows(output)
        draw_helper = []
        draw_helper.append(tot_pos)
        draw_helper.append(tot_neg)
        draw_helper.append(tot_neu)
        draw_helper.append(indiv)
        all_figs.append(draw_helper)

        #figs.append(drawing())


def drawing():
        global all_figs
        for one_fig in all_figs:
            all_total = 0
            sentiments = {}
            sentiments["Positive"] = one_fig[0]
            sentiments["Negative"] = one_fig[1]
            sentiments["Neutral"]  = one_fig[2]
            all_total = one_fig[0] + one_fig[1] + one_fig[2]
            sizes = []

            sizes = [sentiments['Positive']/float(all_total), sentiments['Negative']/float(all_total), sentiments['Neutral']/float(all_total)]


            plt.pie(sizes,labels=labels, colors=colors, autopct='%1.1f%%', shadow=True)
            plt.axis('equal')

            plt.title('sentiment for the word - ' + str(one_fig[3]))
            fig_name = "fig_" + str(one_fig[3]) + ".png"
            # Save the figures
            plt.savefig(fig_name)
            plt.close()
        plt.show()


def main():
    global Total_tweet_count,outfile,file,search_words_list,auth,labels,colors,all_figs
    consumer_key = 'O9KXKiFmfzTNgF0eevXXXX'
    consumer_secret = 'ozgNXFyi4A0rimGGPx8bGJHLGosJibGiFASZbXXXXX'
    access_token = '300198545-EsrLh8Xh9OzkRUkjwubPomH0M4GS3pXOfGgBLXXX'
    access_secret = 'NlqKNVpnbYK1T5WuOROjSdGrmSfxy8mluggN0w36uzxXXX'

    search_words = str(raw_input("Enter Search words - separate them by comma: "))
    Total_tweet_count = int(raw_input("Enter tweets to be pulled for each search word: "))
    #print search_words
    search_words_list = search_words.split(",")
    Total_tweet_count = 10
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    labels = ['Positive','Negative','Neutral']
    colors = ['yellowgreen','lightcoral','gold']
    all_figs= []
    search_tweets()
    outfile = codecs.open("F:\\test_tweets1.txt", 'w', "utf-8")#iphone

def search_tweets():
    global search_words_list,counter,auth,indiv,outfile,file2,plt,access
    consumer_key = 'ZkIxjbsPacixuhTg7aclkQ'
    consumer_secret = 'yme0jG3UDhG0CFgqlc50UQFSspo3EkUfPziUf2FFo'
    access_token = '1635433267-29ZpqtvpBIzVOQTnz1wgCsaotyEBTgs4V4jkUEM'
    access_secret = '33ZEGzs7pR1M0AYnD0mwOaZJ8JIF1Nc183VOFNkeug'
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    #auth.set_access_token(access_token, access_secret)
    print search_words_list
    for indiv in search_words_list:
        #indiv = indiv.split()
        print "Search Word - " + indiv + " - is being processed"
        counter = 0
        file2 = "test_" + str(indiv[0]) + ".txt"
        outfile = codecs.open(file2, 'w', "utf-8")
        twitterStream = Stream(auth, tweetlistener())
        one_list = []
        one_list.append(indiv)
        print one_list
        twitterStream.filter(track=one_list,languages = ["en"])
    #for i in range(len(figs)):
    drawing()
    sys.exit()

main()
