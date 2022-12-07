import json
import requests
import pandas as pd
from Biblioteca import File as pq
from Biblioteca import Google as gc
from getKeys import Keys

class TwitterApi:
    def __init__(self):
        self.bearer = "Bearer " + Keys.getApiKey()
        self.headers = {"Authorization": self.bearer}
        self.nm_file = 'twitter.parquet'
        self.parquet = '.\{}'.format(self.nm_file)        
        self.project = 'boticario-prd'
        self.dataset = 'Twitter'
        self.table   = 'tbl_twitter'        
        self.storage = 'Parquet/'
        self.json_auth = Keys.getJsonGCP()
        self.proj_dat_table = self.project + '.' + self.dataset + '.' + self.table
        self.url_file = 'gs://' + self.project + '/Parquet/' + self.nm_file

    def getApi(self, endpoint):
        json_string = requests.get(url=endpoint,headers=self.headers).json()
        df = pd.json_normalize(json_string, 
                                record_path='data',
                                errors='ignore'
                                )
        return df

    def getTweets(self):
        limitResults, busca = 50, 'Boticario Maquiagem'    
        endpoint = "https://api.twitter.com/2/tweets/search/recent?max_results={}&expansions=author_id&query={}".format(limitResults, busca)
        df = TwitterApi().getApi(endpoint)    
        return df

    def getUsers(self, dfTweets):
        dfUsers = pd.DataFrame()
        for index, row in dfTweets.iterrows():    
                    endpoint = "https://api.twitter.com/2/users?ids={}".format(dfTweets.iloc[index].author_id)
                    df = TwitterApi().getApi(endpoint)                
                    dfUsers = pd.concat([dfUsers,df])
        
        return dfUsers

    def dfToParquet(self, df):
        pq.createParque(df=df, dir=self.parquet)
        
    def fileToStorage(self):
        result = gc.storageFileUpload(
                     project   = self.project
                    ,json_auth = self.json_auth
                    ,storage   = self.storage + self.nm_file
                    ,file      = self.nm_file
        )

    def getTwitterToGCP(self):
        dfTweets = TwitterApi().getTweets()
        dfUsers  = TwitterApi().getUsers(dfTweets)
        df = pd.merge(left=dfTweets, right=dfUsers, how='left',left_on='author_id',right_on='id')
        TwitterApi().dfToParquet(df)
        gc.bigqueryInsert(self.project, self.json_auth, self.proj_dat_table, self.url_file)

TwitterApi().getTwitterToGCP()