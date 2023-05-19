import json
import base64
import requests
import pandas as pd
import Biblioteca as bb
from Biblioteca import Google as gc
from getKeys import Keys

class SpotifyApi:
    def __init__(self):
        self.pq      = bb.File()
        self.credentials = Keys.getApiKeySpotify()
        self.auth_url = 'https://accounts.spotify.com/api/token'
        self.auth_headers = {'Authorization': f'Basic {self.credentials}',}
        self.auth_data = {'grant_type': 'client_credentials',}

        self.nm_file = 'spotify.parquet'
        self.project = 'boticario-prd-srv'
        self.dataset = 'Spotify'
        self.table   = 'tbl_spotify'        
        self.storage = 'Parquet/'
        self.parquet = '.\{}'.format(self.nm_file)    
        self.json_auth = Keys.getJsonGCP()
        self.proj_dat_table_top = self.project + '.' + self.dataset + '.' + self.table        
        self.url_file = 'gs://' + self.project + '/Parquet/' + self.nm_file
        

    def dfToParquet(self, df):
        self.pq.createParque(df=df, dir=self.parquet)
        
    def fileToStorage(self):
        result = gc.storageFileUpload(
                     project   = self.project
                    ,json_auth = self.json_auth
                    ,storage   = self.storage + self.nm_file
                    ,file      = self.nm_file
        )


    def getPodcastAll(self):
        auth_response = requests.post(self.auth_url, headers=self.auth_headers, data=self.auth_data)
        auth_response_data = auth_response.json()

        access_token = auth_response_data['access_token']
        search_url = 'https://api.spotify.com/v1/search'

        headers = {
            'Authorization': f'Bearer {access_token}',
        }

        params = {
            'query': 'data hackers',
            'type': 'show',
            'market':'BR',
            #'limit': 50,
        }
        paramspod = {'market':'BR',}

        response = requests.get(search_url, headers=headers, params=params)
        response_data = response.json()

        df = pd.DataFrame()
        for item in response_data['shows']['items']:
            id   = item['id']
            name = item['name']
            desc = item['description']
            epis = item['total_episodes']

            podcast_url = f'https://api.spotify.com/v1/shows/{id}'
            response = requests.get(podcast_url, headers=headers, params=paramspod)
            podcast_data = response.json()
    
            for pod in podcast_data['episodes']['items']:
                idpod = pod['id']
                nmepi = pod['name']
                dscep = pod['description']                
                rdate = pod['release_date']
                durat = pod['duration_ms']
                langu = pod['language']
                expli = pod['explicit']
                types = pod['type']

                dfjson = pd.DataFrame({
                                         'id_podcast':[id]
                                        ,'name_podcast':[name]
                                        ,'description_podcast':[desc]
                                        ,'total_episodes':[epis]
                                        ,'id_episodio':[idpod]
                                        ,'name_episodio':[nmepi]
                                        ,'description_episodio':[dscep]                                        
                                        ,'release_date':[rdate]
                                        ,'duration_ms':[durat]
                                        ,'language':[langu]
                                        ,'explicit':[expli]
                                        ,'type':[types]
                                        
                                      }) 
                df = pd.concat([df, dfjson],ignore_index=True)
            

        return df

    def getSpotifyToGCP(self):
        dfPodcast = SpotifyApi().getPodcastAll()
        print('### Tabela Spotify All Data Hackers ###')
        print('Fase 1: Busca de todos os Podcasts realizada com sucesso!')
        SpotifyApi().dfToParquet(dfPodcast)
        print('Fase 2: Arquivo parquet gerado com sucesso!')
        SpotifyApi().fileToStorage()
        print('Fase 3: Parquet importado no Storage com sucesso!')
        gc.bigqueryInsert(self.project, self.json_auth, self.proj_dat_table_top, self.url_file, False)
        print('Fase 4: Dados inseridos no Big Query com sucesso!')

        query = """
        CREATE OR REPLACE TABLE `boticario-prd-srv.Spotify.tbl_spotify_podcast_top50` 
        AS
        SELECT DISTINCT
             id_podcast as id
            ,name_podcast as name
            ,description_podcast as description 
            ,total_episodes
        FROM `boticario-prd-srv.Spotify.tbl_spotify` 
        LIMIT 50
        """
        gc.bqQuery(self.json_auth,query)
        print('Fase 5: Tabela spotify_podcasts_top criada com sucesso!')

        query = """
        CREATE OR REPLACE TABLE `boticario-prd-srv.Spotify.tbl_spotify_episodes` 
        AS
        SELECT DISTINCT
             id_episodio as id
            ,name_episodio as name
            ,description_episodio as description
            , * EXCEPT(id_podcast,name_podcast,description_podcast,total_episodes) 
        FROM `boticario-prd-srv.Spotify.tbl_spotify`         
        """
        gc.bqQuery(self.json_auth,query)
        print('Fase 5: Tabela spotify_episodios criada com sucesso!')

        print('### Tabela Spotify All Data Hackers ###')

SpotifyApi().getSpotifyToGCP() 