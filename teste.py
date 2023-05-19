import requests
import base64

# Defina suas credenciais do Spotify
client_id = 'e56974f9cc774256a5e98f927eb91447'
client_secret = 'e96c3cadeb184545b78007789e8fa718'

# Codifique as credenciais para Base64
credentials = f'{client_id}:{client_secret}'
encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')

# URL de autenticação
auth_url = 'https://accounts.spotify.com/api/token'

# Parâmetros para a solicitação de autenticação
auth_headers = {
    'Authorization': f'Basic {encoded_credentials}',
}

auth_data = {
    'grant_type': 'client_credentials',
}

# Faça a solicitação de autenticação
auth_response = requests.post(auth_url, headers=auth_headers, data=auth_data)
auth_response_data = auth_response.json()

# Extraia o token de acesso da resposta de autenticação
access_token = auth_response_data['access_token']

# URL da API do Spotify para pesquisar podcasts
search_url = 'https://api.spotify.com/v1/search'

# Parâmetros para a pesquisa de podcasts
headers = {
    'Authorization': f'Bearer {access_token}',
}

params = {
    'query': 'data hackers',
    'type': 'show',
    'market':'BR',    
    'limit' : 1
}
paramspod = {'market':'BR',}
# Faça a solicitação para pesquisar podcasts
response = requests.get(search_url, headers=headers, params=params)
response_data = response.json()

# Imprima os títulos dos podcasts encontrados
for item in response_data['shows']['items']:
    id = item['id']
    print(item['id'])
    print(item['name'])
    print(item['description'])
    
    podcast_url = f'https://api.spotify.com/v1/shows/{id}'
    response = requests.get(podcast_url, headers=headers, params=paramspod)
    podcast_data = response.json()
    
    #print(podcast_data)

    for pod in podcast_data['episodes']['items']:
        print(pod['id'])
        print(pod['release_date'])
        print(pod['duration_ms'])
        print(pod['language'])
        print(pod['explicit'])
        print(pod['type'])

    print('######################')