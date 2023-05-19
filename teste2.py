import requests

# Defina o ID do podcast que você deseja buscar
podcast_id = '2yaqJ4H0jc11phvCA60wKn'

# Defina suas credenciais do Spotify
client_id = 'e56974f9cc774256a5e98f927eb91447'
client_secret = 'e96c3cadeb184545b78007789e8fa718'

# URL de autenticação
auth_url = 'https://accounts.spotify.com/api/token'

# Parâmetros para a solicitação de autenticação
auth_data = {
    'grant_type': 'client_credentials',
    'client_id': client_id,
    'client_secret': client_secret
}

# Faça a solicitação de autenticação
auth_response = requests.post(auth_url, data=auth_data)
auth_response_data = auth_response.json()

# Extraia o token de acesso da resposta de autenticação
access_token = auth_response_data['access_token']

# URL da API do Spotify para buscar dados do podcast
podcast_url = f'https://api.spotify.com/v1/shows/{podcast_id}'
podcast_url = f'https://api.spotify.com/v1/shows/2yaqJ4H0jc11phvCA60wKn'
id = '38bS44xjbVVZ3No3ByF1dJ'
podcast_url = f'https://api.spotify.com/v1/shows/{id}'

# Parâmetros para a busca do podcast
headers = {
    'Authorization': f'Bearer {access_token}',
}

params = {
    'query': 'data hackers',
    'type': 'show',
    'market':'BR',    
    'limit' : 2
}


# Faça a solicitação para buscar dados do podcast
response = requests.get(podcast_url, headers=headers, params=params)
podcast_data = response.json()

# Imprima os dados do podcast
#print('Título:', podcast_data['name'])
#print('Descrição:', podcast_data['description'])
#print('Número de episódios:', podcast_data['total_episodes'])
# ... e assim por diante, você pode acessar outros campos do podcast_data conforme necessário

print(podcast_data)
