import requests
'''
url = 'http://127.0.0.1:5000/process_data'
data = {'tenant_id': 'sow', 'usecase_id': 'cf'}

response = requests.post(url, data=data)

print(response.json())
'''

url = 'http://127.0.0.1:5000/api'

# POST request
data_to_post = {'tenant_id': 'sow', 'usecase_id': 'cf'}
response_post = requests.post(url, json=data_to_post)
print(response_post.json())

# GET request
response_get = requests.get(url)
print(response_get.json())