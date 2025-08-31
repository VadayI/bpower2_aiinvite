import base64, hashlib
import requests, json


login = "jacek.rakoczy"
password = "Jacek2021"
sha = hashlib.sha256(password.encode()).hexdigest()
user_key = base64.b64encode(f"{login}:{sha}".encode()).decode()

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json"
}

url = "https://api.tasklytics.eu/app/legacy/login/v3?scope=PRODUCTION"
# UWAGA: curl -d '...' wysyła to jako zwykły tekst, więc w requests musisz dać data=..., nie json=...
resp = requests.post(url, headers=headers, data=user_key)
resp_json = resp.json()
token = resp_json[0]['cloudToken']

url = "https://api.tasklytics.eu/app/email/message/mails"

params = {
    "page": 1,
    "messagePerPage": 100,
    "mailBoxId": 1177807,
    "folder": "SENT"
}

headers = {
    "Accept": "application/json",
    "Authorization": token
}

resp = requests.get(url, headers=headers, params=params)
data = resp.json()
url = "https://api.tasklytics.eu/app/email/message/details"
for item in data['default']['data']:
    params = {
        "mailBoxId": 1177807,
        "messageId": item['Id']
    }
    resp = requests.get(url, headers=headers, params=params)
    data = resp.json()
    print(data)