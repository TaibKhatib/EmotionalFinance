import requests
import json
url = "https://api.gdeltproject.org/api/v2/doc/doc"
params = {
    "query": "apple sourcelang:english ",
    "mode": "artList",
    "maxrecords": "10",
    "timespan": "1week",
    "format": "json"
}
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

result = requests.get(url, params=params, headers=headers)
print("FULL URL:", result.url)
print(json.dumps(result.json(), indent=4, ensure_ascii=False))