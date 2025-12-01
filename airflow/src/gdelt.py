import requests
import json
url = "https://api.gdeltproject.org/api/v2/doc/doc"
params = {
    "query": "(apple OR AAPL) ( decision OR innovate OR forecast) sourcelang:english",
    "mode": "artList",
    "timespan": "1w",
    "format": "json"
}
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

result = requests.get(url, params=params, headers=headers)
print("FULL URL:", result.url)


articles = result.json()["articles"]
URLs=[article["url"] for article in articles]
print(URLs)
print(len(articles))