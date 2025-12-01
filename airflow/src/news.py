import requests
import json
api_key = "f3db7c5fdd9d4800be8ff781f9b1ecac"
company = "apple inc"
from_date = "2025-11-31"
result = requests.get(f"https://newsapi.org/v2/everything?q={company}&from={from_date}&sortBy=publishedAt&apiKey={api_key}")
data = json.dumps(result.json(), indent=4, ensure_ascii=False)
print(data)

with open("C:\\Users\\taibk\\Downloads\\news_output.json", "w", encoding="utf-8") as f:
    json.dump(result.json(), f, indent=4, ensure_ascii=False)
