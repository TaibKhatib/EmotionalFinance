import requests

api_key = "f3db7c5fdd9d4800be8ff781f9b1ecac"
company = "apple"
from_date = "2025-09-06"
result = requests.get(f"https://newsapi.org/v2/everything?q={company}&from={from_date}&sortBy=publishedAt&apiKey={api_key}")
print(result.json)