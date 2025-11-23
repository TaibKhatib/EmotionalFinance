import os
from airflow.decorators import dag, task
from datetime import datetime, timedelta

api_key = os.environ.get("YOUR_API_KEY")

@dag(
    start_date=datetime(2023, 11, 21),
    schedule="@daily",
    catchup=False
)
def news_finance_pipeline():
    @task()
    def fetch_news(company, api_key, from_date):
        import requests
        recent_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        url = "https://newsapi.org/v2/everything"
        params = {"q": company, "from": recent_date, "sortBy": "publishedAt", "apiKey": api_key}
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    
    @task()
    def fetch_financials(ticker_symbol):
        import yfinance as yf
        ticker = yf.Ticker(ticker_symbol)
        return {
            "history": json.loads(ticker.history(period="1y").to_json(orient= records)),
            "financials": json.loads(ticker.financials.to_json(orient= records)),
            "actions": json.loads(ticker.actions.to_json(orient= records))
        }
    
    @task()
    def log_results(news, financials):
        print("Fetched News:", news)
        print("Fetched Financials:", financials)

    news = fetch_news("apple", api_key, "2025-11-20")
    financials = fetch_financials("AAPL")

    log_results(news, financials)

    # add a publish_to_kafka task here if needed


dag = news_finance_pipeline()
