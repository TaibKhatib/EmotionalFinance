from airflow.decorators import dag, task
from datetime import datetime

@dag(start_date=datetime(2025,11,20), schedule_interval=None, catchup=False)
def news_finance_pipeline():
    @task()
    def fetch_news(company, api_key, from_date):
        import requests
        url = "https://newsapi.org/v2/everything"
        params = {"q": company, "from": from_date, "sortBy": "publishedAt", "apiKey": api_key}
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    
    @task()
    def fetch_financials(ticker_symbol):
        import yfinance as yf
        ticker = yf.Ticker(ticker_symbol)
        return {
            "history": ticker.history(period="1y").to_dict(),
            "financials": ticker.financials.to_dict(),
            "actions": ticker.actions.to_dict()
        }
    
    news = fetch_news("apple", "YOUR_API_KEY", "2024-09-06")
    financials = fetch_financials("AAPL")

    # add a publish_to_kafka task here if needed
    

pipeline_dag = news_finance_pipeline()
