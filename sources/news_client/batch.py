import os
import sys
from pathlib import Path

# Add parent directory to path to allow imports when running directly
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from newsapi import NewsApiClient
from utils.time_client.last_timestamp import get_current_utc_time,get_last_timestamp
load_dotenv()

api_key = os.getenv("NEWSAPI_KEY")
newsapi = NewsApiClient(api_key=api_key)

query_ar="""
(
    بيتكوين OR العملات الرقمية OR سوق العملات الرقمية
) OR (
    الذهب OR أسعار الذهب OR المعادن الثمينة
) OR (
    النفط OR أسعار النفط OR الطاقة
) OR (
    الشرق الأوسط OR حرب غزة OR صراع إيران إسرائيل OR توترات جيوسياسية
) OR (
    التضخم OR أسعار الفائدة OR البنك المركزي OR الاحتياطي الفيدرالي
) OR (
    نتنياهو OR ترامب OR بايدن OR بوتين OR اقتصاد الصين
) OR (
    عقوبات OR نزاع عسكري OR تصعيد الحرب OR أزمة عالمية
)
"""
ar_langugae="ar"
from_param="2026-04-13T00:00:00Z"
sort_by="publishedAt"
page_size=100
page=1

def fetch_newsapi_data(from_param,query=query_ar,to=None,language=ar_langugae,sort_by=sort_by,page=page,page_size=page_size):
    
    return newsapi.get_everything(q=query,
                                      from_param=from_param,
                                      to=to,
                                      language=language,
                                      sort_by=sort_by,
                                      page=page,
                                      page_size=page_size
                                      )

def format_newsapi_data(newsapi_response):
    articles = newsapi_response.get("articles", [])
    formatted_articles = []
    for article in articles:
        formatted_article = {
            "author": article.get("author"),
            "title": article.get("title"),
            "description": article.get("description"),
            "url": article.get("url"),
            "publishedAt": article.get("publishedAt"),
            "source": article.get("source", {}).get("name")
        }
        formatted_articles.append(formatted_article)
    return formatted_articles


def ingest_newsapi_data():
    current_time = get_current_utc_time().replace(":","-")
    last_timestamp = get_last_timestamp("newsapi-arabic")
    print(f"Fetching newsapi data at {current_time}...")
    ar_formatted_news = format_newsapi_data(fetch_newsapi_data(from_param=last_timestamp))
    ar_news_metadata= {
      "source": "news api",
      "type": "batch",
      "language": "ar",
      "timestamp": current_time}
    path=f"news/batch/arabic/{current_time}.json"

    return {
        "data": ar_formatted_news,
        "metadata": ar_news_metadata,
        "path": path,
        "timestamp": current_time,
        "length": len(ar_formatted_news)
    }

# if __name__ == "__main__":
# print(get_newsapi_data())