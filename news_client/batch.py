import os
from dotenv import load_dotenv
from newsapi import NewsApiClient
from last_timestamp.last_timestamp import get_current_utc_time, update_state,get_last_timestamp
from minio_client.minio import save_to_minio
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

def get_newsapi_data(from_param,query=query_ar,to=None,language=ar_langugae,sort_by=sort_by,page=page,page_size=page_size):
    
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

def save_newsapi_data(bucket_name):
    current_time = get_current_utc_time()
    last_timestamp = get_last_timestamp("newsapi-arabic")
    print(f"Fetching newsapi data at {current_time}...")
    ar_formatted_news = format_newsapi_data(get_newsapi_data(last_timestamp))
    ar_news_metadata= {
      "source": "news api",
      "type": "batch",
      "language": "ar",
      "timestamp": current_time}
    path=f"news/batch/arabic/{current_time}.json"
    save_to_minio(bucket_name, path, ar_formatted_news, metadata=ar_news_metadata)
    update_state("newsapi-arabic", current_time)
    