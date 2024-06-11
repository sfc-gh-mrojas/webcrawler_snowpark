from __future__ import annotations

import sys

from snowflake.snowpark import Session
from snowflake.snowpark.functions import udf
import requests
from bs4 import BeautifulSoup
import json
from urllib.parse import urlparse
import os
def fetch_data(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.text
        else:
            return None
    except:
        return ''

from urllib.parse import urljoin

def get_valid_url(base_url, href):
    return urljoin(base_url, href)

def get_filename_from_url(url):
    # Parse the URL to extract the path component
    parsed_url = urlparse(url)
    path = parsed_url.path
    
    # Extract the filename from the path
    filename = os.path.basename(path)
    return filename    

def scrape_notices(session, district_url):
    main_page = fetch_data(district_url)
    soup = BeautifulSoup(main_page, 'html.parser')
    list_of_news = [news_item.find_all("li") for news_item in soup.select('section.alist ul') if not 'class' in news_item.attrs]
    
    from functools import reduce
    import operator
    # flattened
    list_of_news = reduce(operator.concat, list_of_news)

    for news_item_soup in list_of_news:
        title = news_item_soup.find('a').text.strip()
        time  = news_item_soup.find('time')['datetime']
        url   = news_item_soup.find("a")["href"]
        expiration   = ''
        expiration_soup = news_item_soup.find('p',class_="standout")
        if expiration_soup:
            expiration = expiration_soup.text
        if not url.startswith('#'):
            article_data = fetch_data(url)
            article_soup = BeautifulSoup(article_data, 'html.parser')
            # Extract and save PDFs
            pdf_links = article_soup.find_all('a', href=True)  # Adjust selector as needed
            for link in pdf_links:
                if 'pdf' in link['href']:
                    pdf_url = link['href']
                    valid_url = get_valid_url(url, pdf_url)
                    pdf_response = requests.get(valid_url)
                    import io
                    pdf_filename = get_filename_from_url(valid_url)
                    session.file.put_stream(io.BytesIO(pdf_response.content), f"@mystage/{pdf_filename}", auto_compress=False)

    # Return or store metadata and PDF location
    return {
        "title": title,
        "publish_date": publish_date,
        "expiration_date": expiration_date,
        "body": body,
        "pdfs": [link['href'] for link in pdf_links if 'pdf' in link['href']]
    }



def webscrape_procedure(session: Session) -> str:
    # Example district URLs
    district_urls = [
        'https://www.swg.usace.army.mil/Media/Public-Notices/',
        'https://www.swf.usace.army.mil/Media/Public-Notices/'
    ]

    for url in district_urls:
        results = scrape_notices(session, url)
        # Process results, such as storing in Snowflake tables or further analysis
    return "Done"



# For local debugging
# Beware you may need to type-convert arguments if you add input parameters
if __name__ == "__main__":
    # Create a local Snowpark session
    with Session.builder.config("connection_name", "migrations1").getOrCreate() as session:
        session.use_database("snowpark_testdb")
        session.use_schema("public")
        print(hello_procedure(session, *sys.argv[1:]))  # type: ignore
