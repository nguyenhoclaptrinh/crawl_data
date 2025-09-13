import requests
from requests.exceptions import RequestException, ChunkedEncodingError
from bs4 import BeautifulSoup
import urllib3
import time
from config import BASE_DOMAIN, DATASET_DIR
from pdf_to_text import process_file

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def download_pdf(url, session, drop_levels=None, page_num=None, pdf_index=None):
    try:
        response = session.get(url, verify=False)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        pdf_links = [a for a in soup.find_all("a", href=True) if a["href"].lower().endswith(".pdf")]

        if pdf_links:
            for pdf_link in pdf_links:
                pdf_url = pdf_link["href"]
                if not pdf_url.startswith("http"):
                    pdf_url = BASE_DOMAIN + pdf_url
            
            pdf_response = session.get(pdf_url, verify=False)
            pdf_response.raise_for_status()
            
            # Đặt tên file: Droplevel + số page + index.pdf
            if drop_levels is not None and page_num is not None and pdf_index is not None:
                filename = f"{drop_levels}_{page_num}_{pdf_index}.pdf"
            else:
                # fallback cũ
                url_parts = url.rstrip('/').split('/')
                if len(url_parts) >= 2:
                    filename = f"{url_parts[-2]}.pdf"
                else:
                    filename = pdf_url.split("/")[-1]
            with open(f"{DATASET_DIR}/{filename}", "wb") as f:
                f.write(pdf_response.content)
            print(f"Downloaded: {filename}")
        else:
            print("No PDF link found on the page.")
    except RequestException as e:
        print(f"Error downloading PDF from {url}: {e}")
        