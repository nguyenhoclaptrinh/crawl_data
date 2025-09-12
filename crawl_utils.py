import requests
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
import urllib3
from config import BASE_URL

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_hidden_fields(response):
    try:
        soup = BeautifulSoup(response, "html.parser")
        hidden = {}
        for input_tag in soup.find_all("input", type="hidden"):
            if input_tag.get("name") and input_tag.get("value"):
                hidden[input_tag["name"]] = input_tag["value"]
        return hidden
    except RequestException as e:
        print(f"Error fetching hidden fields: {e}")
        return {}

def initialize_session():
    session = requests.Session()
    response = session.get(BASE_URL, verify=False)
    response.raise_for_status()
    hidden_fields = get_hidden_fields(response.text)
    return session, hidden_fields

def create_payload(hidden_fields, page, drop_levels, SEARCH_KEYWORD):
    if page == 1:
        return {
            **hidden_fields,
            "ctl00$Content_home_Public$ctl00$txtKeyword": SEARCH_KEYWORD,
            "ctl00$Content_home_Public$ctl00$Drop_Levels": drop_levels,
            "ctl00$Content_home_Public$ctl00$Ra_Drop_Courts": "",
            "ctl00$Content_home_Public$ctl00$Rad_DATE_FROM": "",
            "ctl00$Content_home_Public$ctl00$cmd_search_banner": "Tìm kiếm"
        }
    else:
        return {
            **hidden_fields,
            "ctl00$Content_home_Public$ctl00$txtKeyword": SEARCH_KEYWORD,
            "ctl00$Content_home_Public$ctl00$Drop_Levels": drop_levels,
            "ctl00$Content_home_Public$ctl00$Ra_Drop_Courts": "",
            "ctl00$Content_home_Public$ctl00$Rad_DATE_FROM": "",
            "ctl00$Content_home_Public$ctl00$DropPages": str(page),
            "__EVENTTARGET": "ctl00$Content_home_Public$ctl00$DropPages",
            "__EVENTARGUMENT": ""
        }

def crawl_page(session, page, hidden_fields, drop_levels, BASE_DOMAIN, SEARCH_KEYWORD):
    payload = create_payload(hidden_fields, page, drop_levels, SEARCH_KEYWORD)
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    try:
        response = session.post(BASE_URL, data=payload, headers=headers, verify=False)
        response.raise_for_status()
        print(f"📄 Page {page} (DROP_LEVELS={drop_levels}) fetched successfully.")
        soup = BeautifulSoup(response.text, "html.parser")
        links = [a["href"] for a in soup.find_all("a", href=True)]
        page_links = []
        for link in links:
            text_split = link.split("/")
            if len(text_split) > 2 and text_split[2] == "chi-tiet-ban-an":
                full_link = BASE_DOMAIN + link
                page_links.append(full_link)
        new_hidden_fields = get_hidden_fields(response.text)
        print(f"✅ Found {len(page_links)} detail links on page {page}")
        return page_links, new_hidden_fields, True
    except RequestException as e:
        print(f"❌ Error on page {page}: {e}")
        return [], hidden_fields, False
