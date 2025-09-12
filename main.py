import requests
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
import urllib3
import time
import os
from crawl_url_pdf import download_pdf
from config import BASE_URL, BASE_DOMAIN, DATASET_DIR, CHECKPOINT_FILE, BATCH_SIZE, NUM_BATCHES
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
    

session = requests.Session()
response = session.get(BASE_URL, verify=False)
response.raise_for_status()
hidden_fields = get_hidden_fields(response.text)

all_links = []
pair_list = []
checkpoint_dict = {}
for i in range(1, NUM_BATCHES + 1):
    start = (i - 1) * BATCH_SIZE + 1
    end = i * BATCH_SIZE
    pair_list.append((start, end))

    checkpoint_dict[i] = 0
# pair_list[-1] = (9001, 9136) 

import json
if os.path.exists(CHECKPOINT_FILE):
    with open(CHECKPOINT_FILE, "r") as f:
        checkpoint_dict = json.load(f)
        print("Loaded checkpoint:", checkpoint_dict)
else:
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(checkpoint_dict, f)

for i in range(1, NUM_BATCHES + 1):
    if checkpoint_dict[str(i)] != 0:
        print(f"Batch {i} already completed up to page {checkpoint_dict[str(i)]}. Skipping.")
    else:
        print(f"Batch {i} not started yet.")
        
input_str = input(f"Chọn số tiếp tục batch: " )
input_str = int(input_str)

for page in range(pair_list[input_str - 1][0], pair_list[input_str - 1][1] + 1):
    if page == 1:
        # Lần đầu: bấm nút "Tìm kiếm"
        payload = {
            **hidden_fields,
            "ctl00$Content_home_Public$ctl00$txtKeyword": "Nhập tên vụ/việc hoặc số bản án, quyết định", 
            "ctl00$Content_home_Public$ctl00$Drop_Levels": "T",
            "ctl00$Content_home_Public$ctl00$Ra_Drop_Courts": "",
            "ctl00$Content_home_Public$ctl00$Rad_DATE_FROM": "",
            "ctl00$Content_home_Public$ctl00$cmd_search_banner": "Tìm kiếm"
        }
    else:
        payload = {
            **hidden_fields,
            "ctl00$Content_home_Public$ctl00$txtKeyword": "Nhập tên vụ/việc hoặc số bản án, quyết định",
            "ctl00$Content_home_Public$ctl00$Drop_Levels": "T",
            "ctl00$Content_home_Public$ctl00$Ra_Drop_Courts": "",
            "ctl00$Content_home_Public$ctl00$Rad_DATE_FROM": "",
            "ctl00$Content_home_Public$ctl00$DropPages": str(page),
            "__EVENTTARGET": "ctl00$Content_home_Public$ctl00$DropPages",
            "__EVENTARGUMENT": ""
        }


    headers = {
        "User-Agent": "Mozilla/5.0",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    try:
        response = session.post(BASE_URL, data=payload, headers=headers, verify=False)
        response.raise_for_status()
        
        print(f"Page {page} fetched successfully.")
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        links = [a["href"] for a in soup.find_all("a", href=True)]
        # print(links[0])
        for link in links: 
            text_split = link.split("/")
            if len(text_split) > 2 and text_split[2] == "chi-tiet-ban-an":
                full_link = BASE_DOMAIN + link
                all_links.append(full_link)
        
        hidden_fields = get_hidden_fields(response.text)
        # time.sleep(1)  
        
    except RequestException as e:
        print(f"Error on page {page}: {e}")
        break

set_all_links = set(all_links)
list_all_links = list(set_all_links)
print(f"Total unique links collected: {len(list_all_links)}")

if not os.path.exists(DATASET_DIR):
    os.makedirs(DATASET_DIR)

for i, link in enumerate(list_all_links):
    print(f"{i+1}: {link}")
    download_pdf(link, session)
    
checkpoint_dict[str(input_str)] = 1
with open(CHECKPOINT_FILE, "w") as f:
    json.dump(checkpoint_dict, f)