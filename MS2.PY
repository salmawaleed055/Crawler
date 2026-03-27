# Name: Salma Elmarakby
# ID: 900232658

import requests
from bs4 import BeautifulSoup
import time
import csv
import re

def clean_text(text):
    return re.sub(r'\s+', ' ', text).strip()

BASE_URL = "https://catalog.data.gov/dataset/?page="
MAX_PAGES = 100  

def crawl_data_gov():
    datasets = []
    
    for page_num in range(1, MAX_PAGES + 1):
        print(f"Crawling page {page_num}...")
        url = f"{BASE_URL}{page_num}"
        
        try:
            response = requests.get(url, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            items = soup.find_all('div', class_='dataset-content')
            
            for item in items:
                h3_elem = item.find('h3', class_='dataset-heading')
                if not h3_elem:
                    continue
                name = clean_text(h3_elem.text)
                a_elem = h3_elem.find('a')
                if not a_elem or not a_elem.get('href'):
                    continue
                link = "https://catalog.data.gov" + a_elem['href']
                
                detail_data = scrape_detail_page(link)
                
                dataset_entry = {
                    'name': name,
                    'url': link,
                    **detail_data
                }
                datasets.append(dataset_entry)
                
            time.sleep(1) 
            
        except Exception as e:
            print(f"Error on page {page_num}: {e}")
            
    return datasets

def scrape_detail_page(url):
    try:
        res = requests.get(url, timeout=10)
        ds_soup = BeautifulSoup(res.content, 'html.parser')
        
        org_name = "N/A"
        org_element = ds_soup.find('a', href=True, class_='organization-logo')
        if org_element and org_element.find('img'):
            org_name = org_element.find('img').get('alt', 'N/A')
        
        if org_name == "N/A":
            th_tags = ds_soup.find_all('th')
            for th in th_tags:
                label = th.text.strip().lower()
                td = th.find_next('td')
                if td and ('organization' in label or 'publisher' in label):
                    org_text = clean_text(td.text.strip())
                    if org_text and org_text != "N/A":
                        org_name = org_text
                        break
        
        format_labels = ds_soup.find_all('span', class_='format-label')
        formats = [clean_text(f.text) for f in format_labels]
        formats = [f for f in formats if f]
        
        access_level = "N/A"
        license_info = "N/A"
        maintainer = "N/A"
        
        th_tags = ds_soup.find_all('th')
        for th in th_tags:
            label = th.text.strip().lower()
            td = th.find_next('td')
            if td:
                value = clean_text(td.text.strip())
                if any(x in label for x in ['access level', 'access type']):
                    access_level = value
                elif any(x in label for x in ['license', 'licence']):
                    license_info = value
                elif 'maintainer' in label:
                    maintainer = value
        
        return {
            'organization': org_name,
            'formats': ", ".join(formats) if formats else "N/A",
            'access_level': access_level,
            'license': license_info,
            'maintainer': maintainer,
        }
    except Exception as e:
        print(f"Error scraping detail page {url}: {e}")
        return {}

if __name__ == "__main__":
    data = crawl_data_gov()
    
    if not data:
        print("No data collected.")
    else:
        keys = data[0].keys()
        output_file = 'crawled_datasets_improved.csv'
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            dict_writer = csv.DictWriter(f, fieldnames=keys)
            dict_writer.writeheader()
            dict_writer.writerows(data)
        print(f"Crawling complete. {len(data)} datasets saved to {output_file}")