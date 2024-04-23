import urllib.parse
import concurrent.futures
from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
import time
import logging
from airflow.settings import AIRFLOW_HOME

def acessa_site(page_url): 
    headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36'}
    try:
        html = requests.get(page_url)
        status = html.status_code
        if(html.status_code != 200):
            logging.warning('Error: ' + str(status) + ' - ' + page_url)
            scrapped_product = {
                'product_link': page_url,
                'http_status': status,
                'store_name' : 'Magazine Luiza',
                'product_name': '',
                'product_code': '',
                'product_brand': '',
                'product_category': '',
                'review_score': '',
                'review_quantity': '',
                'sold_by': '',      
                'seller_cnpj': '',
                'additional_info': '',
                'product_sold':''
            }
            return scrapped_product
        else:
            html = html.content
        soup = BeautifulSoup(html, 'html.parser')
    except:
        return None

    code_brand = soup.find('small', {'class': 'header-product__code'}).text.replace('Código ', '').split(' ') if soup.find('small', {'class': 'header-product__code'}) else None
    product_code = code_brand[0] if code_brand else ''
    product_brand = code_brand[7] if code_brand else ''

    product_category = soup.find_all('a', {'class': 'breadcrumb__item'})
    product_category = [re.sub(' +', ' ', category.text).strip() for category in product_category] if product_category else ''

    product_info_main_collum = soup.find_all('td', {'class': 'description__information-left'})
    product_info_secondary_collum = soup.find_all('td', {'class': 'description__information-box-left'}) if soup.find_all('td', {'class': 'description__information-box-left'}) else None

    product_info_description = soup.find_all('td', {'class': 'description__information-box-right'}) 

    product_info_collum = []
    if(product_info_secondary_collum):
        if(product_info_secondary_collum[0].text.isspace()):
            product_info_collum = [re.sub(' +', ' ', info.text).strip() for info in product_info_main_collum] if product_info_main_collum else ''
        else:
            product_info_collum = [re.sub(' +', ' ', info.text).strip() for info in product_info_secondary_collum] if product_info_secondary_collum else ''

    product_info_description = [re.sub(' +', ' ', info.text).strip() for info in product_info_description] if product_info_description else ''
    product_info = dict(zip(product_info_collum, product_info_description))

    product_price = soup.find('span', {'class': 'price-template__text'}).text.replace(',','.') if soup.find('span', {'class': 'price-template__text'}) else ''
    product_full_price = soup.find('div', {'class': 'price-template__from'}).text.replace(',','.').replace('de R$ ', '') if soup.find('div', {'class': 'price-template__from'}) else product_price

    scrapped_product = {
        'product_link': page_url,
        'http_status': status,
        'store_name' : 'Magazine Luiza',
        'product_name': soup.find('h1', {'class': 'header-product__title'}).text if soup.find('h1', {'class': 'header-product__title'}) else '',
        'product_code': product_code,
        'product_brand': product_brand,
        'product_category': product_category,
        'product_price': product_price,
        'product_full_price': product_full_price,
        'review_score': soup.find('span', {'class': 'js-rating-value'}).text.replace(',','.') if soup.find('span', {'class': 'js-rating-value'}) else '',
        'review_quantity': soup.find('span', {'class': 'product-review__rating-total'}).text.replace(' avaliações','') if soup.find('span', {'class': 'product-review__rating-total'}) else 0,
        'sold_by': soup.find('button', {'class': 'seller-info-button js-seller-modal-button'})['data-seller-description'] if soup.find('button', {'class': 'seller-info-button js-seller-modal-button'}) else 'Magazine Luiza',
        'seller_cnpj': soup.find('button', {'class': 'seller-info-button js-seller-modal-button'})['data-seller-cnpj'] if soup.find('button', {'class': 'seller-info-button js-seller-modal-button'}) else '47960950000121',
        'additional_info': product_info,
        'product_sold':'',
    }
    return scrapped_product

def convert_to_df(scrapped_product):
    df = pd.DataFrame(scrapped_product)
    print("Exportando arquivo")
    print(df.columns)
    df.to_csv(AIRFLOW_HOME + '/dags/data/magalu.csv', index=False, encoding='utf-8')