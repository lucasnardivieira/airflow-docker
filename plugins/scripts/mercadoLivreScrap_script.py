
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
        html = requests.get(page_url, headers=headers)
        status = html.status_code
        if(html.status_code != 200):
            logging.warning('Error: ' + str(status) + ' - ' + page_url)
            scrapped_product = {
                'product_link': page_url,
                'http_status': status,
                'store_name' : 'Mercado Livre',
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

    product_category = soup.find_all('a', {'class': 'andes-breadcrumb__link'})
    product_category = [re.sub(' +', ' ', category.text).strip() for category in product_category] if product_category else ''

    product_info_collum = soup.find_all('th', {'class': 'andes-table__header andes-table__header--left ui-pdp-specs__table__column ui-pdp-specs__table__column-title'})
    product_info_description = soup.find_all('span', {'class': 'andes-table__column--value'}) 

    product_info_collum = [re.sub(' +', ' ', info.text).strip() for info in product_info_collum] if product_info_collum else ''
    product_info_description = [re.sub(' +', ' ', info.text).strip() for info in product_info_description] if product_info_description else ''

    product_info = dict(zip(product_info_collum, product_info_description))

    product_price_tag = soup.find('div', {'class': 'ui-pdp-price__second-line'}) if soup.find('div', {'class': 'ui-pdp-price__second-line'}) else ''
    product_price = product_price_tag.find('span', {'class': 'andes-money-amount__fraction'}).text if soup.find('span', {'class': 'andes-money-amount__fraction'}) else ''
    product_cents = product_price_tag.find('span', {'class': 'andes-money-amount__cents andes-money-amount__cents--superscript-36'}).text if product_price_tag.find('span', {'class': 'andes-money-amount__cents andes-money-amount__cents--superscript-36'}) else None
    if(product_cents):
      product_price = product_price+'.'+product_cents
    
    product_full_price_tag = soup.find('s', {'class': 'andes-money-amount ui-pdp-price__part ui-pdp-price__original-value andes-money-amount--previous andes-money-amount--cents-superscript andes-money-amount--compact'}) if soup.find('s', {'class': 'andes-money-amount ui-pdp-price__part ui-pdp-price__original-value andes-money-amount--previous andes-money-amount--cents-superscript andes-money-amount--compact'}) else None
    if(product_full_price_tag):
      product_full_price = product_full_price_tag.find('span', {'class': 'andes-money-amount__fraction'}).text if soup.find('span', {'class': 'andes-money-amount__fraction'}) else ''
    else:
      product_full_price = product_price

    seller = soup.find('div', {'class': 'ui-pdp-seller__header__title'}).text if soup.find('div', {'class': 'ui-pdp-seller__header__title'}) else ''
    if(seller==''):
      seller = soup.find('a', {'class': 'ui-pdp-media__action ui-box-component__action'})['href'] if soup.find('a', {'class': 'ui-pdp-media__action ui-box-component__action'}) else ''
      seller = seller.split('.br/')
      seller=seller[1]

    scrapped_product = {
        'product_link': page_url,
        'http_status': status,
        'store_name' : 'Mercado Livre',
        'product_name': soup.find('h1', {'class': 'ui-pdp-title'}).text if soup.find('h1', {'class': 'ui-pdp-title'}) else '',
        'product_code': '',
        'product_brand': product_info['Marca'] if 'Marca' in product_info else '',
        'product_category': product_category,
        'product_price': product_price,
        'product_full_price': product_full_price,
        'review_score': soup.find('p', {'class': 'ui-pdp-reviews__rating__summary__average'}).text if soup.find('p', {'class': 'ui-pdp-reviews__rating__summary__average'}) else '',
        'review_quantity': re.sub('[^0-9]', '', soup.find('span', {'class': 'ui-pdp-review__amount'}).text) if soup.find('span', {'class': 'ui-pdp-review__amount'}) else '',
        'sold_by': seller,
        'seller_cnpj': '',
        'additional_info': product_info,
        'product_sold': re.sub('[^0-9]', '', soup.find('span', {'class': 'ui-pdp-subtitle'}).text) if soup.find('span', {'class': 'ui-pdp-subtitle'}) else ''
    }
    return scrapped_product

def convert_to_df(scrapped_product):
    df = pd.DataFrame(scrapped_product)
    print("Exportando arquivo")
    print(df.columns)
    df.to_csv(AIRFLOW_HOME + '/dags/data/mercadoLivre.csv', index=False, encoding='utf-8')