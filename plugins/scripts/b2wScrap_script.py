from bs4 import BeautifulSoup
import requests
import pandas as pd
from airflow.settings import AIRFLOW_HOME

def acessa_site(url):
    headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36'}
    try:
        html = requests.get(url, headers = headers)
        status = html.status_code
        if(html.status_code != 200):
            scrapped_product = {
                'product_link': url,
                'http_status': status,
                'store_name' : '',
                'product_name': '',
                'product_code': '',
                'produtct_brand': '',
                'prodduct_category': '',
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
    #Product Info
    infos = []
    infos = soup.find_all('tr', attrs={'class':'spec-drawer__View-sc-jcvy3q-4 eHkstE'})
    conteudo = []
    first_column = []
    second_column = []
    for info in infos:
        conteudo = info.find_all('td', attrs={'class':'spec-drawer__Text-sc-jcvy3q-5 fMwSYd'})
        
        first_column.append(conteudo[0].text)
        second_column.append(conteudo[1].text)
    product_info = dict(zip(first_column, second_column))
    #Product Name
    if soup.find('h1', attrs={'class':'product-title__Title-sc-1hlrxcw-0 jyetLr'}):
      product_name = soup.find('h1', attrs={'class':'product-title__Title-sc-1hlrxcw-0 jyetLr'}).text
    else:
      product_name = '*'
    #Product Code
    product_code = product_info.get('Código')
    #Product Brand
    product_brand = product_info.get('Marca')
    #Box content
    box_content = product_info.get('Conteúdo da Embalagem')
    #Review Quantity
    if soup.find('span', attrs={'class':'src__Count-sc-gi2cko-1 dEfRHV'}):
        review_quantity = soup.find('span', attrs={'class':'src__Count-sc-gi2cko-1 dEfRHV'}).text
    else:
        review_quantity = '*'
    #Vendido por
    if soup.find('span', attrs={'class':'sold-and-delivered__Link-sc-17c758d-0 fWFsnH'}):
        sold_by = soup.find('span', attrs={'class':'sold-and-delivered__Link-sc-17c758d-0 fWFsnH'}).text
    else:
        sold_by = 'Americanas'
    #Categoria
    product_category = []
    categorias = []
    categorias = soup.find_all('li', attrs={'class':'src__ListItem-sc-11934zu-2 kKrvKo'})
    for cat in categorias:
        classe = cat.find('a', attrs={'class':'src__Link-sc-11934zu-4 ezeDVK'}).text
        product_category.append(classe)
    #Review
    if soup.find('span', attrs={'class':'header__RatingValue-sc-ibr017-9 jnVXpb'}):
      review_score = soup.find('span', attrs={'class':'header__RatingValue-sc-ibr017-9 jnVXpb'}).text
    else:
      review_score = 'Sem avaliações'
     
    scrapped_product = {
        'product_link': url,
        'http_status': status,
        'store_name' : 'B2W',
        'product_name': product_name,
        'product_code': product_code,
        'product_brand': product_brand,
        'product_category': product_category,
        'box_content': box_content,
        'review_score': review_score,
        'review_quantity': review_quantity,
        'sold_by': sold_by,
        'product_info': product_info
    }
    return scrapped_product

def convert_to_df(scrapped_product):
    df = pd.DataFrame(scrapped_product)
    print("Exportando arquivo")
    print(df.columns)
    df.to_csv(AIRFLOW_HOME + '/dags/data/b2w.csv', index=False, encoding='utf-8')
