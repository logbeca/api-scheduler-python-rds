from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from dotenv import load_dotenv
import json
import os
from pprint import  pprint
import schedule
import time
import csv
import psycopg2
from  psycopg2 import sql


load_dotenv()

url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest'


parameters = {
  'symbol': 'BTC',
  'convert':'BRL'
}
headers = {
  'Accepts': 'application/json',
  'X-CMC_PRO_API_KEY': os.getenv('CMC_API_KEY')}

session = Session()
session.headers.update(headers)

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

def criar_tabela():
    try:
        conn = psycopg2.connect(
        host = DB_HOST,
        database = DB_NAME,
        user = DB_USER,
        password = DB_PASS

        )
        cursor = conn.cursor()
        create_table_query = ''' 
        CREATE TABLE IF NOT EXISTS bitcoin_quotes (
            id SERIAL PRIMARY KEY,
            PRICE NUMERIC,
            volume_24h NUMERIC,
            market_cap NUMERIC,
            last_updated TIMESTAMP
        
        );
        
        '''
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        conn.close()
        print("Tabela criada ou já existente.")
    except Exception as e:
       print(f"Erro ao criar a tabela: {e}")

def salvar_no_rds(brl_quote):
    try:
        conn = psycopg2.connect(
        host = DB_HOST,
        database = DB_NAME,
        user = DB_USER,
        password = DB_PASS

        )
        cursor = conn.cursor()
        insert_query = sql.SQL(
           '''INSERT INTO bitcoin_quotes (price, volume_24h, market_cap, last_updated)
           VALUES (%s,%s,%s,%s)
           '''
        )
        cursor.execute(insert_query,(
            brl_quote['price'],
            brl_quote['volume_24h'],
            brl_quote['market_cap'],
            brl_quote['last_updated']
        ) )
        conn.commit()
        cursor.close()
        conn.close()
        print("Dados salvos com sucesso!")
    except Exception as e:
       print(f"Erro ao salvar dados no RDS: {e}")



def consultar_cotacao_bitcoin():
  try:  
    response =session.get(url=url , params =parameters)
    data = json.loads(response.text)

    if 'data' in data and 'BTC' in data['data']:
      bitcoin_data = data["data"]["BTC"]
      brl_quote = bitcoin_data["quote"]["BRL"]

      with open ('bitcoin.csv','a',newline ='') as csvfile:
          csv.reader(csvfile, delimiter=',', quotechar ='|')
          spamwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting =csv.QUOTE_MINIMAL)
          #spamwriter.writerow(['price', 'volume_24h', 'market_cap', 'last_updated'])
          spamwriter.writerow([
              brl_quote['price'],
              brl_quote['volume_24h'],
              brl_quote['market_cap'],
              brl_quote['last_updated']
          ])
      salvar_no_rds(brl_quote)
      #pprint(brl_quote)
      print(f"Última cotação do Bitcoin: ${brl_quote['price']} BRL")
      print(f"Volume 24: ${brl_quote['last_updated']} BRL")
      print(f"Market Cap: ${brl_quote['volume_24h']} BRL")
      print(f"última atualização: {brl_quote['market_cap']} BRL")
    else:
      print("Erro ao obter a cotação do Bitcoin:", data['status'].get('error_message','Erro desconhecido'))
  except (ConnectionError,Timeout, TooManyRedirects):
    print(f"Erro na requisição: {e}")

criar_tabela()

if __name__ == "__main__":
    schedule.every(15).seconds.do(consultar_cotacao_bitcoin)
    print("Iniciando o agendamento para consultar a API a cada 15 segundos...")
    while True:
        schedule.run_pending()
        time.sleep(1)