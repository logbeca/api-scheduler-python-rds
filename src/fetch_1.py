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
      
      #pprint(brl_quote)
      print(f"Última cotação do Bitcoin: ${brl_quote['price']} BRL")
      print(f"Volume 24: ${brl_quote['last_updated']} BRL")
      print(f"Market Cap: ${brl_quote['volume_24h']} BRL")
      print(f"última atualização: {brl_quote['market_cap']} BRL")
    else:
      print("Erro ao obter a cotação do Bitcoin:", data['status'].get('error_message','Erro desconhecido'))
  except (ConnectionError,Timeout, TooManyRedirects):
    print(f"Erro na requisição: {e}")


schedule.every(15).seconds.do(consultar_cotacao_bitcoin)
print("Iniciando o agendamento para consultar a API a cada 15 segundos...")
while True:
  schedule.run_pending()
  time.sleep(1)