from bs4 import BeautifulSoup
import requests
import time
import datetime
import pandas as pd
import json
from json import dumps
from time import sleep
from kafka import KafkaProducer

def scrape_data(url):
    allRecordsCombined = []
    for page in range(1,3):    
        response = requests.get(url+str(page))
        current_timestamp = datetime.datetime.now()
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find the table containing the top 100 cryptocurrencies
        treeTag = soup.find_all('tr')
        for tree in treeTag[1:]:
            rank = tree.find('td',{'class': 'css-w6jew4'}).get_text()
            name = tree.find('p',{'class': 'chakra-text css-rkws3'}).get_text()
            symbol = tree.find('span',{'class': 'css-1jj7b1a'}).get_text()
            if tree.find_all('td',{'class':'css-15lyn3l'})[1]==None:
                market_cap=None
            else: 
                market_cap = tree.find_all('td',{'class':'css-15lyn3l'})[1].get_text()
            change_24h = ""
            price_arr = str(tree.find('div',{'class':'css-16q9pr7'}).get_text())
            if('-' in price_arr):
                price_arr = price_arr.split('-')
                change_24h = '-'+price_arr[1]
            else:
                price_arr = price_arr.split('+')
                change_24h = '+'+price_arr[1]
            price = price_arr[0]
            if tree.find_all('td',{'class':'css-15lyn3l'})[0]==None: 
                volume_24=None
            else:
                volume_24 = tree.find_all('td',{'class':'css-15lyn3l'})[0].get_text()
            allRecordsCombined.append([current_timestamp, rank, name, symbol, price, change_24h, volume_24, market_cap])
    columns = ['SYSTEM_INSERTED_TIMESTAMP', 'RANK','NAME', 'SYMBOL', 'PRICE', 'PERCENT_CHANGE_24H','VOLUME_24H', 'MARKET_CAP']
    data= pd.DataFrame(columns=columns, data=allRecordsCombined)
    json_export = data.to_json(orient='records')
    return json.loads(json_export)


def produceData(topic):
    while True:
        url = 'https://crypto.com/price?page='
        json_file = scrape_data(url)
        #print(json_file)

        print("Producing user records to topic {}. ^C to exit.".format(topic))
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'], #change ip here
                                value_serializer=lambda x: 
                                dumps(x).encode('utf-8'))
        try:
            for temp_rec in json_file:
                producer.send(topic, value=temp_rec)
                print("sending record {} successfully !".format(temp_rec))
                
        except ValueError:
            print("Invalid input, discarding record...")
            pass

        #5 sec delay
        sleep(5)

        print("\nFlushing records...")
        producer.flush()
    

def main():
        
    topic = "my-topic"
    print("starting producer: ",topic)
    produceData(topic)  
	
	
main()	