from kafka import KafkaConsumer
from time import sleep
from json import dumps,loads
import json
import datetime
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

def consumeData(topic):
    connect_str ="DefaultEndpointsProtocol=https;AccountName=storageaccounthajar;AccountKey=PG/+WC7my8slgqJVzTt3UhYMt5/tFSxzUoD/q2fU/4QAVUrRgM3SRw9BBO59NcCp99ORQ7FGMFwO+AStMjLs3A==;EndpointSuffix=core.windows.net"
    # Create the BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    # Create a unique name for the container
    container_name = "cryptorecords"
    # Create the container
    container_client = blob_service_client.create_container(container_name)
    consumer = KafkaConsumer(
    "my-topic",
     bootstrap_servers=['localhost:9092'], #add your IP here
    value_deserializer=lambda x: loads(x.decode('utf-8')))

    for msg in consumer:
        try:
            
            #to hold tranformed data
            transform_data = {}
            cryptoRecord=msg.value

            if cryptoRecord is not None:
                #print("User record {}: crypto_Data: {}\n".format(msg.key(), cryptoRecord))
                transform_data['SYSTEM_INSERTED_TIMESTAMP'] = datetime.datetime.fromtimestamp(cryptoRecord['SYSTEM_INSERTED_TIMESTAMP'] / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
                transform_data['RANK'] = int(cryptoRecord['RANK'])
                transform_data['NAME'] = cryptoRecord['NAME']
                transform_data['SYMBOL'] = cryptoRecord['SYMBOL']
                transform_data['PRICE'] = float((cryptoRecord['PRICE'].replace('$', '').replace(',', '').replace(' ', '')))
                transform_data['PERCENT_CHANGE_24H'] = float(cryptoRecord['PERCENT_CHANGE_24H'].replace('%', '').replace(',', '').replace(' ', ''))
                if cryptoRecord['VOLUME_24H'].replace('$', '').replace('B', 'E9').replace('M', 'E6').replace(',', '').replace(' ', '')=='N/A':
                    transform_data['VOLUME_24H']=None
                else:
                    transform_data['VOLUME_24H'] = float(cryptoRecord['VOLUME_24H'].replace('$', '').replace('B', 'E9').replace('M', 'E6').replace(',', '').replace(' ', ''))
                transform_data['MARKET_CAP'] = float(cryptoRecord['MARKET_CAP'].replace('$', '').replace('B', 'E9').replace('M', 'E6').replace(',', '').replace(' ', ''))
                transform_data['CURRENCY'] = 'USD'
                
                json_str = json.dumps(transform_data)
                #print(json_str)
                file_name = "real_time_data/top_100_crypto_data_" + str(cryptoRecord.record['SYSTEM_INSERTED_TIMESTAMP']) + '_' + str(cryptoRecord.record['RANK']) + '.json'
                # Create a blob client using the local file name as the name for the blob
                blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)

                print("\nUploading to Azure Storage as blob:\n\t" + file_name)

                # Upload the created file
                with open(file=file_name, mode="rb") as data:
                    blob_client.upload_blob(data)
                print("file_uploaded: ",file_name)
        except KeyboardInterrupt:
            break

    consumer.close()    




def main():
        
    topic = "my-topic"
    print("starting consumer: ",topic)
    consumeData(topic)
    

main()	
	