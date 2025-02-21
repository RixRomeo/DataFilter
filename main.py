import paho.mqtt.client as mqtt
from pymongo import MongoClient
from pprint import pprint
import json


# Carica il File JSON
with open("config.json", "r") as file:
    config = json.load(file)
    

# Configurazione MongoDB
mongo_client = MongoClient(config["mongodb"]["host"], config["mongodb"]["port"])
db = mongo_client[config["mongodb"]["database"]]
collection = db[config["mongodb"]["collection"]]



# Funzione di callback per i messaggi ricevuti
def on_message(client, userdata, msg):
    pprint(f"Dati ricevuti: {msg.payload.decode()}\n\n")
    dati = json.loads(msg.payload.decode())
    collection.insert_one(dati)




if(config["mqtt"]["id"] != " "):
    client = mqtt.Client(client_id=config["mqtt"]["id"]) 
else:
    client = mqtt.Client()



if(config["mqtt"]["ca_cert"] and config["mqtt"]["ca_cert"] and config["mqtt"]["ca_cert"] != " "):
    client.tls_set(
    ca_certs=config["mqtt"]["ca_cert"],
    certfile=config["mqtt"]["certfile"],
    keyfile=config["mqtt"]["keyfile"]
    )




# Callback di connessione e iscrizione ai vari topic
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connesso con successo")
        for topic in config["mqtt"]["topic"]:
            client.subscribe(topic)
    else:
        print(f"Errore di connessione: Codice {rc}")



client.on_connect = on_connect
client.on_message = on_message


# Connessione al broker MQTT
client.connect(config["mqtt"]["broker_address"], config["mqtt"]["port"], 60)


#loop
client.loop_forever()
