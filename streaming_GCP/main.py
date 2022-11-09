import requests
from google.cloud import pubsub_v1
import os
import json
import base64
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=r"C:\Users\sebastian.zapata23\OneDrive - Universidad EIA\2022-2s\datos tiempo real\ejemplotwitter\informacion\ensayo2-368100-043de50b4a19.json"
def publish(client, pubsub_topic, data_lines):
    """Publish to the given pubsub topic."""
    messages = []
    for line in data_lines:
        messages.append({'data': line})
    body = {'messages': messages}
    #str_body = json.dumps(body)
    str_body = json.dumps([line])
    
    data = base64.urlsafe_b64encode(bytearray(str_body, 'utf8'))    
    pubsub_message = base64.urlsafe_b64decode(data).decode('utf-8')
    
    future=client.publish(topic=pubsub_topic, data=data)
    print(future.result())

#programa principal para mandar mensajes
client = pubsub_v1.PublisherClient()
#el primero es el proyecto y el segundo el tema
pubsub_topic = client.topic_path("ensayo2-368100", "breakingbad")
url= "https://breakingbadapi.com/api/characters"
data=requests.get(url)
mensaje=[]
if data.status_code==200:
  data=data.json()
  diccionario={}
  for e in data:
    diccionario['name']=e['name']
    cont=0
    diccionario['occupation']=len(e['occupation'])
    diccionario['status']=e['status']
    diccionario['char_id']=e['char_id']
    cont=0
    diccionario['appearance']=len(e['appearance'])
    mensaje.append(diccionario)
    publish(client, pubsub_topic, mensaje)
    diccionario={}


