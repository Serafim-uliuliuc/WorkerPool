import logging

import json
import pika
import requests

logging.basicConfig(filename='slave.log', filemode='w', format='%(asctime) - %(name): %(message)')
conexiune = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
coada = conexiune.channel()
coada.queue_declare(queue='principal')
coada.basic_qos(prefetch_count=1)


def run(ch, method, properties, body):
    date = json.load(body)
    fisier = open(date['local'] + date['web'].removeprefix('https://www.'), 'a')
    pagina = requests.get(date['web'], timeout=5)
    if pagina.ok:
        fisier.write(pagina.text)
        logging.info('Situl ' + date['web'] + ' pentru tara ' + date['local'][-2:]) + 'a fost descaract cu succes!'
    else:
        mesaj = 'Eroare la descarcare ' + date['web'] + ' pentru tara ' + date['local'][-2:]
        logging.info(mesaj)
    fisier.close()


for i in range(50):
    coada.basic_consume(queue='principal', on_message_callback=run)

conexiune.close()
