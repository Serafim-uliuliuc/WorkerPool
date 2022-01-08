import logging
import os.path
import sys
import json
import pika
import requests

logger = logging.getLogger("Radu")
handler = logging.FileHandler(filename='slave.log', mode='a')
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s: %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

conexiune = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
coada = conexiune.channel()
coada.queue_declare(queue='principal')
coada.basic_qos(prefetch_count=1)

for i in range(50):
    body = coada.basic_get(queue='principal')
    date = json.loads(body[2].decode('utf-8'))

    if not os.path.exists(date['local']):
        os.makedirs(date['local'])

    try:
        fisier = open(date['local'] + '\\' + date['web'].removeprefix('https://www.').split('.')[0] + '.html', 'a', encoding='utf-8')
        pagina = requests.get(date['web'], timeout=5)

        if pagina.ok:
            fisier.write(pagina.text)
            logger.info('Situl ' + date['web'] + ' pentru tara ' + date['local'][-2:] + 'a fost descaract cu succes!' )
    except:
        mesaj = 'Eroare la descarcare ' + date['web'] + ' pentru tara ' + date['local'][-2:]
        logger.info(mesaj)
    fisier.close()
conexiune.close()
