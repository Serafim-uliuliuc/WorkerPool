import json
import logging
import os.path

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

def verif(path):
    try:
        if not os.path.exists(path):
            os.makedirs(path)
    except:
        print()

for i in range(100):
    body = coada.basic_get(queue='principal')
    date = json.loads(body[2].decode('utf-8'))

    verif(date['local'])

    try:
        pagina = requests.get(date['web'], timeout=5)

        if pagina.ok:
            fisier = open(date['local'] + '\\' + date['web'].removeprefix('https://www.').split('.')[0] + '.html', 'a',
                          encoding='utf-8')
            fisier.write(pagina.text)
            logger.info('Situl ' + date['web'] + ' pentru tara ' + date['local'][-2:] + ' a fost descaract cu succes!')
            fisier.close()
    except:
        mesaj = 'Eroare la descarcare ' + date['web'] + ' pentru tara ' + date['local'][-2:]
        logger.info(mesaj)

conexiune.close()
