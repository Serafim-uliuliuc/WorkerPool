import json
import logging
import pika
import requests
import subprocess
import sys

from bs4 import BeautifulSoup

urlBaza = 'https://www.alexa.com/topsites/countries'
urlSit = 'https://www.'
cale = 'E:\\info\\PP\\Date\\'

logger = logging.getLogger("Petru")
handler = logging.FileHandler(filename='master.log', mode='a')
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s: %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

raspuns = requests.get(urlBaza)
parseaza = BeautifulSoup(raspuns.text, 'html.parser')
listaTaguri = parseaza.select('li a')

conexiune = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
coada = conexiune.channel()
coada.queue_declare(queue='principal')

listaTari = []
for tag in listaTaguri:
    listaTari.append(tag.get('href').removeprefix('countries'))

for tara in listaTari:

    try:
        paginaTara = requests.get(urlBaza + tara, timeout=5)

        if paginaTara.ok:
            parseaza = BeautifulSoup(paginaTara.text, 'html.parser')

            listaSituri = parseaza.select('div > p > a')

            adresa = cale + tara[1:]  # print(tara)
            dictionar = {}

            for sit in listaSituri:
                dictionar['web'] = (urlSit + sit.get('href').removeprefix('/siteinfo/'))
                dictionar['local'] = adresa

                dePusInCoada = json.dumps(dictionar)
                coada.basic_publish(exchange='', routing_key='principal', body=dePusInCoada.encode('utf-8'))

            logger.info('Am adaugat in coada siturile de la tara: ' + tara)

            break

    except:
        mesajEroare = 'Eroare la tara: ' + tara
        logger.info(mesajEroare)

conexiune.close()

procese = []
for proces in range(1):
    procese.append(subprocess.Popen([sys.executable, 'worker.py']))

for proces in procese:
    proces.wait()
