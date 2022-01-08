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
logging.basicConfig(filename='master.log', filemode='w', format='%(asctime) - %(name): %(message)')

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
            coada.basic_publish(exchange='', routing_key='principal', body=dePusInCoada)

        logging.info('Am adaugat in coada siturile de la tara: ' + tara)
    else:
        mesajEroare = 'Eroare la tara: ' + tara
        logging.info(mesajEroare)

procese = []
for proces in range(len(listaTari)):
    procese.append(subprocess.Popen([sys.executable, 'worker.py']))

for proces in procese:
    proces.wait()

conexiune.close()
