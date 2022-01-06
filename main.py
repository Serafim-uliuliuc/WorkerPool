import json, requests, multiprocessing, pika
from bs4 import BeautifulSoup

urlBaza = 'https://www.alexa.com/topsites/countries'
urlSit = 'https://www.'
cale = 'E:\\info\\PP\\Date\\'

raspuns = requests.get(urlBaza)
parseaza = BeautifulSoup(raspuns.text, 'html.parser')
listaTaguri = parseaza.select('li a')

# conexiune = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
# coada = conexiune.channel()
# coada.queue_declare(queue='principal')

listaTari = []
for tag in listaTaguri:
    listaTari.append(tag.get('href').removeprefix('countries'))

for tara in listaTari:

    if tara == '/AX':
        continue  # serverul din insulele aland nu raspunde

    paginaTara = requests.get(urlBaza + tara)
    parseaza = BeautifulSoup(paginaTara.text, 'html.parser')

    listaSituri = parseaza.select('div > p > a')

    adresa = cale + tara[1:]   # print(tara)
    dictionar = {}

    for sit in listaSituri:
        dictionar['web'] = (urlSit + sit.get('href').removeprefix('/siteinfo/'))
        dictionar['local'] = adresa

        dePusInCoada=json.dumps(dictionar)
        # coada.basic_publish(exchange='',routing_key='principal',body=dePusInCoada)
        print(dePusInCoada)

# conexiune.close()