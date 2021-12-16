import json, requests, multiprocessing, rq
from bs4 import BeautifulSoup

urlBaza = 'https://www.alexa.com/topsites/countries'
urlSit = 'https://www.'

raspuns = requests.get(urlBaza)
parseaza = BeautifulSoup(raspuns.text, 'html.parser')
listaTaguri = parseaza.select('li a')

listaTari = []
for tag in listaTaguri:
    listaTari.append(tag.get('href').removeprefix('countries'))

for tara in listaTari:

    if tara == '/AX':
        continue #serverul din insulele aland nu raspunde

    paginaTara = requests.get(urlBaza+tara)
    parseaza = BeautifulSoup(paginaTara.text, 'html.parser')

    listaSituri = parseaza.select('div > p > a')
    print(tara)
    for sit in listaSituri:
        print(urlSit+sit.get('href').removeprefix('/siteinfo/'))
