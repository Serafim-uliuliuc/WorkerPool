import pika, json, requests

conexiune = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
coada = conexiune.channel()
coada.queue_declare(queue='principal')

def run(ch, method, properties, body):
    date = json.load(body)
    fisier = open(date['local']+date['web'].removeprefix('https://www.'), 'a')
    pagina = requests.get(date['web'])
    fisier.write(pagina.text)
    fisier.close()

for i in range(50):


conexiune.close()