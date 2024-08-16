import pika, sys



# Connessione al server RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Dichiarazione di una coda chiamata 'hello'
channel.exchange_declare(exchange='logs', exchange_type='fanout')

# Pubblico il messaggio sull'exchange specifico
message = ' '.join(sys.argv[1:]) or "Hello World!"
channel.basic_publish(exchange='logs', routing_key='', body=message)
print(f" [x] Sent {message}")

# Chiusura della connessione
connection.close()