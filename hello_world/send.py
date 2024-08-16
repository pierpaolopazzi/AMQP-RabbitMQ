#!/usr/bin/env python
import pika

def main():
    # Connessione al server RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Dichiarazione di una coda chiamata 'hello'
    channel.queue_declare(queue='hello')

    # Invio di un messaggio 'Hello World!' alla coda 'hello'
    channel.basic_publish(exchange='',
                          routing_key='hello',
                          body='Hello World!')
    print(" [x] Sent 'Hello World!'")

    # Chiusura della connessione
    connection.close()

if __name__ == '__main__':
    main()
