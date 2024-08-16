import pika, sys, os, time

def main():
    # Connessione al server RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Dichiaro l'exchange uguale al publisher
    channel.exchange_declare(exchange='logs', exchange_type='fanout')

    # Dichiarazione di una coda vuota che viene generata
    # automaticamente con id che viene inserito in 'result'
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    # binding tra exchange e coda
    channel.queue_bind(exchange='logs', queue=queue_name)

    print('[*] Waiting for messages. To exit press CTRL+C')
    
    def callback(ch, method, properties, body):
        print(f" [x] Received {body}")

    # Consume message
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    
    # Waiting loop for consuming data
 
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)



