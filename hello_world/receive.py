import pika, sys, os

def main():
    # Connessione al server RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()


    # Dichiarazione di una coda chiamata 'hello'
    channel.queue_declare(queue='hello')


    def callback(ch, method, properties, body):
    	print(f" [x] Received {body}")

    channel.basic_consume(queue='hello',
                          auto_ack=True,
                          on_message_callback=callback)
    
    # Waiting loop for consuming data
    print('[*] Waiting for messages. To exit press CTRL+C')
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



