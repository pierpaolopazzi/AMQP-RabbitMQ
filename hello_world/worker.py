import pika, sys, os, time

def main():
    # Connessione al server RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()


    # Dichiarazione di una coda chiamata 'hello'
    # In caso di problemi la coda persiste 'durable=True'
    channel.queue_declare(queue='task_queue', durable = True)


    def callback(ch, method, properties, body):
        print(f" [x] Received {body.decode()}")
        time.sleep(body.count(b'.'))
        print(" [X] Done")
        ch.basic_ack(delivery_tag = method.delivery_tag)

    # Accept only 1 message at a time ---> Fair dispatch
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='task_queue', on_message_callback=callback)
    
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



