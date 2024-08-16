import pika, sys



# Connessione al server RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Dichiarazione di una coda chiamata 'hello'
channel.queue_declare(queue='task_queue', durable=True)

message = ' '.join(sys.argv[1:]) or "Hello World!"
channel.basic_publish(exchange='',
                      routing_key='task_queue',
                      body=message,
                      properties=pika.BasicProperties(
                        delivery_mode = pika.DeliveryMode.Persistent
                      ))
print(f" [x] Sent {message}")

# Chiusura della connessione
connection.close()