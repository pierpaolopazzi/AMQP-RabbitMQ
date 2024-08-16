import sys
import pika
import json, time


def handle_message(ch, method, properties, body):
    data_message= json.loads(body)

    print("-> Log n°%r received!" % data_message['logdata'])
    print("\t[x] Producer: " + data_message['producer'])
    print("\t[x] Datatime: " + data_message['data'])

if __name__ == '__main__':
    
    # Connection to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))

    # Create Channel
    channel= connection.channel()

    # Check the existence of the logs exchange
    exchange_flag= True
    while exchange_flag:
        try:
            channel= connection.channel()
            channel.exchange_declare(exchange='logs', exchange_type='fanout', passive=True)
        except:
            print("The logs exchange doesn't exist. Waiting...")
            time.sleep(2)
        else:
            exchange_flag= False

    private_queue= channel.queue_declare(queue='', exclusive=True)
    private_queue_name= private_queue.method.queue
    print(f"\n- Queue Name: {private_queue_name}")

    # Bind to the queue
    channel.queue_bind(exchange='logs', queue=private_queue_name)

    try:
        print("[*] Waiting for logs. To exit press CTRL+C")
        channel.basic_consume(queue=private_queue_name, on_message_callback=handle_message, auto_ack= True)
        channel.start_consuming()
    except KeyboardInterrupt as e:
        # Exit form basic_consume loop
        print(f"Interrupt Signal {e}")
        channel.close()
        connection.close()
        sys.exit(0)