import pika
import sys, json, time


def handle_messages(ch, method, propr, body):
    data_message= json.loads(body)

    print("-> Log received!")
    print("\t[x] Producer: " + data_message['producer'])
    print("\t[x] Datatime: " + data_message['data'])
    print("\t[x] Topic: " + data_message['topic'])
    print("\t[x] Message: " + data_message['message'])



if __name__ == '__main__':
    # Connection to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    # Create Channel
    channel= connection.channel()
    
    # Check the existence of the managerA_exchange exchange
    exchange_flag= True
    while exchange_flag:
        try:
            channel= connection.channel()
            channel.exchange_declare(exchange='managerA_exchange', exchange_type='fanout', passive=True)
        except:
            print("The managerA_exchange exchange doesn't exist. Waiting...")
            time.sleep(2)
        else:
            exchange_flag= False
    
    managerA_queue= channel.queue_declare(queue='', exclusive=True)
    managerA_queueName= managerA_queue.method.queue

    channel.queue_bind(exchange="managerA_exchange", queue= managerA_queueName)

    try:
        print("[*] Waiting for logs. To exit press CTRL+C")
        channel.basic_consume(queue= managerA_queueName, on_message_callback=handle_messages, auto_ack= True)
        channel.start_consuming()
    except KeyboardInterrupt as e:
        # Exit form basic_consume loop
        print(f"Interrupt Signal {e}")
        channel.close()
        sys.exit(0)