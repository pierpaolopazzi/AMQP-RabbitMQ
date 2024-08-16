import pika
import sys, time


def handle_messages(ch, method, propr, body):
    try:
        channel.basic_publish(exchange='', routing_key='managerB_queue', body= body)
    except Exception as e:
        print(f"Error: {e}")
        print("Error in forwording the received msg")
    else:
        print("Message successfully forwarded!")

if __name__ == '__main__':
    # Connection to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    # Create Channel
    channel= connection.channel()

    # Check the existence of the file_generator exchange
    exchange_flag= True
    while exchange_flag:
        try:
            channel= connection.channel()
            channel.exchange_declare(exchange='file_generator', exchange_type='fanout', passive=True)
        except:
            print("The file_generator exchange doesn't exist. Waiting...")
            time.sleep(2)
        else:
            exchange_flag= False
    
    # Create the queue for file_generator exchange
    fileGenerator_queue= channel.queue_declare(queue='', exclusive=True)
    generator_queueName= fileGenerator_queue.method.queue

    channel.queue_bind(exchange="file_generator", queue= generator_queueName)

    # Create the queue to publish the received msgs
    channel.queue_declare("managerB_queue", False, False, False, False, None)

    try:
        print("[*] Waiting for logs. To exit press CTRL+C")
        channel.basic_consume(queue= generator_queueName, on_message_callback=handle_messages, auto_ack= True)
        channel.start_consuming()
    except KeyboardInterrupt as e:
        # Exit form basic_consume loop
        print(f"Interrupt Signal {e}")
        channel.close()
        sys.exit(0)