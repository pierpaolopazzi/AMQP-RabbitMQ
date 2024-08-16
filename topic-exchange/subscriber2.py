import pika
import sys, json, time



"""
    USAGE

    $ python subscriber2.py <topic1> <topic2> ...

    This script create a queue and for each <topic> argument received create a specific bind to the exchange defined 

    Special char:
    ".*" - can substitute for exactly one word
    '#' can substitute for zero or more words.

"""



def handle_message(ch, method, propr, body):
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

    # Check the existence of the topic_logs exchange
    exchange_flag= True
    while exchange_flag:
        try:
            channel= connection.channel()
            channel.exchange_declare(exchange='topic_logs', exchange_type='topic', passive=True)
        except:
            print("The topic_logs exchange doesn't exist. Waiting...")
            time.sleep(2)
        else:
            exchange_flag= False

    new_queue= channel.queue_declare(queue='', exclusive=True)
    queue_name= new_queue.method.queue

    topics= sys.argv[1:] # List of severity we are interested in 

    if not topics:
        sys.stderr.write("Usage: %s [binding_key]\n" % sys.argv[0])
        sys.exit(1)

    for topic in topics:
        channel.queue_bind(exchange='topic_logs', queue= queue_name, routing_key= topic)

    try:
        print("[*] Waiting for logs. To exit press CTRL+C")
        channel.basic_consume(queue=queue_name, on_message_callback=handle_message, auto_ack= True)
        channel.start_consuming()
    except KeyboardInterrupt as e:
        # Exit form basic_consume loop
        print(f"Interrupt Signal {e}")
        channel.close()
        connection.close()
        sys.exit(0)