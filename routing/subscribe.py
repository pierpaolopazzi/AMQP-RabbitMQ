import queue
import pika
import sys, json, time



"""
    USAGE

    $ python subscribe.py <severity1> <severity2> ...

    This script create a queue and for each <severity> argument received create a specific bind to the exchange defined 

"""



def handle_message(ch, method, propr, body):
    data_message= json.loads(body)

    print("-> Log received!")
    print("\t[x] Producer: " + data_message.get('producer', 'Unkown'))
    print("\t[x] Datatime: " + data_message.get('data', 'Unkown'))
    print("\t[x] Severity: " + data_message.get('severity','Unkown'))
    print("\t[x] Message: " + data_message.get('message','Unkown'))

if __name__ == '__main__':

    # Connection to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    # Create Channel
    channel= connection.channel()

    # Check the existence of the direct_logs exchange
    exchange_flag= True
    while exchange_flag:
        try:
            channel= connection.channel()
            channel.exchange_declare(exchange='direct_logs', exchange_type='direct', passive=True)
        except:
            print("The direct_logs exchange doesn't exist. Waiting...")
            time.sleep(2)
        else:
            exchange_flag= False

    new_queue= channel.queue_declare(queue='', exclusive=True)
    queue_name= new_queue.method.queue

    severities= sys.argv[1:] # List of severity we are interested in 

    if not severities:
        sys.stderr.write("Usage: %s [info] [warning] [error]\n" % sys.argv[0])
        sys.exit(1)

    for severity in severities:
        channel.queue_bind(exchange='direct_logs', queue= queue_name, routing_key= severity)

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