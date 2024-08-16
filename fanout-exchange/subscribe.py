import sys, os
import pika
import json, time

def handle_message(ch, method, properties, body):
    data_message= json.loads(body)

    file_name= "./LogFolder/logFile_" + str(data_message['logdata']) + ".json"

    print("-> Log n°%r received!" % data_message['logdata'])

    try:
        with open(file_name, "wb") as json_outfile:
            json_outfile.write(body)
            json_outfile.close()
    except Exception as e:
        print("Error in saving logFile_" + str(data_message['logdata']) + ".json!\nclosing..")
        json_outfile.close()

if __name__ == '__main__':

    # Create Folder
    # 1- Check if the folder already exists
    if os.path.isdir('./LogFolder'):
        print(f"Folder LogFolder already exists!")
    else:
        os.makedirs('./LogFolder')
        print("Folder 'LogFolder' correctfully created!")

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
            exchange_flag=False

    # New private queue
    private_queue= channel.queue_declare(queue='', exclusive=True)
    private_queue_name= private_queue.method.queue
    print(f"\n- Queue Name: {private_queue_name}")
    
    # Bind to the exchange
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