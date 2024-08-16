import pika
import sys,json,os
import time

def handle_message(ch, method, propr, body):
    data_message= json.loads(body)
    time= data_message['data'].split(' ')[1]
    time= time.replace(':', '_')
    file_name= "log_" + str(time[0:8]) + ".json"

    try:
        with open("./LogBackupA2/" + file_name, "wb") as output_jsonFile:
            output_jsonFile.write(body)
            output_jsonFile.close()
    except Exception as e:
        print(f"Error: {e}")
        print(f"Error in saving file log_{time[0:8]}.json")

    else:
            print(f"-> [{time[0:8]}] Log successfully saved!")
            print("\t[x] Datatime: " + data_message['data'])
            print("\t[x] Topic: " + data_message['topic'])

if __name__ == '__main__':

    if os.path.isdir("./LogBackupA2"):
        print("Folder 'LogBackupA2' already exists!")
    else:
        try:
            os.mkdir("./LogBackupA2")
        except Exception as e:
            print("Error: {e}")
            print("Error in creating 'LogBackupA2' folder")
            sys.exit(1)


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
        channel.basic_consume(queue=managerA_queueName, on_message_callback=handle_message, auto_ack= True)
        channel.start_consuming()
    except KeyboardInterrupt as e:
        # Exit form basic_consume loop
        print(f"Interrupt Signal {e}")
        channel.close()
        connection.close()
        sys.exit(0)