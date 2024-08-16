import pika
import sys, json, os, time


def handle_messages(ch, method, propr, body):
    data_message= json.loads(body)
    time= data_message['data'].split(' ')[1]
    time= time.replace(':', '_')
    file_name= "log_" + str(time[0:8]) + ".json"

    try:
        with open("./LogBackupB2/" + file_name, "wb") as output_jsonFile:
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

    if os.path.isdir("./LogBackupB2"):
        print("Folder 'LogBackupB2' already exists!")
    else:
        try:
            os.mkdir("./LogBackupB2")
        except Exception as e:
            print("Error: {e}")
            print("Error in creating 'LogBackupB2' folder")
            sys.exit(1)


    # Connection to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    # Create Channel
    channel= connection.channel()

    # Check the existence of the managerB_queue queue
    exchange_flag= True
    while exchange_flag:
        try:
            channel= connection.channel()
            channel.queue_declare("managerB_queue", True)
        except:
            print("The managerB_queue queue doesn't exist. Waiting...")
            time.sleep(2)
        else:
            exchange_flag= False
           

    try:
        print("[*] Waiting for logs. To exit press CTRL+C")
        channel.basic_consume(queue= 'managerB_queue', on_message_callback=handle_messages, auto_ack= True)
        channel.start_consuming()
    except KeyboardInterrupt as e:
        # Exit form basic_consume loop
        print(f"Interrupt Signal {e}")
        channel.close()
        sys.exit(0)