import pika
import sys, json
from datetime import datetime


if __name__ == '__main__':

    # Connection to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))

    # Create Channel
    channel= connection.channel()

    # Declare fanout exchange
    channel.exchange_declare(exchange= "file_generator", exchange_type="fanout")


    topic= input("Insert the topic of the message: ")
    message= input("Insert the body of the message: ")
    dict_log= {
        "producer": "student",
        "data": str(datetime.now()),
        "topic": topic,
        "message": message
    }
    
    data_log= json.dumps(dict_log)
    channel.basic_publish(exchange="file_generator", routing_key=topic, body= data_log)

    print(f"[x] Sent {dict_log['topic']} log!")
    
    channel.close()
    connection.close()
