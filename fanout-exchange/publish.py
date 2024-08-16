import pika
import json
from datetime import datetime


def create_json_logfile(i):

    # Create dataLog python dictionary
    data= {
        "data": str(datetime.now()),
        "producer": 'student',
        "logdata": i,
    }

    # Transform python dictionary into json formatted string
    json_data= json.dumps(data)

    return json_data

    

if __name__ == '__main__':
    
    # Connessione al server RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Decleare exchange
    channel.exchange_declare(exchange='logs', exchange_type='fanout')

    input("Press any button to send the messages...")
    for i in range(0,10):

        message= create_json_logfile(i)
        print(f"Message: {message}")
        channel.basic_publish(exchange='logs', routing_key='', body= message)
        print("[x] Sent n° {i} message")

    # Close channel and connection
    channel.close()
    connection.close()