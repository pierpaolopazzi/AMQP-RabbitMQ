import pika
import sys, json
from datetime import datetime

"""
USAGE

$ python publisher2.py <topic> <msg>

    - topic: routing/binding key - <severity>.<subject>   # topic must be a list of words delimited by dots
    - msg: body of the message

  topic example: art.music, humanities.history, ...

  The execution of this script will emit a specific msg to the defined direct type emitter. The emitter will transmit the received msg to all the queues with binding_key equals 
  to the specified topic (which correspond to the routing_key of the message itself) 

"""

if __name__ == '__main__':
    
    # Connection to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    # Create Channel
    channel= connection.channel()

    # Declere a direct type excange
    channel.exchange_declare(exchange= 'topic_logs', exchange_type='topic')

    dict_log= {
        "producer": "student",
        "data": str(datetime.now()),
        "topic": sys.argv[1] if len(sys.argv) > 2 else 'info',
        "message": ' '.join(sys.argv[2:]) or 'Hello World!'
    }

    data_log= json.dumps(dict_log)
    input("Press any button to send the messages...")
    channel.basic_publish(exchange="topic_logs", routing_key= dict_log['topic'], body= data_log)
    print(f"[x] Sent {dict_log['topic']} log!")
    
    channel.close()
    connection.close()
