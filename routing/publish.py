from ntpath import join
import pika
import sys, json
from datetime import datetime


"""
USAGE

$ python publish.py <severity> <msg>

    - severity: routing/binding key
    - msg: body of the message

  The execution of this script will emit a specific msg to the defined direct type emitter. The emitter will transmit the received msg to all the queues with binding_key equals 
  to the specified area of the message (which correspond to the routing_key of the message itself) 

"""

if __name__ == '__main__':
    
    # Connection to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    # Create Channel
    channel= connection.channel()

    # Declare a direct type excange
    channel.exchange_declare(exchange= 'direct_logs', exchange_type='direct')

    dict_log= {
        "producer": "student",
        "data": str(datetime.now()),
        "severity": sys.argv[1] if len(sys.argv) > 1 else 'info',
        "message": ' '.join(sys.argv[2:]) or 'Hello World!'
    }

    data_log= json.dumps(dict_log)
    
    input("Press any button to send the messages...")
    channel.basic_publish(exchange="direct_logs", routing_key= dict_log['severity'], body= data_log)
    print(f"[x] Sent {dict_log['severity']} log!")
    
    channel.close()
    connection.close()