import pika
import json
import time

if __name__ == '__main__':

    # Connection to RabbitMQ to remote 
    # Credentials
    #credentials = pika.PlainCredentials(username, password)
    # addressing
    #address_host = "ip_address"
    #port_host = "port"
    # virtual host
    #virtual_host = "/host_name"
    # create connection -> connection.channel()
    #connection = pika.BlockingConnection(pika.ConnectionParameters(address_host, port_host, virtual_host, credentials))
    #channel = connection.channel()
    
    # Connessione al server RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Create a Queue
    # queue_declare(queue, passive, durable, exclusive, auto_delete, arguments)
    # 'queue' = queue name
    # 'passive' = If 'True' checks if the queue exists without creating it
    # 'durable' = If 'True', queue survive at broker shutdown
    # 'exclusive = If 'True', queue is accessble only by the connection created
    # 'auto_delete = If 'True', queue is deleted when there are not consumer anymore
    # 'arguments' = Specific plus dictionary configuration/specification
    queue_name = "MyQueue"
    channel.queue_declare(queue_name, False, False, False, False, None)
    
    # Create the files to send, python dictionary and json, json.dumps()
    for i in range(0,10):
        # create 'data' dictionary
        data= {}
        data['author']= "send"
        data['n_file']= i
        # convert data{} dictionary in JSON format with 'json.dumps(data)'
        json_data= json.dumps(data)
        print(f"->Sending file n°  {i}")
        # RabbitMQ is used as broker messages
        # It sends the message to the queue
        # basic_publish(exchange, routing_key, body, properties)
        channel.basic_publish('', queue_name, json_data, None)
        
        time.sleep(1)
    
    # Handling channel and connection
    channel.close()
    connection.close()
