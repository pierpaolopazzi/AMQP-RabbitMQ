import pika, sys, os, json
from pika import exceptions

# Consume the message, saves it in the proper folder and after that send the ack to RabbitMQ
# on_message_callback(ch, method, properties, body)
# 'ch' = Is the channel where the message is received
# 'method' = delivery information
# 'properties' = message properties as priority, timestamp, header...
# 'body' = message body the consumer has to compute
def message_handler(ch, method, properties, body):
    # Message Handling
    # Convert JSON message to Python dictionary
    data=json.loads(body)
    print(f"[x] Received {body}")
    file_name= "./worker/json_data" + str(data['n_file']) + ".json"
    
    # Save the file
    try:
        with open(file_name, 'wb') as json_outfile:
            json_outfile.write(body)
            json_outfile.close()
    except Exception as e:
        print("Error in saving json file n " + str(data['n_file']) + "\nclosing..")
        json_outfile.close()
    else:    
        # Sending the ack to RabbitMQ
        ch.basic_ack(delivery_tag= method.delivery_tag)
        print("[*]File n° " + str(data['n_file']) + " correctly saved!") 


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

    channel = connection.channel()
    queue_name = "MyQueue"
    
    channel.queue_declare(queue_name, False)
    # Check if the queue where we want to read exists or not
    #try:
    #    channel.queue_declare(queue_name, True)
    #except exceptions.ChannelClosedByBroker as e:
    #    print(f"Errore: {e}")
    #    print(f"Coda {queue_name} non presente!")
     
    # Setting the Quality of Services for a more fair dispatching of messages
    channel.basic_qos(prefetch_count= 1)
    
    # Creating the destination saving folder
    # 1- Check if the folder already exists
    savingDir_path= './worker'
    if os.path.isdir(savingDir_path):
        print(f"Folder 'worker' already exists!")
    else:
        os.makedirs(savingDir_path)
        print("Folder 'worker' correctfully created!")
    
    # Read the messages and waits for them indefinitely...
    try:
        print("[*] Waiting for messages. To exit press CTRL+C")
        
        # Disabling the auto_ack
        # 'auto_ack' informs RabbitMQ that message is computed correctly
        auto_ack = False
        
        # Consuming msgs from queue -> basic_consume()
        # basic_consume(queue, on_message_callback, auto_act, exclusive, consumer_tag, arguments)
        # 'queue' = Queue name
        # 'on_message_callback' = function to manage every single message received
        # 'auto_ack' (optional)
        # 'exclusive' = If 'True', the queue will be used only by this consumer
        # 'consumer_tag' = uid for the consumer 
        # 'arguments' = plus dictionary specification
        channel.basic_consume(queue_name, message_handler, auto_ack)
        # Start loop
        channel.start_consuming()
    except KeyboardInterrupt as e:
        # Exit from basic_consume loop
        print(f"Interrupted signal {e}")
        try:
            sys.exit(0)
        except SystemExit as e1:
            print(f"SystemExit: {e1}")
            os._exit(0)
