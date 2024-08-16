#!/usr/bin/env python
import pika

# Connect to localhost server
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))

channel = connection.channel()

# Declare queue
channel.queue_declare(queue='rpc_queue')

# Fibonacci function definition
def fib(n):
    if n==0:
        return 0
    elif n == 1:
        return 1
    else:
        return fib(n-1) + fib(n-2)

# Declare callback function
# Executed when the request is received
# It sends the response back
def on_request(ch, method, props, body):
    n = int(body)

    print(f" [.] fib({n})")
    response = fib(n)

    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                        props.correlation_id),
                     body=str(response))
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Fair dispatch
channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='rpc_queue', on_message_callback=on_request)
+
print(" [X] Awaiting RPC requests")
channel.start_consuming()