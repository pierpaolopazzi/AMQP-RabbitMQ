#!/usr/bin/env python
import pika
import uuid


class FibonacciRpcClient(object):

    def __init__(self):

        # Connection to localhost
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))

        self.channel = self.connection.channel()

        # Queue declare
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        # Consume
        # Subscribe to the 'callback_queue' to receive server response
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

        self.response = None
        self.corr_id = None

    # Callback executed on every response
    # It saves the response in 'self.response'
    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    # RPC request
    # 'correlation_id' is a message property
    # it correlates RPC responses with requests
    # it's set to a unique value for every request
    # 'call' method generates a unique 'correlation_id' number and save it
    # The 'on_response' callback will use this value to catch the appropriate response
    def call(self, n):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='rpc_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=str(n))
        while self.response is None:
            self.connection.process_data_events(time_limit=None)
        return int(self.response)

# fibonacci remote procedure call
fibonacci_rpc = FibonacciRpcClient()

print(" [x] Requesting fib(30)")
response = fibonacci_rpc.call(30)
print(f" [.] Got {response}")