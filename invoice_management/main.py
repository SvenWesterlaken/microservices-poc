import pika, time, os, json
from retrying import retry

@retry(stop_max_attempt_number=3, wait_fixed=10000)
def connectToRabbitMQ():
    print('[*]', 'Trying to connect to RabbitMQ...')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    print('[*]', 'Connected to RabbitMQ')
    return (connection, channel)

with open(os.path.join(os.path.dirname(__file__), '../config/config.json'), 'r') as settings_file:
    settings = json.load(settings_file)
    is_debug = settings['debug']

if __name__ == '__main__':
    connection, channel = connectToRabbitMQ()

    channel.queue_declare(queue='invoice_queue', durable=True)
    if is_debug:
        print('[*]', 'Waiting for messages...')

    channel.queue_declare(queue='notification_queue', durable=True)
    props = pika.BasicProperties(delivery_mode=2)  # make message persistent

    def callback(ch, method, properties, body):
        msg = body.decode("utf-8")

        if is_debug:
            print('[x]', 'Received:\t', msg)

        # Sleep for 10 seconds
        time.sleep(settings['sleeptime'])

        message = msg + ' [Invoice]'

        ch.basic_ack(delivery_tag=method.delivery_tag)
        channel.basic_publish(exchange='', routing_key='notification_queue', body=message, properties=props)
        if is_debug:
            print('[x]', 'Sent:\t', message)


    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='invoice_queue', on_message_callback=callback)

    channel.start_consuming()
