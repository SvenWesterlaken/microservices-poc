import pika, os, json
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
    is_debug = settings['mode'] == "logging"

if __name__ == '__main__':
    connection, channel = connectToRabbitMQ()

    channel.queue_declare(queue='invoice_queue', durable=True)

    props = pika.BasicProperties(delivery_mode=2)  # make message persistent

    for i in range(1,settings['amount_of_jobs'] + 1):
        message = f'Maintainance Job {i}'

        channel.basic_publish(exchange='', routing_key='invoice_queue', body=message, properties=props)

        if is_debug:
            print('[x]', 'Sent:\t', message)


    connection.close()
