import pika, colorama, os, json
from timelog import pretty_time_delta
from retrying import retry
from timeit import default_timer as timer
from colorama import Fore

colorama.init()

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
    amount = settings['amount_of_jobs']

if __name__ == '__main__':
    connection, channel = connectToRabbitMQ()

    channel.queue_declare(queue='notification_queue', durable=True)

    if is_debug:
        print('[*]', 'Waiting for messages...')

    start_time = timer()
    print('[-]', 'Started benchmark')

    def callback(ch, method, properties, body):
        msg = body.decode("utf-8")
        if is_debug:
            print('[x] Received:\t', msg)

        if int(msg.split()[2]) == amount:
            end_time = timer()
            print('[-]', f'Processing {Fore.MAGENTA}{amount}{Fore.RESET} items took', Fore.CYAN + str(round(end_time - start_time, 3)) + Fore.RESET + 's')

        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='notification_queue', on_message_callback=callback)

    channel.start_consuming()
