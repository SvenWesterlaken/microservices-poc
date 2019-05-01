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
    is_debug = settings['debug']

def parseJobMessage(msg):
    split = msg.split('/')

    msg_index = int(split[0][1:].strip())
    msg_total = int(split[1][:split[1].index(']')].strip())

    job_index = int(int(split[1][-2:].strip()))
    job_total = int(split[2][:2].strip())

    return ((msg_index, msg_total),(job_index, job_total))

if __name__ == '__main__':
    connection, channel = connectToRabbitMQ()

    channel.queue_declare(queue='notification_queue', durable=True)

    if is_debug: print('[*]', 'Waiting for messages...')

    start_time = timer()
    print('[-]', 'Started benchmark')
    item_index = 0

    def callback(ch, method, properties, body):
        global item_index
        msg = body.decode("utf-8")
        item_index += 1
        if is_debug: print('[x] Received:\t', msg)

        (msg_index, msg_total), (job_index, job_total) = parseJobMessage(msg)

        if job_index == job_total:
            global start_time
            
            end_time = timer()

            print('[-]', f'Processing {Fore.MAGENTA}{job_total}{Fore.RESET} items took ' + Fore.CYAN + str(round(end_time - start_time, 3)) + Fore.RESET + 's')

            start_time = timer()

        if item_index == job_total * settings['test_amount']:
            print('[-]', '--------------------------------')
            item_index = 0

        if msg_index == msg_total: print('[-]', 'Finished all tests...')

        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='notification_queue', on_message_callback=callback)

    channel.start_consuming()
