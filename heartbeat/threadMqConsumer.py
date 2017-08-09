# -*- coding: utf-8 -*-
import pika
import threading
import time



class Consumers()::
   
    # static
    threads={}
    instances={}

    # static
    # consumerDict like {consumerName:(amqpUrl,queuName)}
    def createUpdateDeleteConsumers(consumerDict):
        a=pika.BlockingConnection(pika.URLParameters(mqConf['amqpUrl']))
        return c


    # static
    # returns list with messages or raises error
    # clear internal message store after return
    def popConsumerMessages(consumerName):
        pass


    def _stopConsume(self):
        pass


    def _onMessage(channel, method_frame, header_frame, body):
        print("message",method_frame.delivery_tag)
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)


    def _startConsume(self):
        t = threading.Thread(target=self._consumeLoop)
        t.start()

    # Step #2
    def _onConnected(connection):
        """Called when we are fully connected to RabbitMQ"""
        # Open a channel
        connection.channel(on_channel_open)

    # Step #3
    def _onChannelOpen(new_channel):
        """Called when our channel has opened"""
        global channel
        channel = new_channel
        channel.queue_declare(queue="test", durable=True, exclusive=False, auto_delete=False, callback=on_queue_declared)

    # Step #4
    def _onQueueDeclared(frame):
        """Called when RabbitMQ has told us our Queue has been declared, frame is the response from RabbitMQ"""
        channel.basic_consume(handle_delivery, queue='test')

    # Step #5
    def handleDelivery(channel, method, header, body):
        """Called when we receive a message from RabbitMQ"""
        print ('msg')
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def timeout():
        print(connection.is_open)
        if connection.is_open:
            # channel.stop_consuming()
            print('1')
            print('2')
            # channel.close()
            print('3')
            # connection.close()
            print('4')
            # connection.ioloop.stop()
            # print(dir(channel))
            # con.ioloop.stop()
        else:
            pass
            connection.ioloop.stop()
        connection.add_timeout(5, timeout)


    def aaa():
        # Step #1: Connect to RabbitMQ using the default parameters
        parameters = pika.ConnectionParameters(
                        '127.0.0.1',
                        5672,
                        '/',
                        pika.PlainCredentials('guest', 'guest'))

        print('begin')
        while True:
            try:
                connection = pika.SelectConnection(parameters, on_connected)
                timeout.con=connection
                connection.add_timeout(5, timeout)
                connection.ioloop.start()
                connection.close()    
            except Exception as e:
                print("error",str(e))
            
            
            print("retry")
            time.sleep(5)

        print ('end')
            # # Loop until we're fully closed, will stop on its own
            # connection.ioloop.start()


def main():
    pass

if __name__=='__main__':
    main()
