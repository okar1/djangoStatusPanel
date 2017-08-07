# -*- coding: utf-8 -*-
import pika
import threading
import time

def on_message(channel, method_frame, header_frame, body):
    print("message",method_frame.delivery_tag)
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)

class mqConsumers():
    _mqConnection=None
    _breakLoop=False
    queueName=""
    messages=[]
    errors=[]

    def createUpdateConsumer(consumerName,mqConnection,queueName):
        if not hasattr(mqConsumers,"consumers"):
            mqConsumers.consumers={}
        c=mqConsumers.consumers.get(consumerName,mqConsumers())
        c.setConnection(mqConnection)
        c.queueName=queueName
        mqConsumers.consumers[consumerName]=c
        return c

    # def __init__(self):
    #     super().__init__(self)

    def setConnection(self,connection):
        if self._mqConnection is not None and self._mqConnection.is_open:
            self._breakLoop=True
            if self._mqConnection is not None:
                self._mqConnection.channel().stop_consuming()
                self._mqConnection.channel().close()
                # self._mqConnection.close()
            print("connection closed")
        self._mqConnection=connection


    def _consumeLoop(self):
        if self._mqConnection!=None:
            channel = self._mqConnection.channel()
            channel.basic_consume(on_message, self.queueName)
            print("start cons")
            try:
                channel.start_consuming()
            except KeyboardInterrupt:
                print("interupt")
                channel.stop_consuming()
            print("end cons")                
            # connection.close()
            # print('connection lost. Reconnect in 5 seconds')
            # time.sleep(5)


    def startConsume(self):
        t = threading.Thread(target=self._consumeLoop)
        t.start()



def thr():
    print('kill')
    main.channel.stop_consuming()
def main():


    while True:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                'demo.tecom.nnov.ru',
                5672,
                '/',
                pika.PlainCredentials('guest', 'guest')))
        main.connection=connection
        channel = connection.channel()
        main.channel=channel
        
        # channel.add_on_cancel_callback(self.on_consumer_cancelled)

        connection.add_timeout(5,thr)
        print("start")
        channel.basic_consume(on_message, 'test')

        main.channel.start_consuming()
        # t = threading.Thread(target=thr)
        # t.start()
        print('wait to stop')
        time.sleep(7)
        print('stop')
        channel.stop_consuming()
        channel.close()
        
        connection.close()


        # c=mqConsumers.createUpdateConsumer('test',connection,'test')
        # c.startConsume()
        # print(len(mqConsumers.consumers['test'].messages))
        # time.sleep(10)
        # c._mqConnection.channel().stop_consuming()
        # c._mqConnection.channel().close()
        # c._mqConnection.close()
        
        break


if __name__=='__main__':
    main()
    print('over')
