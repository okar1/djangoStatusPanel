# -*- coding: utf-8 -*-
import pika
import threading
import time

class MqConsumers():
   
    # const
    stopConsumerCheckIntervalSec=1
    queueArgs={'x-message-ttl':1800000}

    # static vars
    # consumers store like {name: instance}
    store={}
    # this consumerd will not be deleted by cleanupConsumers()
    updatedConsumers=set()

    # non-static (instance) vars
    consumerName=""
    amqpUrl=""
    queueName=""
    stopConsumerFlag=False
    queueDeclaredOK=False
    thread=None
    error=None
    messages=[]

    # static
    # newStore like {consumerName:(amqpUrl,queueName)}
    def createUpdateConsumers(newStore):
        store=MqConsumers.store
        sStore=set(store)
        sNewStore=set(newStore)

        consToAdd=sNewStore-sStore
        # remember consToAdd in updatedConsumers set
        MqConsumers.updatedConsumers.update(consToAdd)
        for conName in consToAdd:
            newC=MqConsumers(conName,newStore[conName][0],newStore[conName][1])
            store[conName]=newC
            newC.startConsumer()

        consToUpdate=sStore.intersection(sNewStore)
        # remember consToUpdate in updatedConsumers set
        MqConsumers.updatedConsumers.update(consToUpdate)
        for conName in consToUpdate:
            c=store[conName]
            newUrl=newStore[conName][0]
            newQueue=newStore[conName][1]
            
            if c.amqpUrl!=newUrl or c.queueName!=newQueue:
                c.stopConsumer()
                store.pop(conName)

                newC=MqConsumers(conName,newStore[conName][0],newStore[conName][1])
                store[conName]=newC
                newC.startConsumer()

    # static
    # stop consumers wich was not updated since last cleanupConsumers() call
    def cleanupConsumers():
        consToDelete=set(MqConsumers.store)-MqConsumers.updatedConsumers
        for conName in consToDelete:
            MqConsumers.store[conName].stopConsumer()
            MqConsumers.store.pop(conName)

        MqConsumers.updatedConsumers=set()


    # static
    # returns list with messages or raises error
    # clear internal message store after return
    def popConsumerMessages(consumerName):
        store=MqConsumers.store
        if consumerName not in store.keys():
            return []

        res=store[consumerName].messages
        store[consumerName].messages=[]
        error=store[consumerName].error
        store[consumerName].error=None
        if error is not None:
            raise Exception(error)
        return res


    def __init__(self,consumerName,amqpUrl,queueName):
        self.consumerName=consumerName
        self.amqpUrl=amqpUrl
        self.queueName=queueName
        # self.queueArgs=queueArgs

    # starts a mq consumer in new thread (async)
    def startConsumer(self):
        self.thread=threading.Thread(target=self._consumerLoop)
        self.thread.start()
        # print('start mq consumer for',self.consumerName)

    # stops a mq consumer and wait for stop (sync)
    def stopConsumer(self):
        self.stopConsumerFlag=True
        self.thread.join()
        print('Consumer stopped for',self.consumerName)

    def _consumerLoop(self):
        connection=None
        channel=None

        # Step #2
        def onConnected(connection):
            """Called when we are fully connected to RabbitMQ"""
            nonlocal self
            # Open a channel
            if not connection.channel(onChannelOpen):
                self.error="Channel open error for "+self.consumerName


        # Step #3
        def onChannelOpen(newChannel):
            """Called when our channel has opened"""
            nonlocal channel
            channel = newChannel
            nonlocal self
            channel.queue_declare(queue=self.queueName, durable=True, exclusive=False, auto_delete=False, arguments=self.queueArgs, callback=onQueueDeclared)

        # Step #4
        def onQueueDeclared(frame):
            """Called when RabbitMQ has told us our Queue has been declared, frame is the response from RabbitMQ"""
            # print("consumer start")
            nonlocal self
            self.queueDeclaredOK=True
            if not channel.basic_consume(onMessage, queue=self.queueName):
                self.error="Consumer start error for "+self.consumerName
            else:
                print("Consumer started for",self.consumerName)


        def onMessage(channel, method, header, body):
            # print("message",method.delivery_tag)
            nonlocal self
            self.messages+=[(method,header,body)]
            channel.basic_ack(delivery_tag=method.delivery_tag)


        def checkConsumerStop():
            # print("timcheckConsumerStop")
            nonlocal connection
            nonlocal self

            if not connection.is_open:
                connection.add_timeout(self.stopConsumerCheckIntervalSec, checkConsumerStop)
            else:    
                if not self.queueDeclaredOK:
                    self.error="Queue declare error for "+self.consumerName

                if self.stopConsumerFlag or (self.error is not None):
                        connection.ioloop.stop()
                else:
                    connection.add_timeout(self.stopConsumerCheckIntervalSec, checkConsumerStop)

        # Step #1: Connect to RabbitMQ
        try:
            # print('started mq consumer for',self.consumerName)
            while True:
                connection = pika.SelectConnection(pika.URLParameters(self.amqpUrl), onConnected)
                connection.add_timeout(self.stopConsumerCheckIntervalSec, checkConsumerStop)
                
                connection.ioloop.start()
                connection.close()

                if self.stopConsumerFlag:
                    # print ('consumer loop end for',self.consumerName)
                    break
                
                # stop for 5 seconds. Check for break every 1 second
                breakThisThread=False
                if self.error is not None:
                    print('connection lost ('+self.error+'). Try to reconnect in 5 sec for',self.consumerName)
                else:
                    print('connection lost. Try to reconnect in 5 sec for',self.consumerName)

                for i in range(5):
                    if self.stopConsumerFlag:
                        breakThisThread=True
                        break
                    time.sleep(1)
                if breakThisThread:
                    break
                # reset error status for next iteration
                self.error=None

        except Exception as e:
            self.error=str(e)


def main():
    queueArgs={'x-message-ttl':1800000}
    # queueArgs={}
    cons1={"c1":("amqp://localhost/%2f","test",queueArgs),"c2":("amqp://localhost/%2f","q2",queueArgs)}
    MqConsumers.createUpdateDeleteConsumers(cons1)
    time.sleep(5)
    try:
        msg=MqConsumers.popConsumerMessages("c1")
        MqConsumers.popConsumerMessages("c2")
        print(len(msg), "msg loaded")
        print (msg[0])
    except Exception as e:
        print(str(e))
    cons3={}
    MqConsumers.createUpdateDeleteConsumers(cons3)

if __name__=="__main__":
    main()
