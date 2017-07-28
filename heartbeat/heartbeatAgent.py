# # -*- coding: utf-8 -*-
# import pika
# from time import time


# mqIp='127.0.0.1'
# mqPort='15672'
# mqUser='guest'
# mqPassword='guest'
# mqVhost='/'
# logFile='heartbeatAgent.log'
# receiveFromQueue='heartbeatAgentRequest'
# sendToExchange='heartbeatAgentReply'
# maxMsgTotal=50000

# # receive heartbeat tasks request from rabbitmq queue
# def receiveHeartBeatTasks():
#     return []

# # send heartbeat tasks request to rabbitmq exchange
# def sendHeartBeatTasks(tasksToPoll):
#     return

# def processHeartBeatTasks(tasksToPoll):
#     pass

# if __name__ == '__main__':
#     # getMqConnection
#     tasksToPoll=receiveHeartBeatTasks()
#     processHeartBeatTasks(tasksToPoll)
#     sendHeartBeatTasks(tasksToPoll) 