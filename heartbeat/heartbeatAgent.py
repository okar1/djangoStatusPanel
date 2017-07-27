# -*- coding: utf-8 -*-
import pika
from time import time

receiveFromQueue='heartbeatAgentRequest'
sendToExchange='heartbeatAgentReply'
mqIp='127.0.0.1'
mqPort='15672'
mqUser='guest'
mqPassword='guest'
mqVhost='/'
logFile='heartbeatAgent.log'

# receive heartbeat tasks request from rabbitmq queue
def receiveHeartBeatTasks():
    return []

# send heartbeat tasks request to rabbitmq exchange
def sendHeartBeatTasks(tasksToPoll):
	return

def processHeartBeatTasks(tasksToPoll):
	pass

if __name__ == '__main__':
	tasksToPoll=receiveHeartBeatTasks()
	processHeartBeatTasks(tasksToPoll)
	sendHeartBeatTasks(tasksToPoll) 