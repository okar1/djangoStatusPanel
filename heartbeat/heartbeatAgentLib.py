# -*- coding: utf-8 -*-
amqpPort = 5672

timeStampFormat="%Y%m%d%H%M%S"
hbOwnExchange='heartbeatAgentRequest'
hbOwnQueue='heartbeatAgentReply'

# mqconfig --> (msgTotal, mqConnection)
def getMqConnection(mqConf):

    # try to connect via http api
    req = requests.get('http://{0}:{1}/api/overview'.format(
        mqConf["server"], mqConf["port"]), auth=(mqConf["user"], mqConf["pwd"]))

    # get total number of messages on rabbitMQ
    msgTotal = req.json()['queue_totals']['messages']

    # try to connect via amqp
    amqpLink = pika.BlockingConnection(
        pika.ConnectionParameters(
            mqConf["server"],
            amqpPort,
            '/',
            pika.PlainCredentials(mqConf["user"], mqConf["pwd"])))
    return (msgTotal, amqpLink)

def sendHeartBeatTasks(mqAmqpConnection,tasksToPoll):
    if not mqAmqpConnection:
        return ['Соединение с RabbitMQ не установлено']
    
    errors=[]
    nowDateTime=(datetime.utcnow()).strftime(timeStampFormat)

    channel = mqAmqpConnection.channel()

    try:
        channel.exchange_declare(exchange=hbOwnExchange, exchange_type='topic')
    except Exception as e:
        errors+= [str(e)]
        return errors

    for taskKey, task in tasksToPoll.items():
        
        # process only heartbeat tasks
        if task.get('module',None)!='heartbeat':
            continue

        # process only enabled tasks
        if not task.get('enabled',False):
            continue

        msgRoutingKey=task['agentKey']
        msgBody=(json.dumps(task['config'])).encode('UTF-8')
        
        msgHeaders={'key':taskKey,'type':task['type'],'timestamp':nowDateTime}
        channel.basic_publish(
            exchange=hbOwnExchange,
            routing_key=msgRoutingKey,
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
                content_type='application/json',
                content_encoding='UTF-8',
                priority=0,
                expiration="86400000",
                headers=msgHeaders),
            body=msgBody
        )
    
    return errors


def receiveHeartBeatTasks(mqAmqpConnection,tasksToPoll):
    if not mqAmqpConnection:
        return ['Соединение с RabbitMQ не установлено']
    vErrors=[]

    amqpLink = mqAmqpConnection.channel()
    mqMessages = [""]
    while len(mqMessages) > 0:

        # connect and check errors (amqp)
        getOk = None
        try:
            getOk, *mqMessages = amqpLink.basic_get(hbOwnQueue, no_ack=True)
        except Exception as e:
            vErrors += [str(e)]
            return vErrors

        if getOk:
            if mqMessages[0].content_type != 'application/json':
                vErrors += ["Неверный тип данных " + mqMessages[0].content_type]
                return vErrors
            mqMessages = [mqMessages]
        else:
            mqMessages = []

        # now we have list of mqMessages
        for msg in mqMessages:

            try:
                headers = msg[0].headers
                taskKey=headers['key']
                taskTimeStamp=headers['timestamp']
            except Exception as e:
                errStr = "Ошибка обработки сообщения: неверный заголовок."
                if errStr not in vErrors:
                    vErrors += [errStr]
                continue

            # parse message payload
            try:
                mData = json.loads((msg[1]).decode('utf-8'))
                a=random.random()
                if a>0.3:
                    continue
                mData.update({'value':random.choice(([0,1,2,3,4,5,6,7,8,9]))})
                taskValue=mData['value']
            except Exception as e:
                vErrors += ['Ошибка обработки сообщения: неверное содержимое.']
                return vErrors

            if taskKey not in tasksToPoll.keys():
                vErrors += ['Ошибка обработки сообщения: неверный ключ.']
                return vErrors
            
            tasksToPoll[taskKey]['timeStamp']=taskTimeStamp
            tasksToPoll[taskKey]['value']=taskValue
                        
        # endfor messages in current request
    # endwhile messages in rabbit queue
    return vErrors