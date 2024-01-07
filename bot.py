#!/usr/bin/env python3

import configparser
import os
import random
import socket
import time

from threading import Thread

import google.protobuf.json_format as json_format
import paho.mqtt.client as mqtt

from meshtastic import BROADCAST_ADDR, BROADCAST_NUM
from meshtastic import mqtt_pb2 as mqtt_pb2
from meshtastic import portnums_pb2 as PortNum
from meshtastic import serial_interface as serial

from pubsub import pub


class MQTTSerialBot:
    MessageMap = {}
    TOPICLIST = []

    def __init__(self, port=None):
        self.exit = False
        self.config = configparser.RawConfigParser()
        if os.path.exists('config.ini'):
            self.config.read('config.ini')
        else:
            raise RuntimeError('Config file could not be found...')
        devPath = self.config['Meshtastic']['port']
        if devPath == 'auto':
            devPath = None
        self.interface = serial.SerialInterface(devPath)
        self.channel = self.config['MQTT']['channel']

    @property
    def my_id_hex(self):
        return self.interface.getMyUser().get('id')

    @property
    def my_id_int(self):
        return self.interface.localNode.nodeNum

    def onReceive(self, packet, interface):
        decoded = packet.get('decoded')
        node_id = packet.get('fromId')
        if decoded.get('portnum') != 'TEXT_MESSAGE_APP' or node_id == self.my_id:
            return

        message = decoded.get('payload').decode()
        print(message)

    @property
    def generate_message_id(self):
        message_id = random.randint(0, 100000)
        if not self.MessageMap.get(message_id):
            return message_id
        return self.generate_message_id()

    def send_message(self, text, client, destinationId=BROADCAST_ADDR):
        # Create first message without from
        m = mqtt_pb2.ServiceEnvelope()
        m.channel_id = self.channel
        m.gateway_id = self.my_id_hex
        m.packet.to = int(BROADCAST_NUM) if destinationId == BROADCAST_ADDR else int(destinationId.replace('!', '0x'))
        m.packet.hop_limit = 3
        m.packet.id = self.generate_message_id()
        m.packet.rx_time = int(time.time())
        m.packet.decoded.portnum = PortNum.TEXT_MESSAGE_APP
        m.packet.decoded.payload = bytes(text, 'utf-8')

        # Create second message from first one and add from
        asDict = json_format.MessageToDict(m.packet)
        asDict["from"] = self.my_id_int

        # Create third message and combine first and second
        full = json_format.MessageToDict(m)
        full['packet'] = asDict

        # Create forth and final message. Convert it to protobuf
        new_msg = mqtt_pb2.ServiceEnvelope()
        msX = json_format.ParseDict(full, new_msg)

        result = client.publish(f'msh/2/c/{self.channel}/{self.my_id_hex}', msX.SerializeToString())
        print(result)

    def onConnection(self, interface, topic=pub.AUTO_TOPIC):
        print('Connected to device')

    def run_mesh(self):
        pub.subscribe(self.onReceive, "meshtastic.receive")
        pub.subscribe(self.onConnection, "meshtastic.connection.established")
        while not self.exit:
            time.sleep(1)
        print('Mesh exiting...')


    def on_mqtt_connect(self, client, userdata, flags, rc):
        print("Connected with result code "+str(rc))
        client.subscribe(f'msh/2/c/{self.channel}/#')


    def on_mqtt_message(self, client, userdata, msg):
        topic = msg.topic.split('/')[3]
        if not topic in self.TOPICLIST and topic != self.channel:
            print(f'Unauthorized topic: {topic}')
            self.TOPICLIST.append(topic)

        try:
                m = mqtt_pb2.ServiceEnvelope().FromString(msg.payload)
        except Exception as exc:
            print(str(msg.payload))
            return

        portnum = m.packet.decoded.portnum
        if portnum != PortNum.TEXT_MESSAGE_APP:
            return

        # confirm that it's not us
        asDict = json_format.MessageToDict(m)
        nodeInt = asDict.get('packet', {}).get('from', 0)
        if nodeInt == self.my_id_int:
            return

        # check message id
        if self.MessageMap.get(m.packet.id):
            return
        self.MessageMap[m.packet.id] = True

        full_msg = m.packet.decoded.payload.decode()
        f_split = full_msg.split(': ')
        result = ''
        if len(f_split) == 1:
            result = f_split[0]
        else:
            result = ': '.join(f_split[1:])

        nodeId = hex(nodeInt).replace('0x', '!')
        print(f'{nodeId}: {m.packet.id} {result}')
        self.interface.sendText(result)

    def run_mqtt(self):
        client = mqtt.Client()
        client.on_connect = self.on_mqtt_connect
        client.on_message = self.on_mqtt_message

        client.username_pw_set(self.config['MQTT']['username'], self.config['MQTT']['password'])

        while not self.exit:
            try:
                client.connect(self.config['MQTT']['broker'], int(self.config['MQTT']['port']), 60)
            except socket.timeout:
                print('Connect timeout...')
                time.sleep(10)

            try:
                client.loop_forever()
            except TimeoutError:
                print('Loop timeout...')
                time.sleep(10)
        print('MQTT exiting...')

    def run(self):
        mesh_thread = Thread(target=self.run_mesh)
        mesh_thread.start()
        mqtt_thread = Thread(target=self.run_mqtt)
        mqtt_thread.start()
        while not self.exit:
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                print('Exit requested...')
                self.exit = True



if __name__ == '__main__':
    bot = MQTTSerialBot()
    bot.run()
