#!/usr/bin/env python3

import configparser
import logging
import os
import random
import socket
import time

from typing import Any

from threading import RLock, Thread

import google.protobuf.json_format as json_format
import paho.mqtt.client as mqtt

from meshtastic import BROADCAST_ADDR, BROADCAST_NUM
from meshtastic import mqtt_pb2 as mqtt_pb2
from meshtastic import portnums_pb2 as PortNum
from meshtastic import serial_interface as serial

from pubsub import pub

# pylint:disable=no-name-in-module
from setproctitle import setthreadtitle


def setup_logger(name=__name__, level=logging.INFO) -> logging.Logger:
    """
    Set up logger and return usable instance

    :param name:
    :param level:

    :return:
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # create console handler and set level to debug
    handler = logging.StreamHandler()
    handler.setLevel(level)

    with open('VERSION', 'r', encoding='utf-8') as fh:
        VERSION = fh.read().rstrip('\n')
    LOGFORMAT = '%(asctime)s - %(name)s/v{} - %(levelname)s file:%(filename)s %(funcName)s line:%(lineno)s %(message)s'
    LOGFORMAT = LOGFORMAT.format(VERSION)

    # create formatter
    formatter = logging.Formatter(LOGFORMAT)

    # add formatter to ch
    handler.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(handler)
    return logger


class Memcache:
    """
    Memcache - in-memory cache for storing data
    """
    name = 'Memcache.Reaper'

    def __init__(self, logger):
        self.lock = RLock()
        self.logger = logger
        self.cache = {}

    def get(self, key) -> Any:
        """
        get - get data by key

        :param key:
        :return:
        """
        value = self.get_ex(key)
        return value.get('data') if value else None

    def get_ex(self, key) -> Any:
        """
        get_ex - get data by key with expiration

        :param key:
        :return:
        """
        with self.lock:
            return self.cache.get(key)

    def set(self, key, value, expires=0) -> None:
        """
        set - set data by key

        :param key:
        :param value:
        :param expires:
        :return:
        """
        with self.lock:
            if expires > 0:
                expires = time.time() + expires
            self.cache[key] = {'data': value, 'expires': expires}

    def delete(self, key) -> None:
        """
        delete - delete data by key

        :param key:
        :return:
        """
        with self.lock:
            del self.cache[key]

    def reaper(self) -> None:
        """
        reaper - reaper thread

        :return:
        """
        setthreadtitle(self.name)
        while True:
            time.sleep(0.1)
            for key in list(self.cache):
                expires = self.get_ex(key).get('expires')
                if time.time() > expires > 0:
                    self.logger.warning(f'Removing key {key}...')
                    self.delete(key)

    def run_noblock(self) -> None:
        """
        run_noblock - run reaper thread

        :return:
        """
        locker = Thread(target=self.reaper, daemon=True, name=self.name)
        locker.start()


class MQTTSerialBot:
    MessageMap = {}
    TOPICLIST = []

    def __init__(self, logger, memcache):
        self.exit = False
        # MQTT client
        self.client = None
        self.config = configparser.RawConfigParser()
        if os.path.exists('config.ini'):
            self.config.read('config.ini')
        else:
            raise RuntimeError('Config file could not be found...')
        devPath = self.config['Meshtastic']['port']
        if devPath == 'auto':
            devPath = None
        self.logger = logger
        self.memcache = memcache
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

        if self.filter_message(node_id, packet.get('id'), message):
            return

        self.logger.info(f'Radio to MQTT: {message}')
        self.send_message(message)

    @property
    def generate_message_id(self):
        message_id = random.randint(0, 100000)
        if not self.MessageMap.get(message_id):
            return message_id
        return self.generate_message_id()

    def send_message(self, text, destinationId=BROADCAST_ADDR):
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

        result = self.client.publish(f'msh/2/c/{self.channel}/{self.my_id_hex}', msX.SerializeToString())
        self.logger.info(f'MQTT send_message: {result}')

    def onConnection(self, interface, topic=pub.AUTO_TOPIC):
        self.logger.info('Connected to device')

    def run_mesh(self):
        pub.subscribe(self.onReceive, "meshtastic.receive")
        pub.subscribe(self.onConnection, "meshtastic.connection.established")
        while not self.exit:
            time.sleep(1)
        self.logger.info('Mesh exiting...')


    def filter_message(self, node_id, packet_id, message):
        # Filter by message ID
        if self.memcache.get(f'{node_id}_{packet_id}'):
            return True
        self.memcache.set(f'{node_id}_{packet_id}', True, expires=300)
        # Filter by message contents
        if self.memcache.get(f'{node_id}_{message}'):
            return True
        self.memcache.set(f'{node_id}_{message}', True, expires=300)
        return False

    def on_mqtt_connect(self, client, userdata, flags, rc):
        self.logger.info(f"Connected with result code {str(rc)}")
        client.subscribe(f'msh/2/c/{self.channel}/#')


    def on_mqtt_message(self, client, userdata, msg):
        topic = msg.topic.split('/')[3]
        if topic not in self.TOPICLIST and topic != self.channel:
            self.logger.info(f'Unauthorized topic: {topic}')
            self.TOPICLIST.append(topic)

        try:
            m = mqtt_pb2.ServiceEnvelope().FromString(msg.payload)
        except Exception as exc:
            self.logger.error(str(msg.payload))
            return

        portnum = m.packet.decoded.portnum
        if portnum != PortNum.TEXT_MESSAGE_APP:
            return

        # confirm that it's not us
        asDict = json_format.MessageToDict(m)
        nodeInt = asDict.get('packet', {}).get('from', 0)
        if nodeInt == self.my_id_int:
            return

        nodeId = hex(nodeInt).replace('0x', '!')
        if self.filter_message(nodeId, m.packet.id, m.packet.decoded.payload.decode()):
            return

        full_msg = m.packet.decoded.payload.decode()
        f_split = full_msg.split(': ')
        result = f_split[0] if len(f_split) == 1 else ': '.join(f_split[1:])
        #
        self.logger.info(f'MQTT to Radio: {nodeId}: {m.packet.id} {result}')
        self.interface.sendText(result)

    def run_mqtt(self):
        self.client = mqtt.Client()
        self.client.on_connect = self.on_mqtt_connect
        self.client.on_message = self.on_mqtt_message

        self.client.username_pw_set(self.config['MQTT']['username'], self.config['MQTT']['password'])

        while not self.exit:
            try:
                self.client.connect(self.config['MQTT']['broker'], int(self.config['MQTT']['port']), 60)
            except socket.timeout:
                self.logger.error('Connect timeout...')
                time.sleep(10)

            try:
                self.client.loop_forever()
            except TimeoutError:
                self.logger.error('Loop timeout...')
                time.sleep(10)
        self.logger.error('MQTT exiting...')

    def run(self):
        mesh_thread = Thread(target=self.run_mesh)
        mesh_thread.start()
        mqtt_thread = Thread(target=self.run_mqtt)
        mqtt_thread.start()
        while not self.exit:
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                self.logger.error('Exit requested...')
                self.exit = True


if __name__ == '__main__':
    logger = setup_logger(name='bot.py')
    memcache = Memcache(logger)
    memcache.run_noblock()
    bot = MQTTSerialBot(logger, memcache)
    bot.run()
