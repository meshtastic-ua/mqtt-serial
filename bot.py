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
from meshtastic import serial_interface as serial, tcp_interface as tcp

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
            self.interface = serial.SerialInterface()
        elif devPath.startswith('/dev/'):
            self.interface = serial.SerialInterface(devPath)
        else:
            self.interface = tcp.TCPInterface(hostname=devPath.lstrip('tcp:'))

        self.logger = logger
        self.memcache = memcache
        self.channel = self.config['MQTT']['channel']
        # Slow start
        self.slow_start = True

    @property
    def my_id_hex(self):
        return self.interface.getMyUser().get('id')

    @property
    def my_id_int(self):
        return self.interface.localNode.nodeNum

    def onReceive(self, packet, interface):
        decoded = packet.get('decoded')
        node_id = packet.get('fromId')
        # skip packets without node_id
        if node_id is None:
            return
        # skip packets from us
        if decoded.get('portnum') != 'TEXT_MESSAGE_APP' or node_id == self.my_id_hex:
            return

        f_split = decoded.get('payload').decode().split(': ')
        message = f_split[0] if len(f_split) == 1 else ': '.join(f_split[1:])

        if self.filter_message(node_id, message):
            return

        self.logger.info(f'Radio to MQTT: f`{node_id}: {message}`')
        Thread(target=self.wait_and_send, args=(node_id, message, self.send_message), name=f"Radio_{node_id}").start()

    @property
    def generate_message_id(self):
        message_id = random.randint(0, 100000)
        if not self.memcache.get(message_id):
            self.memcache.set(message_id, True, expires=3600)
            return message_id
        return self.generate_message_id

    def send_message(self, text, destinationId=BROADCAST_ADDR):
        # Create first message without from
        m = mqtt_pb2.ServiceEnvelope()
        m.channel_id = self.channel
        m.gateway_id = self.my_id_hex
        m.packet.to = int(BROADCAST_NUM) if destinationId == BROADCAST_ADDR else int(destinationId.replace('!', '0x'))
        m.packet.hop_limit = 3
        m.packet.id = self.generate_message_id
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

    def wait_and_send(self, node_id, message, send_fn):
        # Wait for message to be sent
        full_msg = f'{node_id}: {message}'
        for _ in range(self.config['General'].getint('wait_before_send') * 10):
            if self.exit:
                return
            if self.memcache.get(full_msg) == "sent":
                return
            time.sleep(0.1)
        # Send message
        self.memcache.set(full_msg, "sent", expires=600)
        if self.slow_start:
            self.logger.info(f'Slow start active, messages will not be sent: `{full_msg}`')
            return
        self.logger.info(f'Wait and send: `{full_msg}`')
        send_fn(full_msg)

    def filter_message(self, node_id, message):
        full_msg = f'{node_id}: {message}'
        if not self.memcache.get(full_msg):
            self.memcache.set(full_msg, "waiting", expires=600)
        elif self.memcache.get(full_msg) == "waiting":
            self.logger.info(f'Already waiting for `{full_msg}`')
            self.memcache.set(full_msg, "sent", expires=600)
            return True
        elif self.memcache.get(full_msg) == "sent":
            return True
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
            self.logger.error("Error %s occurred while parsing message: ", exc, str(msg.payload))
            return

        portnum = m.packet.decoded.portnum
        if portnum != PortNum.TEXT_MESSAGE_APP:
            return

        # confirm that it's not us
        asDict = json_format.MessageToDict(m)
        nodeInt = asDict.get('packet', {}).get('from', 0)
        if nodeInt == self.my_id_int:
            return

        node_id = hex(nodeInt).replace('0x', '!')
        f_split = m.packet.decoded.payload.decode().split(': ')
        message = f_split[0] if len(f_split) == 1 else ': '.join(f_split[1:])
        #
        if self.filter_message(node_id, message):
             return

        self.logger.info(f'MQTT to Radio: `{node_id}: {message}`')
        Thread(target=self.wait_and_send, args=(node_id, message, self.interface.sendText), name=f"MQTT_{node_id}").start()

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

    def disable_slow_start(self):
        time.sleep(300)
        self.slow_start = False
        self.logger.info('Slow start disabled...')

    def run(self):
        mesh_thread = Thread(target=self.run_mesh)
        mesh_thread.start()
        mqtt_thread = Thread(target=self.run_mqtt)
        mqtt_thread.start()
        # Disable slow start after 5 minutes
        Thread(target=self.disable_slow_start, daemon=True).start()
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
