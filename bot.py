#!/usr/bin/env python3

import time

from meshtastic import serial_interface as serial
from pubsub import pub

class MQTTSerialBot:
    def __init__(self, port=None):
        self.interface = serial.SerialInterface()

    @property
    def my_id(self):
        return self.interface.getMyUser().get('id')

    def onReceive(self, packet, interface):
        decoded = packet.get('decoded')
        node_id = packet.get('fromId')
        if decoded.get('portnum') != 'TEXT_MESSAGE_APP' or node_id == self.my_id:
            return

        message = decoded.get('payload').decode()
        print(message)

    def onConnection(self, interface, topic=pub.AUTO_TOPIC):
        print('Connected to device')

    def run(self):
        pub.subscribe(self.onReceive, "meshtastic.receive")
        pub.subscribe(self.onConnection, "meshtastic.connection.established")
        while True:
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                break
        print('Exiting...')

if __name__ == '__main__':
    bot = MQTTSerialBot()
    bot.run()
