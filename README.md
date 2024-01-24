# mqtt-serial
Meshtastic MQTT to serial bridge

Bridge waits for the first 5 minutes before sending any messages.


## Dependencies and configuration (needs to be done once)

- `pip install pipenv`

- `git clone https://github.com/meshtastic-ua/mqtt-serial; cd mqtt-serial`

- `pipenv install`

- `cp config.ini.example config.ini`

- Edit `config.ini`

## Run

`make`
