import datetime
import time
import jwt
import paho.mqtt.client as mqtt
import logging

from grove.i2c import Bus

LOG_LEVEL = logging.INFO

LOG_FILE = "/var/log/sht35.log"
LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"
logging.basicConfig(filename=LOG_FILE, format=LOG_FORMAT, level=LOG_LEVEL)

ssl_private_key_filepath = '/home/pi/grove.py/kojibox_private.pem'
ssl_algorithm = 'RS256' # Either RS256 or ES256
root_cert_filepath = '/home/pi/grove.py/roots.pem'
project_id = 'kojiboxproject'
gcp_location = 'europe-west1'
registry_id = 'myregistry'
device_id = 'kojisan'

def CRC(data):
  crc = 0xff
  for s in data:
    crc ^= s
    for i in range(8):
      if crc & 0x80:
        crc <<= 1
        crc ^= 0x131
      else:
        crc <<= 1
  return crc

cur_time = datetime.datetime.utcnow()

def create_jwt():
  token = {
      'iat': cur_time,
      'exp': cur_time + datetime.timedelta(minutes=60),
      'aud': project_id
  }

  with open(ssl_private_key_filepath, 'r') as f:
    private_key = f.read()

  return jwt.encode(token, private_key, ssl_algorithm)

_CLIENT_ID = 'projects/{}/locations/{}/registries/{}/devices/{}'.format(project_id, gcp_location, registry_id, device_id)
_MQTT_TELEMETRY_TOPIC = '/devices/{}/events'.format(device_id)

client = mqtt.Client(client_id=_CLIENT_ID)
# authorization is handled purely with JWT, no user/pass, so username can be whatever
client.username_pw_set(
    username='unused',
    password=create_jwt())

def error_str(rc):
    return '{}: {}'.format(rc, mqtt.error_string(rc))

def on_connect(unusued_client, unused_userdata, unused_flags, rc):
    print('on_connect', error_str(rc))

def on_publish(unused_client, unused_userdata, unused_mid):
    print('on_publish')

client.on_connect = on_connect
client.on_publish = on_publish

client.tls_set(ca_certs=root_cert_filepath) # Replace this with 3rd party cert if that was used when creating registry
client.connect('mqtt.googleapis.com', 8883)
client.loop_start()


class GroveTemperatureHumiditySensorSHT3x(object):
    def __init__(self, address=0x45, bus=None):
        self.address = address
        # I2C bus
        self.bus = Bus(bus)

    def read(self):
        # high repeatability, clock stretching disabled
        self.bus.write_i2c_block_data(self.address, 0x24, [0x00])
        # measurement duration < 16 ms
        time.sleep(0.016)

        # read 6 bytes back
        # Temp MSB, Temp LSB, Temp CRC, Humididty MSB, Humidity LSB, Humidity CRC
        data = self.bus.read_i2c_block_data(0x45, 0x00, 6)
        temperature = data[0] * 256 + data[1]
        celsius = -45 + (175 * temperature / 65535.0)
        humidity = 100 * (data[3] * 256 + data[4]) / 65535.0
        if data[2] != CRC(data[:2]):
            raise RuntimeError("temperature CRC mismatch")
        if data[5] != CRC(data[3:5]):
            raise RuntimeError("humidity CRC mismatch")
        return celsius, humidity

def main():
    sensor = GroveTemperatureHumiditySensorSHT3x()
    while True:
        temperature, humidity = sensor.read()
        print('Temperature in Celsius is {:.2f} C'.format(temperature))
        print('Relative Humidity is {:.2f} %'.format(humidity))
	payload =  '{{"ts": \"{}\", "temperature": {}, "humidity": {}}}'.format(datetime.datetime.utcnow().isoformat()[:-3], temperature, humidity)
	print("{}\n".format(payload))
	logging.info("{}\n".format(payload))
	client.publish(_MQTT_TELEMETRY_TOPIC, payload, qos=1)
        time.sleep(60)

if __name__ == "__main__":
	main()


