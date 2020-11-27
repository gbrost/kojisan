import datetime
import time
import logging
import json
from influxdb import InfluxDBClient
from influxdb.client import InfluxDBClientError

from grove.i2c import Bus

LOG_LEVEL = logging.INFO

LOG_FILE = "/var/log/sht35.log"
LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"
logging.basicConfig(filename=LOG_FILE, format=LOG_FORMAT, level=LOG_LEVEL)
USER = 'grafana'
PASSWORD = '43ralight'
DBNAME = 'kojibox'
HOST='localhost'
PORT=8086

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
        # Temp MSB, Temp LSB, Temp, Humididty MSB, Humidity LSB, Humidity CRC
        data = self.bus.read_i2c_block_data(0x45, 0x00, 6)
        temperature = data[0] * 256 + data[1]
        celsius = -45 + (175 * temperature / 65535.0)
        humidity = 100 * (data[3] * 256 + data[4]) / 65535.0
        if data[2] != CRC(data[:2]):
            raise RuntimeError("temperature CRC mismatch")
        if data[5] != CRC(data[3:5]):
            raise RuntimeError("humidity CRC mismatch")
        return celsius, humidity

def create_dictionary_for_value(temperature,humidity):
    return [{
        "measurement": "kojiboxclimate",
        "tags": {
		    "host": "kojisan"
        },
        "time": int(time.time() * 1000),
        "fields": {
            "temperature": temperature,
            "humidity": humidity
        }
    }]

def main():
    sensor = GroveTemperatureHumiditySensorSHT3x()
    client = InfluxDBClient(host='localhost', port=8086)
    retention_policy = 'awesome_policy'
    client.switch_database('kojibox')
    client.create_retention_policy(retention_policy, '3d', 3, default=True)

    while True:
        temperature, humidity = sensor.read()
        print('Temperature in Celsius is {:.2f} C'.format(temperature))
        print('Relative Humidity is {:.2f} %'.format(humidity))
        payload = create_dictionary_for_value(temperature, humidity)
        print("Payload:{}\n".format(payload))
        client.write_points(payload, database='kojibox', time_precision='ms', protocol='json')
        logging.info("{}\n".format(payload))
        result = client.query('select temperature,humidity from kojiboxclimate;')
        print("Result: {0}".format(result))
        time.sleep(60)

if __name__ == "__main__":
    main()


