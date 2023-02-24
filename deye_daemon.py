# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import logging
import sys
import time
import datetime

from deye_config import DeyeConfig
from deye_connector import DeyeConnector
from deye_modbus import DeyeModbus
from deye_sensors import sensor_list
from deye_mqtt import DeyeMqttClient
from deye_observation import Observation


class DeyeDaemon():
    
    def __init__(self, config: DeyeConfig):
        self.__log = logging.getLogger(DeyeDaemon.__name__)
        self.__config = config
        self.mqtt_client = DeyeMqttClient(config)
        connector = DeyeConnector(config)
        self.modbus = DeyeModbus(config, connector)
        self.sensors = [s for s in sensor_list if s.in_any_group(self.__config.metric_groups)]

    def do_task(self):
        self.__log.info("Reading start")
        self.__log.info("Simulate: %s" % self.__config.logger.simulate)
        if(self.__config.logger.simulate.lower() == 'true'):
            self.__log.info("Simulating inverter response using test data.")
            regs = {60: b'\x00\x03'}
        else:
            self.__log.info("Fetching inverter response using modbus.")
            regs = self.modbus.read_registers(0x3c, 0x4f) \
            | self.modbus.read_registers(0x50, 0x5f) \
            | self.modbus.read_registers(0x6d, 0x74)  
            self.__log.debug("Regs received: %s" % regs)

        timestamp = datetime.datetime.now()
        observations = []
        for sensor in self.sensors:
            value = sensor.read_value(regs)
            if value is not None:
                observation = Observation(sensor, timestamp, value)
                observations.append(observation)
                self.__log.debug(f'{observation.sensor.name}: {observation.value_as_str()}')

        self.mqtt_client.publish_observations(observations)
        self.__log.info("Reading completed")

def main():
    config = DeyeConfig.from_env()
    daemon = DeyeDaemon(config)
    while True:
        daemon.do_task()
        time.sleep(config.data_read_inverval)


if __name__ == "__main__":
    main()