import ds18x20
from machine import Pin
from onewire import OneWire, OneWireError

import uasyncio as asyncio


class TempSensorDS18B20:

    def __init__(self, name: str, pin_num: int) -> None:
        """
        Digital temp sensor DS18B20 for the tlvlp.iot project

        Tested on ESP32 MCUs
        :param pin_num: Digital input pin number for reading measurements
        """
        self.status = {'type': 'ds18b20', 'name': name}
        one_wire = OneWire(Pin(pin_num))
        self.channel = ds18x20.DS18X20(one_wire)

    async def get_first_reading_in_celsius(self, delay_ms=750):
        """
        :param delay_ms: a set delay before the reading is done
        :return: readings from the first sensor on the pin
        The order is not guaranteed so works best with only one sensor
        """
        readings = await self.read_all_celsius(delay_ms)
        if len(readings) != 0:
            self.status.update({'reading': readings[0]})
        else:
            self.status.update({'reading': -1.0})
        return self.status

    async def read_all_celsius(self, delay_ms=750) -> list:
        """
        :param delay_ms: a set delay before the reading is done
        :return: readings for all the sensors on the same channel/pin
        """
        readings = []
        try:
            sensors = self.channel.scan()
            self.channel.convert_temp()
            await asyncio.sleep_ms(delay_ms)
            for sensor in sensors:
                readings.append(self.channel.read_temp(sensor))
        except OneWireError:
            print("TempSensorDS18B20 - Error! Unable to read temp sensor(s): OneWireError")
        return readings


