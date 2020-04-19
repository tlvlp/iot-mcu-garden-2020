import ujson

import uasyncio as asyncio
from modules.exceptions import InvalidModuleInputException
from modules.relay import Relay
from modules.temp_sensor_ds18b20 import TempSensorDS18B20
from mqtt.mqtt_service import MqttMessage
from unit import config, shared_flags


class UnitService:

    def __init__(self, mqtt_service) -> None:
        """
        Unit Service for the tlvlp.iot project
        Handles all unit related events and information

        Tested on ESP32 MCUs
        :param mqtt_service: tlvlp.iot mqtt service instance
        """
        print("Unit service - Initializing service")
        self.mqtt_service = mqtt_service
        # Init hardware
        self.water_temp_sensor = TempSensorDS18B20("waterTemperatureCelsius", config.water_temp_sensor_pin)
        self.growlight_relay = Relay("growlight",
                                     config.growlight_pin,
                                     config.growlight_relay_active_at,
                                     config.growlight_persistence_path)
        # Run scheduled tasks
        loop = asyncio.get_event_loop()
        loop.create_task(self.status_updater_loop())
        loop.create_task(self.incoming_message_processing_loop())
        print("Unit service - Service initialization complete")

    async def send_status_to_server(self) -> None:
        while not shared_flags.wifi_is_connected and not shared_flags.mqtt_is_connected:
            await asyncio.sleep(0)
        status_dict = config.unit_id_dict.copy()
        status_dict.update({'modules': [
            await self.water_temp_sensor.get_first_reading_in_celsius(),
            self.growlight_relay.get_state(),
        ]})
        status_json = ujson.dumps(status_dict)
        message = MqttMessage(config.mqtt_topic_status, status_json)
        await self.mqtt_service.add_outgoing_message_to_queue(message)

    async def status_updater_loop(self) -> None:
        """ Periodically sends a status update to the server """
        while True:
            await self.send_status_to_server()
            await asyncio.sleep(config.post_status_interval_sec)

    async def incoming_message_processing_loop(self) -> None:
        """ Processes the incoming message queue"""
        while True:
            message = await self.mqtt_service.message_queue_incoming.get()
            topic = message.get_topic()
            payload = message.get_payload()
            print("Unit service - Message received from topic:{} with payload: {}".format(topic, payload))
            if topic == config.mqtt_topic_status_request:
                await self.send_status_to_server()
            elif topic == config.mqtt_topic_control:
                await self.handle_control_event(payload)
                await asyncio.sleep(0)
                await self.send_status_to_server()
            else:
                await self.send_error_to_server("Unit service - Error! Unrecognized topic: {}".format(topic))

    async def handle_control_event(self, payload_json: str) -> None:
        """ Processes an incoming control message """
        try:
            modules = ujson.loads(payload_json)
            if modules is None:
                await self.send_error_to_server("Unit service - Error parsing payload!")

            any_module_matched = False
            for module_json in modules:
                if self.module_matches(module_json, self.growlight_relay):
                    self.growlight_relay.handle_control_message(module_json.get('value'))
                    any_module_matched = True
            if not any_module_matched:
                await self.send_error_to_server("Unit service - Error! Unrecognized module: {}".format(payload_json))

        except ValueError:
            await self.send_error_to_server("Unit service - Error! Invalid payload: {}".format(payload_json))
        except InvalidModuleInputException:
            await self.send_error_to_server(
                "Unit service - Error! Invalid value in control payload: {}".format(payload_json))

    @staticmethod
    def module_matches(module_json, module):
        return module_json['type'] == module.status.get('type') \
               and module_json['name'] == module.status.get('name')

    async def send_error_to_server(self, error: str) -> None:
        while not shared_flags.wifi_is_connected and not shared_flags.mqtt_is_connected:
            await asyncio.sleep(0)
        error_dict = config.unit_id_dict.copy()
        error_dict.update({
            "error": error
        })
        error_json = ujson.dumps(error_dict)
        message = MqttMessage(config.mqtt_topic_error, error_json)
        await self.mqtt_service.add_outgoing_message_to_queue(message)
        print(error)
























