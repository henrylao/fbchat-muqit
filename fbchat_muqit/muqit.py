"""Facebook Messenger uses Mqtt protocol to send and receive messages. This File handles Mqtt broker."""

from __future__ import annotations


import aiomqtt
import asyncio
import aiohttp
import random
import json
import time
import re

from asyncio import Task
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional
from yarl import URL


from .exception.errors import FBChatError

from .graphql import from_doc_id
from .state import State
from .logging.logger import get_logger
from .utils.utils import generate_uuid

logger = get_logger()


def generate_session_id() -> int:
    return random.randint(1, 2**53)


def get_cookie_header(session: aiohttp.ClientSession, url: str) -> str:
    return (
        session.cookie_jar.filter_cookies(URL(url)).output(header="", sep=";").lstrip()
    )


def get_random_reconnect_time() -> int:
    """Generate random reconnection time between 26-60 minutes"""
    min_time = 26 * 60 * 1000  # 26 minutes in milliseconds
    max_time = 60 * 60 * 1000  # 60 minutes in milliseconds
    return random.randint(min_time, max_time)


@dataclass(slots=True)
class Mqtt:
    _state: State = field()
    _mqttClient: aiomqtt.Client = field()
    _chat_on: bool = field()
    _foreground: bool = field()
    _sequence_id: int = field()

    _mqttClientID: str = field()
    _mqttAppID: str = field()
    _region: str = field()
    _sync_token: Any = field(default=None)

    _update_presence: bool = field(default=True)
    _auto_reconnect: bool = field(default=True)
    _presence_task: Optional[Task] = field(default=None)
    _reconnect_task: Optional[Task] = field(default=None)
    _listen_task: Optional[Task] = field(default=None)
    _running: bool = field(default=False)
    _reconnecting: bool = field(default=False)

    _message_handler: Optional[Callable] = field(default=None)

    HOST = "edge-chat.facebook.com"  # Mqtt host for facebook
    iris_re = re.compile(rb'"irisSeqId":\s*"(\d+)"')
    first_re = re.compile(rb'"firstDeltaSeqId":\s*(\d+)')
    last_re = re.compile(rb'"lastIssuedSeqId":\s*(\d+)')
    tq_re = re.compile(rb'"tqSeqId":\s*"(\d+)"')
    sync_re = re.compile(rb'"syncToken":\s*"([^"]+)"')

    @classmethod
    async def connect(
        cls,
        state: State,
        chat_on: bool,
        foreground: bool,
        message_handler: Callable,
        update_presence: bool = True,
        auto_reconnect: bool = True,
    ) -> Mqtt:

        # configuring mqtt client
        mqttClint = aiomqtt.Client(
            hostname=cls.HOST,
            identifier="mqttwsclient",
            clean_session=True,
            protocol=aiomqtt.ProtocolVersion.V31,
            transport="websockets",
            port=443,
            keepalive=60,
        )

        sequence_id = await cls._fetch_sequence_id(state)

        # creating `Mqtt` class instance for handlling mqtt connections
        self = cls(
            _state=state,
            _mqttClient=mqttClint,
            _chat_on=chat_on,
            _foreground=foreground,
            _sequence_id=sequence_id,
            _mqttClientID=state._mqttClientID,
            _mqttAppID=state._mqttAppID,
            _region=state._region,
            _message_handler=message_handler,
            _update_presence=update_presence,
            _auto_reconnect=auto_reconnect,
        )

        self._mqttClient._client.tls_set()
        self._configure_mqtt_options()

        # connect the mqtt client
        await self._mqttClient.__aenter__()

        if self._mqttClient._client.is_connected():
            logger.info("🔨 Successfully connected to MQTT broker")
            await self._messenger_queue_publish()
            self._running = True

            # run them in the background
            if self._update_presence:
                self._presence_task = asyncio.create_task(self._presence_updater())
            if self._auto_reconnect:
                self._reconnect_task = asyncio.create_task(self._schedule_reconnect())

            self._listen_task = asyncio.create_task(self.listen())

        return self

    @staticmethod
    async def _fetch_sequence_id(state) -> int:
        """Fetch sequence ID."""
        params = {
            "limit": 1,
            "tags": ["INBOX"],
            "before": None,
            "includeDeliveryReceipts": False,
            "includeSeqID": True,
        }
        j = await state._graphql_requests(from_doc_id("1349387578499440", params))
        sequence_id = j[0]["viewer"]["message_threads"]["sync_sequence_id"]  # type: ignore
        logger.debug(f"fetched sequence id: {sequence_id}")
        return int(sequence_id)

    def _configure_mqtt_options(self) -> None:
        session_id = generate_session_id()
        region = self._region
        mqttClientID = self._mqttClientID
        mqttAppID = self._mqttAppID

        # The topics fbchat-muqit will listen to
        # with open("./fbchat_muqit/topics.json", "r") as f:
        #     data = json.loads(f.read())
        # topics = [i for i in data]
        # print(*topics)
        topics = [
            "/legacy_web",  # web related messages during graphql requests
            "/ls_req",  # used to publish message payloads
            "/ls_resp",  # receives response after pub to /ls_req
            "/t_ms",  # all kinds of message, message events received
            "/rtc_multi",
            # "/t_rtc_multi",  # receives group calls
            "/thread_typing",  # typing status update received
            "/orca_typing_notifications",  # Messenger notifi.
            "/orca_presence",  # receives users presence updates
            "/br_sr",
            "/friend_request",  # receives friend request notification
            "/friending_state_change",  # when friend request confirmed/removed
            "/friend_requests_seen",  # new friend request seen
            "/sr_res",
            "/webrtc",
            "/onevc",
            "/notify_disconnect",
            "/mercury",
            "/inbox",
            "/messaging_events",
            "/orca_message_notifications",
            "/pp",
            "/webrtc_response",
        ]
        username = {
            "u": self._state.user_id,
            "s": session_id,
            "chat_on": self._chat_on,
            "fg": self._foreground,
            "d": mqttClientID,
            "aid": mqttAppID,
            "st": topics,
            "pm": [],
            "cp": 3,
            "ecp": 10,
            "ct": "websocket",
            "mqtt_sid": "",
            "dc": "",
            "no_auto_fg": True,
            "gas": None,
            "pack": [],
            "p": None,
            "aids": None,
            "a": self._state._userAgent,
        }
        self._mqttClient._client.username_pw_set(json.dumps(username))

        headers = {
            "Cookie": get_cookie_header(
                self._state._session, "https://edge-chat.facebook.com/chat"
            ),
            "User-Agent": self._state._userAgent,
            "Origin": "https://www.facebook.com",
            "Host": self.HOST,
        }

        self._mqttClient._client.ws_set_options(
            path=f"/chat?region={region}&sid={session_id}&cid={mqttClientID}",
            headers=headers,
        )

    async def _messenger_queue_publish(self) -> None:
        # configure receiving messages.
        payload = {
            "sync_api_version": 10,
            "max_deltas_able_to_process": 1000,
            "delta_batch_size": 500,
            "encoding": "JSON",
            "entity_fbid": self._state.user_id,
        }
        if self._sync_token is None:
            topic = "/messenger_sync_create_queue"
            payload["initial_titan_sequence_id"] = str(self._sequence_id)
            payload["device_params"] = None
        else:
            topic = "/messenger_sync_get_diffs"
            payload["last_seq_id"] = str(self._sequence_id)
            payload["sync_token"] = self._sync_token

        await self._mqttClient.publish(topic, json.dumps(payload), qos=1)

    def extract_meta(self, raw):
        """extracts sequence and sync token"""
        ids = {
            "firstDeltaSeqId": (
                int(m.group(1)) if (m := self.first_re.search(raw)) else None
            ),
            "lastIssuedSeqId": (
                int(m.group(1)) if (m := self.last_re.search(raw)) else None
            ),
            "syncToken": (
                m.group(1).decode() if (m := self.sync_re.search(raw)) else None
            ),
        }
        self._sequence_id = (
            ids["lastIssuedSeqId"] or ids["firstDeltaSeqId"] or self._sequence_id
        )
        self._sync_token = ids["syncToken"] or self._sync_token
        if not any(ids.values()):
            logger.debug("No seqId/syncToken found in payload slice")

    async def listen(self):
        if not self._mqttClient:
            raise FBChatError("Mqtt Client is not initialised cannot start listening!")
        logger.info("Starting MQTT listening loop to listen to events")
        try:
            async for message in self._mqttClient.messages:
                if not self._running and not self._reconnecting:
                    break
                try:
                    if b"lastIssuedSeqId" in message.payload or b"firstDeltaSeqId" in message.payload or b"syncToken" in message.payload:  # type: ignore
                        self.extract_meta(message.payload)

                    payload = message.payload  # type: ignore
                    topic = message.topic.value
                    if self._message_handler:
                        await self._message_handler(topic, payload)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(
                        "Failed to process MQTT message payload", exc_info=True
                    )
                    raise FBChatError(
                        "Failed to receive message payloads", original_exception=e
                    ) from e

        except asyncio.CancelledError:
            # normal shutdown, just exit silently
            logger.debug("MQTT listening loop cancelled")
            return
        except Exception as e:
            logger.error(f"MQTT listening loop error: {e}")
            if self._running and self._auto_reconnect:
                logger.info("Attempting to reconnect...")
                await self._reconnect()
        finally:
            logger.info("MQTT listening loop ended")

    def parse_json(self, data):
        try:
            return json.loads(data)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message payload as JSON: {e}")
            return {"raw_data": data}  # Return raw data instead of raising
        except Exception as e:
            logger.error(f"Unexpected error parsing JSON: {e}")
            return {"error": str(e), "raw_data": data}

    async def set_foreground(self, value):
        payload = json.dumps({"foreground": value})
        await self._mqttClient.publish("/foreground_state", payload=payload, qos=1)
        self._foreground = value

    async def set_chat_on(self, value):
        data = {"make_user_available_when_in_foreground": value}
        payload = json.dumps(data)
        await self._mqttClient.publish("/set_client_settings", payload=payload, qos=1)
        self._chat_on = value

    def _generate_presence(self) -> Dict[str, Any]:
        """Generate presence payload"""
        return {
            "user_id": self._state.user_id,
            "last_active": int(time.time() * 1000),
            "active": True,
        }

    async def _presence_updater(self):
        """Periodically update presence status"""
        while self._running:
            try:
                if self._mqttClient._client.is_connected():
                    presence_payload = self._generate_presence()
                    await self._mqttClient.publish(
                        "/orca_presence", json.dumps({"p": presence_payload}), qos=1
                    )
                    logger.debug("Presence updated")
                await asyncio.sleep(50)  # Update every 50 seconds

            except asyncio.CancelledError:
                logger.debug("Presence updater cancelled")
                return
            except Exception as e:
                logger.error(f"Error updating presence: {e}")
                await asyncio.sleep(10)

    async def _cancel_task(self, task: asyncio.Task | None):
        """Cancel an asyncio task if running, and await its cleanup."""
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def stop(self):
        """Clean disconnect from MQTT"""
        self._running = False  # Stop background tasks
        await self._cancel_task(self._listen_task)
        await self._cancel_task(self._reconnect_task)
        await self._cancel_task(self._presence_task)
        # Disconnect MQTT client
        if self._mqttClient:
            try:
                await self._mqttClient.__aexit__(None, None, None)
                logger.info("Disconnected from MQTT")
            except Exception as e:
                logger.error(f"Error disconnecting: {e}")

    ###### mqtt reconnection Scheduled ######
    async def _reconnect(self):
        """Perform reconnection"""
        self._reconnecting = True

        await self._cancel_task(self._listen_task)
        await self._cancel_task(self._presence_task)
        # Disconnect current client
        if self._mqttClient:
            await self._mqttClient.__aexit__(None, None, None)

        await asyncio.sleep(2)

        self._mqttClientID = self._state._mqttClientID = generate_uuid()
        logger.debug(f"Generated new MQTT client ID: {self._mqttClientID}")
        self._sequence_id = await self._fetch_sequence_id(self._state)
        logger.debug(f"Fetched new sequence id: {self._sequence_id}")

        self._mqttClient = aiomqtt.Client(
            hostname=self.HOST,
            identifier="mqttwsclient",
            clean_session=True,
            protocol=aiomqtt.ProtocolVersion.V31,
            transport="websockets",
            port=443,
            keepalive=60,
        )

        # Reconnect with new configuration
        self._mqttClient._client.tls_set()
        self._configure_mqtt_options()
        await self._mqttClient.__aenter__()

        if self._mqttClient._client.is_connected():
            logger.info("✅ MQTT reconnected successfully")
            self._sync_token = None

            logger.debug("Publishing messengee queue...")
            await self._messenger_queue_publish()
            logger.debug("Publishing messengee queue...")

            self._listen_task = asyncio.create_task(self.listen())
            self._presence_task = asyncio.create_task(self._presence_updater())
            logger.info("✅ Reconnected and restarted listen/presence loops")
        self._reconnecting = False

    async def _schedule_reconnect(self):
        """Schedule periodic reconnections with random intervals"""
        while self._running:
            reconnect_time = get_random_reconnect_time() / 1000  # Convert to seconds
            logger.info(f"Scheduled reconnect in {int(reconnect_time / 60)} minutes...")
            await asyncio.sleep(reconnect_time)
            logger.info("Reconnecting MQTT with new clientID...")
            try:
                await self._reconnect()
            except Exception as e:
                logger.error(f"Reconnection failed: {e}")
