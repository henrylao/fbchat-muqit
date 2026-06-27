#fbchat_muqit/messenger/client.py 
"""
Messenger Client used to interact and perform Messenger related actions.
"""
import uuid
import json
import asyncio
from typing import Dict, Optional, List, Tuple



from ..graphql import GraphQLProcessor, QueryRequest
from ..models.deltas.parser import MessageParser, Message
from ..models.themes import Theme
from ..models.user import User, parse_user_graphql 

from ..state import State
from ..muqit import Mqtt
from ..models.message import Mention, Mentions
from ..logging.logger import FBChatLogger, get_logger 
from ..exception.errors import APIError, FBChatError, FacebookAPIError, LoginError, ParsingError, ValidationError, handle_exceptions
from ..utils.utils import generate_offline_threading_id, now
from ..models.thread import Thread, ThreadFolder, ThreadType, parse_thread_info
from ..models.mqtt_response.send_message import extract_message_id_raw
from ..models.mqtt_response.search_message import parse_message_search 
from ..models.mqtt_response.create_group_thread import extract_thread_id_raw
from ..models.mqtt_response.response import LSResp

class MessengerClient:
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._state: Optional[State] = None
        self._uid: str = ""
        self._name: str = ""
        self.logger: FBChatLogger = get_logger()
        self._mqtt: Optional[Mqtt] = None
        self._graphql = GraphQLProcessor()
        self._parser = MessageParser(self.logger)
        self._req_count = 0
        self._task_count = 0
        self._variance: float = 0 


        self._pending_requests: dict[int, asyncio.Future[LSResp]] = {}

    def _get_task_id(self):
        task_count = self._task_count
        self._task_count += 1
        return task_count
    
    def _get_request_id(self):
        req_count = self._req_count
        self._req_count += 1 
        return req_count



    @handle_exceptions(FacebookAPIError)
    async def uploadFiles(self, file_path: Optional[List[str]] = None, file_url: Optional[List[str]] = None, voice_clip = True, full_data = False)-> List[int]:
        """Upload files to facebook and get their Ids.

        Args:
            file_path (List[str]): Local paths of files.
            file_url (List[str]): Urls of remote files.
            voice_clip (bool): Wether the file is an audio or not (default: `True`). No need to change it. 
            full_data (bool): If you want full data of the uploaded files set it `True` by deafult it is `False`

        Returns:
            List[int]: A list of Ids of the uploaded files.
        """
        if not self._state:
            raise FBChatError("State not initialized")
        if file_path:
            async with self._state.get_files_from_paths(file_path) as f:
                files_ids = await self._state._upload(f, voice_clip, full_data)
        elif file_url:
            file_data = await self._state.get_files_from_urls(file_url)
            files_ids = await self._state._upload(file_data)
        else:
            raise APIError("'file_path' or 'file_url' must be provided.")
        return files_ids

    async def _send_request(self, payload: dict, timeout: float = 10.0)->LSResp:
        request_id = payload["request_id"]
        fut = asyncio.get_event_loop().create_future()
        self._pending_requests[request_id] = fut

        if self._mqtt:
            await self._mqtt._mqttClient.publish("/ls_req", json.dumps(payload), qos=1)
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            self._pending_requests.pop(request_id, None)
            raise TimeoutError("Request timed out, did not receive a response")


    async def fetch_thread_info(self, thread_ids: List[str])->Tuple[Thread,...]:
        """
        Fetches multiple thread information by their thread Ids.

        Args: 
            thread_ids (List[str]): List of thread Id to fetch threads info.

        Returns:
            Tuple[Thread]: Returns A Tuple of `Thread` objects containing thread info. 
        """
        queries = (
                QueryRequest(
                    doc_id="3449967031715030",
                    query_params={
                        "id": thread_id,
                        "message_limit": 0,
                        "load_messages": False,
                        "load_read_receipts": False,
                "before": before,
                    }) for thread_id in thread_ids
                )
        data = {
                "queries": self._graphql.queries_to_json(*queries),
                "batch_name": "MessengerGraphQLThreadFetcher"
                }

        if self._state:
            try:
                result = await self._state._post("https://www.facebook.com/api/graphqlbatch/", data=data, as_graphql=True)
                return parse_thread_info(result)

            except Exception as e:
                raise FBChatError("Failed to fetch thread", original_exception=e)
        else:
            raise LoginError("Client is not logged in yet. `State` class is not initialised yet")

    async def fetch_thread_list(self, limit: int = 5, thread_folder: ThreadFolder = ThreadFolder.INBOX, before: Optional[int] = None)->Tuple[Thread,...]:
        """Fetch all threads from INBOX, ARCHIVE etc.

        Args:
            limit (int): The amount of Threads to fetch from the thread folder.
            thread_folder (str): The location of the threads such as INBOX, ARCHIVE, PENDING etc.
            before: A timstamp to fetch threads from. 
        Returns:
            Tuple[Thread, ...]: A Tuple of `Thread` objects is returned cointaining fetched thread information.
        """
        query = QueryRequest(
                doc_id="3426149104143726",
                query_params={
                    "limit": limit,
                    "tags": thread_folder.value,
                    "before": before,
                    "includeDeliveryReceipts": True,
                    "includeSeqID": False
                    }
                )
        data = {
                "queries": self._graphql.queries_to_json(query),
                "batch_name": "MessengerGraphQLThreadlistFetcher"
                }
        
        if self._state:
            try:
                result = await self._state._post("https://www.facebook.com/api/graphqlbatch/", data=data, as_graphql=True)
                return parse_thread_info(result[0]["viewer"]["message_threads"]["nodes"])
            except Exception as e:
                raise FBChatError("Failed to fetch thread folder", original_exception=e)
        else:
            raise LoginError("Client is not logged in yet. `State` class is not initialised yet")



    async def fetch_thread_messages(self, thread_id: str, message_limit: int = 5, before: Optional[int] = None)-> List[Message] | None:
        """Fetch messages from a Thread using The Thread's Id.

        Args:
            thread_id (str): The Thread Id of the Thread to fetch messages from.
            message_limit (int): The amout of messages to fetch. (`message_limit=10` by default)
            before (int): Fetch messages before a specific timestamp (Time in miliseceonds). 

        Returns:
            List[Message]: If succesfull returns a list of ``Message`` objects.
        """

        query = QueryRequest(
                doc_id="1860982147341344",
                query_params={
                    "id": thread_id,
                    "message_limit": message_limit,
                    "load_messages": True,
                    "load_read_receipts": True,
                    "before": before,
                    })

        data = {"queries": self._graphql.queries_to_json(query)}
        try: 
            if not self._state:
                raise LoginError("Client is not logged in yet. `State` class is not initialised yet")
            result = await self._state._post("https://www.facebook.com/api/graphqlbatch/", data=data, as_graphql=True)
            try:
                return self._parser.parse_thread_message(result)
            except Exception as e:
                self.logger.error("Failed to parse fetched response for thread messages, failure could be due to changes in response format")
                self.logger.debug("Failed to parse fetched response for thread messages. The response: {result}")
        except Exception as e:
            self.logger.error("Failed to fetch Thread Messages, Skipping it.", exc_info=e)



    async def fetch_all_users(self)-> Dict[str, User]:
        """Fetch All User Threads The client is chatting with.

        Returns:
            Dict[str, User]: Returns a ``dict`` of 
        """
        data = {"viewer": self._uid}
        if not self._state:
            raise LoginError("Client not logged in - `State` is not initialized")
        try:
            result = await self._state._post("/chat/user_info_all", data=data, raw=True)
            return parse_user_graphql(result[result.index(b"{"):])

        except ParsingError as e:
                self.logger.error(str(e))
                raise
        except FBChatError as e:
            raise FBChatError("Failed to fetch all users info", original_exception=e)


    async def fetch_user_info(self, *user_ids)->Dict[str, User]:
        """Fetches Users info using their user Id. 
        
        Args:
            user_ids (str): Pass Any amout of user Ids to fetch their information.

        Returns: 
            Dict[str, User]: Returns a dict cointaining User objects as value and their User Id as key.
        """
        data = {f"ids[{i}]": user_id for i, user_id in enumerate(user_ids)}

        if not self._state:
            raise LoginError("Client is not logged in yet. `State` class is not initialised yet")
        try:
            result = await self._state._post("/chat/user_info/", data=data, raw=True)
            payload = result[result.index(b"{"):]
            # self_parser.decoder.decode() is same as json.loads()
            return parse_user_graphql(self._parser.decoder.decode(payload))
        except ParsingError as e:
                self.logger.error(str(e))
                raise
        except Exception as e:
            raise APIError("Failed to fetch all users info", original_exception=e)


    async def fetch_message_info(self, message_id: str, thread_id: str)-> Message | None:
        """Fetch a specific message's information using the message's ids
        
        Args:
            message_id (str): Id of The message you want to fetch. 
            thread_id (str): The Thread Id where the message was sent. 

        Returns:
            Optional[Message]: Returns a ``Message`` object if succesfully fetches else returns None. 
        """
        query = QueryRequest(
                doc_id="1768656253222505",
                query_params={
                    "thread_and_message_id": {
                        "thread_id": thread_id,
                        "message_id": message_id
                        }
                    }
                )
        data = {"queries": self._graphql.queries_to_json(query)}
        try: 
            if not self._state:
                raise LoginError("Client is not logged in yet. `State` class is not initialised yet")
            result = await self._state._post("https://www.facebook.com/api/graphqlbatch/", data=data, as_graphql=True)

            return self._parser.parse_message_from_graphql(result[0]["message"], thread_id)
        except ParsingError as e:
                self.logger.error(str(e))
                raise
        except Exception as e:
            self.logger.error(f"Failed to fetch message information. {e}")


    async def fetch_thread_themes(self)->List[Theme]:
        """Fetch available themes for Threads.

        Returns:
            List[Theme]: A List of ``Theme`` objects with Theme data is returned.
        """
        data = {
                "fb_api_caller_class": "RelayModern",
                "fb_api_req_friendly_name": "MWPThreadThemeQuery_AllThemesQuery",
                "variables": json.dumps({
                    "version": "default"
                    }),
                "server_timestamps": True,
                "doc_id": "24474714052117636",
                }

        if not self._state:
            raise LoginError("Client is not logged in yet. `State` class is not initialised yet")
        r = await self._state._post("https://www.facebook.com/api/graphql/", data=data, raw=True)
        fetchedThemes = self._parser.themes_decoder.decode(r)
        return fetchedThemes.data.themes



    """ Send Method """
            
    async def send_message(self, text: str | None, thread_id: str, file_path: Optional[List[str]] = None, file_url: Optional[List[str]] = None, sticker: Optional[str] = None, reply_to_message: Optional[str] = None, files_ids: Optional[str] = None, mentions: Optional[List[Mention]] = None)-> str | None:
        """
        Sends message to a messenger thread (User, Group etc.)
        
        Args:
            text (str): The text message you want to send.
            thread_id (str): The thread id of the Thread you want to send message. 
            file_path (Optional[List[str]]): A list of local file paths such as Download/image.jpg, fav/music.mp3, video.mkv  etc. 
            file_url (Optional[List[str]]): A list of file Urls if you want to send remote files.
            sticker (str): The id of the sticker you want to send.
            reply_to_message (str): The message ID you want to reply. 
            mentions (Optional[List[Mention]]): pass a list of `Mention` objects including offset (start index of the name in the text), length (length of the user's name) and the user's Id if you want to mention a user. 

        Returns:
            message_id (str | None): The Id of the sent message.
        """

        otid = generate_offline_threading_id()
        payload = {
            'thread_id': int(thread_id),
            'otid': otid,
            'source': 0,
            'send_type': 1,
            'sync_group': 1,
            'text': text,
            'initiating_source': 1,
            'skip_url_preview_gen': 0,
        }


        if mentions:
            if not isinstance(mentions[0], Mention):
                raise ValidationError("'mentions' must be a list of `Mention` objects")
            payload["mention_data"] = Mentions(mentions)._to_payload()
        if sticker:
            payload['send_type'] = 2
            payload['sticker_id'] = sticker
            payload['text'] = None

        if files_ids:
            payload["send_type"] = 3
            payload["attachment_fbids"] = files_ids

        if file_path:
            payload['send_type'] = 3
            file_ids = await self.uploadFiles(file_path=file_path) # returns list of uploaded id 
            payload['attachment_fbids'] = file_ids # get just uploaded file ids

        elif file_url:
            payload["send_type"] = 3 
            file_ids = await self.uploadFiles(file_url=file_url) 
            payload["attachment_fbids"] = files_ids

        tasks = [
            {
                'label': '46',
                'payload': payload,
                'queue_name': thread_id,
                'task_id': self._get_task_id(),
                'failure_count': None,
            },
            {
                'label': '21',
                'payload': {
                    'thread_id': int(thread_id),
                    'last_read_watermark_ts': now(),
                    'sync_group': 1,
                },
                'queue_name': thread_id,
                'task_id': self._get_task_id(),
                'failure_count': None,
            }
        ]
        
        if reply_to_message:
            tasks[0]['payload']['reply_metadata'] = {
                'reply_source_id': reply_to_message,
                'reply_source_type': 1,
                'reply_type': 0,
            }
        
        form = {
            'app_id': '2220391788200892',
            'payload': {
                'tasks': tasks,
                'epoch_id': generate_offline_threading_id(),
                'version_id': '6120284488008082',
                'data_trace_id': None,
            },
            'request_id': self._get_request_id(),
            'type': 3,
        } 
        for task in form['payload']['tasks']:
            task['payload'] = json.dumps(task['payload'])
        
        form['payload'] = json.dumps(form['payload'])

        response = await self._send_request(form)
        if "Couldn't send" in response.payload:
            self.logger.error("Couldn't send the message. Could it be that you have provided a `User` thread Id?")
            return
        return extract_message_id_raw(response.payload)
            
    async def send_quick_reaction(self, thread_id: str, emoji: str, emojj_size: int = 3):
        """Send a quick reaction emoji to a Thread.
        Args:
            emoji (str): The emoji you want to send.
            emojj_size (int): The size of the emoji (range: 1-3) by default it is 3. 
        """
        otid = generate_offline_threading_id()
        # Normal, large, larger
        if emojj_size > 3:
            emojj_size = 3 
        elif emojj_size < 1:
            emojj_size = 1

        tasks = [
            {
                "failure_count": None,
                "label": "46",  # Send message
                "payload": json.dumps({
                    "thread_id": int(thread_id),
                    "otid": str(otid),
                    "source": 65537,
                    "send_type": 1,  # Normal text/emoji send
                    "sync_group": 1,
                    "mark_thread_read": 1,
                    "text": emoji,
                    "hot_emoji_size": emojj_size,  # Key difference (quick tap)
                    "initiating_source": 1,
                    "skip_url_preview_gen": 0,
                    "text_has_links": 0,
                    "multitab_env": 0,
                }),
                "queue_name": str(thread_id),
                "task_id": self._get_task_id()
            },
            {
                "failure_count": None,
                "label": "21",  # Mark as read
                "payload": json.dumps({
                    "thread_id": int(thread_id),
                    "last_read_watermark_ts": now(),
                    "sync_group": 1
                }),
                "queue_name": str(thread_id),
                "task_id": self._get_task_id()
            }
        ]

        payload = {
            "app_id": "772021112871879",
            "payload": json.dumps({
                "epoch_id": int(generate_offline_threading_id()),
                "tasks": tasks,
                "version_id": "9507618899363250",  # EXACT from capture
                "data_trace_id": None
            }),
            "request_id": self._get_request_id(),
            "type": 3
        }

        if self._mqtt:
            await self._mqtt._mqttClient.publish("/ls_req", json.dumps(payload), qos=1)

    async def send_files(self, thread_id: str, file_ids: List[int]):
        """Send already-uploaded files (images, videos, attachments) by Id to a thread.

        Args:
            thread_id (str): The Id of the Thread to send message. 
            files_ids (List[int]): A list of file Ids. 
        """
        otid = generate_offline_threading_id()

        payload = {
            "app_id": "772021112871879",
            "payload": json.dumps({
                "epoch_id": int(generate_offline_threading_id()),
                "tasks": [
                    {
                        "failure_count": None,
                        "label": "46",  # From captured payload
                        "payload": json.dumps({
                            "thread_id": int(thread_id),
                            "otid": str(otid),
                            "source": 65537,
                            "send_type": 3,  # File send
                            "sync_group": 1,
                            "mark_thread_read": 0,
                            "text": None,
                            "attachment_fbids": file_ids
                        }),
                        "queue_name": str(thread_id),
                        "task_id": self._get_task_id()
                    }
                ],
                "version_id": "9507618899363250",
                "data_trace_id": None
            }),
            "request_id": self._get_request_id(),
            "type": 3
        }

        if self._mqtt:
            await self._mqtt._mqttClient.publish("/ls_req", json.dumps(payload), qos=1)

    async def send_files_from_path(self, thread_id: str, file_paths: List[str]):
        """Send local files to Thread.
        
        Args: 
            thread_id (str): The Thread's Id to send files.
            file_paths (List[str]): A list of local file paths. 
        """
        file_ids = await self.uploadFiles(file_path=file_paths)
        await self.send_files(thread_id, file_ids) 

    async def send_files_from_url(self, thread_id: str, file_urls: List[str]):
        """Send remote files to a Thread using url of the file.

        Args:
            thread_id (str): The Thread Id to send the files to.
            file_urls (List[str]): A List of file urls. 
        """
        file_ids = await self.uploadFiles(file_url=file_urls)
        await self.send_files(thread_id, file_ids)

    async def forward_message(self, message_id: str, forward_thread_id: str):
        """Forwards a message by Its Id.

        Args:
            message_id (str): The Id of the message you want to forward. 
            forward_thread_id (str): The Thread you want to forward the message to. 
        """
        payload = {
            "app_id": "772021112871879",
            "payload": json.dumps({
                "epoch_id": int(generate_offline_threading_id()),
                "tasks": [
                    {
                        "failure_count": None,
                        "label": "46",  # From captured payload
                        "payload": json.dumps({
                            "thread_id": int(forward_thread_id),
                            "otid": generate_offline_threading_id(),
                            "source": 65537,
                            "send_type": 5,  # File send
                            "sync_group": 1,
                            "mark_thread_read": 0,
                            "forwarded_msg_id": message_id,
                            "strip_forwarded_msg_caption": 0,
                            "initiating_source": 1,
                            "text": None,
                        }),
                        "queue_name": str(forward_thread_id),
                        "task_id": self._get_task_id()
                    }
                ],
                "version_id": "24628521740133582"
            }),
            "request_id": self._get_request_id(),
            "type": 3
        }

        if self._mqtt:
            await self._mqtt._mqttClient.publish("/ls_req", json.dumps(payload), qos=1)
    async def unsend(self, message_id: str, thread_id: str):
        """Unsent a message for everyone which is sent by the Client by its message id.

        Args:
            message_id (str): Id of the message you want to unsend.
            thread_id (str): The Id of the Thread where message was sent.
        """
        form = {
          "app_id": "772021112871879",
          "payload": json.dumps({
            "epoch_id": int(generate_offline_threading_id()),
            "tasks": [
                {
                    "label": "33",
                    "payload": json.dumps({
                        "message_id": message_id,
                        "thread_key": int(thread_id),
                        "sync_group": 1
                        }),
            "queue_name": "unsend_message",
            "task_id": self._get_task_id()
            }
                ],
            "version_id": "24959613840289226"
        }),
        "request_id": self._get_request_id(),
        "type": 3
        }
        if self._mqtt:
            await self._mqtt._mqttClient.publish('/ls_req', json.dumps(form), qos=1)


    async def react(self, reaction: str, message_id: str, thread_id: str):
        """React to message using The message's id 

        Args:
            reaction (str): The reaction you want to react with. Any available emoji. 
            message_id (str): Id of the message to react.
            thread_id (str): Thread Id of the message.
        """
        payload = {
            "app_id": "772021112871879",
            "payload": json.dumps({
                "epoch_id": int(generate_offline_threading_id()),
                "tasks": [
                {
                    "failure_count": None,
                    "label": "29",
                    "payload": json.dumps({
                        "thread_key": int(thread_id),
                        "timestamp_ms": now(),
                        "message_id": message_id,
                        "actor_id": self._uid,
                        "reaction": reaction,
                        "reaction_style": None,
                        "sync_group": 1,
                        "send_attribution": 65537,
                        "dataclass_params": None,
                        "attachment_fbid": None
                        }),
                    "queue_name": json.dumps(["reaction", message_id]),
                    "task_id": self._get_task_id()
                }
                    ],
                "version_id": "25137701082502211"
                }),
            "request_id": self._get_request_id(),
            "type": 3
            }
        if self._mqtt:
            await self._mqtt._mqttClient.publish("/ls_req", json.dumps(payload), qos=1)


   
    async def search_message(self, text: str, thread_id: str, thread_type: ThreadType = ThreadType.GROUP)-> Dict:
        """Search for a specific text message in a Thread 

        Args:
            text (str): The text message to search. 
            thread_id (str): The Thread where you want to search the text. 
            thread_type (ThreadType): The type of the Thread.

        Returns:
            Dict: A Dict containing The text message matched search results. 
        """
        payload = {
            "app_id": "772021112871879",
            "payload": json.dumps({
                "epoch_id": generate_offline_threading_id(),
                "tasks": [
                    {
                    "failure_count": None,
                    "label": "107",
                    "payload": json.dumps({
                        "query": text,  # the text to search
                        "type": thread_type.value,
                        "thread_key": int(thread_id), # id of the thread
                        "next_page_cursor": None
                        }),
                    "queue_name": "message_search",
                    "task_id": self._get_task_id() # gets incremented task id
                    }
                        ],
                "version_id": "24628521740133582"
                }),
            "request_id": self._get_request_id(), # gets incremented request id
            "type": 3
        }
        response = await self._send_request(payload=payload)
        return parse_message_search(response.payload)


    async def pin_message(self, thread_id: str, message_id: str, pin: bool = True):
        """Pin or unpin a message in a thread.
        
        Args:
            thread_id (str): The Thread where to pin the message. 
            message_id (str): The Id of the message you want to pin. The message must be in the same Thread as The passed Thread Id. 
            pin (bool): Pins the message the if True and unpins if the value is False.
        """
        payload = {
            "app_id": "772021112871879",
            "payload": json.dumps({
                "epoch_id": int(generate_offline_threading_id()),
                "tasks": [
                    {
                        "failure_count": None,
                        "label": "751",  # EXACT from captured payload
                        "payload": json.dumps({
                            "thread_key": int(thread_id),
                            "message_id": message_id,
                            "pinned_message_state": 1 if pin else 0
                        }),
                        "queue_name": "set_pinned_message_search",
                        "task_id": self._get_task_id()
                    }
                ],
                "version_id": "9507618899363250"  
            }),
            "request_id": self._get_request_id(),
            "type": 3
        }

        if self._mqtt:
            await self._mqtt._mqttClient.publish("/ls_req", json.dumps(payload), qos=1)
    async def mark_as_read(self, thread_id: str):
        """Mark a Thread As Read. 

        Args:
            thread_id (str): The Thread Id of Thread to mark as read.
        """
        payload = {
            "app_id": "772021112871879",
            "payload": json.dumps({
                "epoch_id": int(generate_offline_threading_id()),
                "tasks": [
                    {
                        "label": "21",
                        "task_id": self._get_task_id(),
                        "payload": json.dumps({
                            "thread_id": int(thread_id),
                            "last_read_watermark_ts": now() + 5000,
                            "sync_group": 1
                            }),
                        "queue_name": thread_id
                    },
                ],
                "version_id": "24279165305039531"
                        }),
            "request_id": self._get_request_id(),
            "type": 3
         }
        
        if self._mqtt:
            await self._mqtt._mqttClient.publish("/ls_req", json.dumps(payload), qos=1)


    async def mark_as_unread(self, thread_id: str):
        """Mark a Thread As Unread. 

        Args:
            thread_id (str): The Thread Id of Thread to mark as unread.
        """
        payload = {
          "app_id": "772021112871879",
          "payload": json.dumps({
            "epoch_id": int(generate_offline_threading_id()),
            "tasks": [
              {
                "label": "49",
                "payload": json.dumps({
                  "thread_key": int(thread_id),
                  "last_read_watermark_timestamp_ms": now(),
                  "sync_group": 1
                    }),
                "queue_name": str(thread_id),
                "task_id": self._get_task_id()
                }
            ],
            "version_id": "24959613840289226"
        }),
        "request_id": self._get_request_id(),
        "type": 3
        }
        

        if self._mqtt:
            await self._mqtt._mqttClient.publish("/ls_req", json.dumps(payload), qos=1)


    async def typing(self, thread_id: str, is_typing: bool, thread_type: ThreadType = ThreadType.GROUP):
        """Starts/Stops typing indicator.

        Args:
            thread_id (str): The Thread to show typing indicator. 
            is_typing (bool): Shows Client Typing if value is `True` else stops showing typing indicator.
            thread_type (ThreadType): Type of the Thread. 
        """
        payload = {
            "app_id": "2220391788200892",
            "payload": json.dumps({
                "label": "3",
                "payload": json.dumps({
                    "thread_key": int(thread_id),
                    "is_group_thread": 0 if thread_type == ThreadType.USER else 1,
                    "is_typing": 1 if is_typing else 0,
                    "attribution": 0,
                    "sync_group": 1,
                    "thread_type": thread_type.value
                    }),
                "version": "5849951561777440"
                }),
            "request_id": self._get_request_id(),
            "type": 4
            }

        if self._mqtt:
            await self._mqtt._mqttClient.publish("/ls_req", json.dumps(payload), qos=1)

    async def create_group_thread(self, participant_ids: List[str], emoji_sticker: str = "369239263222822")-> Optional[str]:
        """Creates a mssengsr Group Chat. 

        Args: 
            participant_ids (List[str]): A list of User Ids to create the group with them. 
            emoji (str): Any emoji is sent to finalise the group creation. 

        Returns:
            Optional[str]: If the group creation is succesfull then it returns the new created Thread's Id. 
        """
        client_thread_key = generate_offline_threading_id()
        # two payloas will be sent 
        payloads = [
                ("153",  json.dumps({
                            "participants": participant_ids,
                            "client_thread_key": generate_offline_threading_id(),
                            "sync_group": 1
                                }) ),
                ("130",  json.dumps({
                        "participants": participant_ids,
                        "send_payload": {
                            "thread_id": client_thread_key,
                            "otid": str(generate_offline_threading_id()),
                            "source": 65537,
                            "send_type": 2,
                            "sync_group": 1,
                            "mark_thread_read": 0,
                            "sticker_id": emoji_sticker,
                            "hot_emoji_size": 1,
                            "initiating_source": 1,
                            "skip_url_preview_gen": 0,
                            "text_has_links": 0,
                            "multitab_env": 0, },
                        "thread_metadata": None
                        }) )
                ]
        
        for label, p in payloads:
            payload = {
                "app_id": "772021112871879",
                "payload": json.dumps({
                    "epoch_id": int(generate_offline_threading_id()),
                    "tasks": [ 
                        {
                            "failure_count": None,
                            "label": label,
                            "payload": p,
                            "queue_name": str(client_thread_key),
                            "task_id": self._get_task_id()
                        }
                    ],
                    "version_id": "25137701082502211"
                }),
                "request_id": self._get_request_id(),
                "type": 3
                }

            if self._mqtt and label == "153":
                await self._mqtt._mqttClient.publish("/ls_req", json.dumps(payload), qos=1)
                continue

            result = await self._send_request(payload)
            new_thread_id = extract_thread_id_raw(result.payload)
            return new_thread_id



    async def change_thread_approval(self, thread_id: str, enabled: bool):
        """Toggle (on/off) group join approval mode.
        
        Args:
            thread_id (str): The Thread to perform the action.
            enabled (bool): Enables join approval mode if True else disables.
        """
        payload = {
            "app_id": "772021112871879",
            "payload": json.dumps({
                "epoch_id": int(generate_offline_threading_id()),
                "tasks": [
                    {
                        "label": "28",  # from captured payload
                        "payload": json.dumps({
                            "thread_key": int(thread_id),
                            "enabled": 1 if enabled else 0,
                            "sync_group": 1
                        }),
                        "queue_name": "set_needs_admin_approval_for_new_participant",
                        "task_id": self._get_task_id(),
                        "failure_count": None
                    }
                ],
                "version_id": "31712138825101068"
            }),
            "request_id": self._get_request_id(),
            "type": 3
        }
        if self._mqtt:
            await self._mqtt._mqttClient.publish("/ls_req", json.dumps(payload), qos=1)

    async def change_thread_message_share(self, thread_id: str, enabled: bool):
        """Toggle (on/off) message sharing persmission of a Thread.

        Args:
            thread_id (str): The Id of the Thread to perform the action.
            enabled (bool): Enables message sharing persmission of a Thread if Value is True else disables if the value is False.
        """
        payload = {
            "app_id": "772021112871879",
            "payload": json.dumps({
                "epoch_id": int(generate_offline_threading_id()),
                "tasks": [
                    {
                        "label": "210002",
                        "payload": json.dumps({
                            "thread_key": int(thread_id),
                            "is_limit_sharing_enabled": 1 if enabled else 0,
                            "sync_group": 1
                        }),
                        "queue_name": "limit_sharing_setting",
                        "task_id": self._get_task_id(),
                        "failure_count": None
                    }
                ],
                "version_id": "31712138825101068"
            }),
            "request_id": self._get_request_id(),
            "type": 3
        }
        if self._mqtt:
            await self._mqtt._mqttClient.publish("/ls_req", json.dumps(payload), qos=1)

    async def change_read_receipts(self, thread_id: str, enabled: bool):
        """Enable or disable read receipts for a group thread.

        Args:
            thread_id (str): The Id of the Thread to perform the action.
            enabled (bool): Enables if value is True else disables read receipts. 
        """
        payload = {
            "app_id": "772021112871879",
            "payload": json.dumps({
                "epoch_id": int(generate_offline_threading_id()),
                "tasks": [
                    {
                        "label": "60003",
                        "payload": json.dumps({
                            "thread_key": int(thread_id),
                            "is_read_receipts_disabled": 1 if enabled else 0,
                            "sync_group": 1
                        }),
                        "queue_name": str(thread_id),
                        "task_id": self._get_task_id(),
                        "failure_count": None
                    }
                ],
                "version_id": "31712138825101068"
            }),
            "request_id": self._get_request_id(),
            "type": 3
        }
        if self._mqtt:
            await self._mqtt._mqttClient.publish("/ls_req", json.dumps(payload), qos=1)

    async def add_participants(self, thread_id: str, user_ids: List[int]):
        """Add users to a messenger group. Only Group admin can perform this action.

        Args:
            thread_id (str): The Thread Id to add participants. 
            user_ids (List[str]): A list of User Ids to add the users to the Group. 
        """
        payload = {
            "app_id": "772021112871879",
            "payload": json.dumps({
                "epoch_id": int(generate_offline_threading_id()),
                "tasks": [
                    {
                        "label": "23",
                        "payload": json.dumps({
                            "thread_key": int(thread_id),
                            "contact_ids": user_ids,
                            "sync_group": 1
                        }),
                        "queue_name": str(thread_id),
                        "task_id": self._get_task_id(),
                        "failure_count": None
                    }
                ],
                "version_id": "31712138825101068"
            }),
            "request_id": self._get_request_id(),
            "type": 3
        }
        if self._mqtt:
            await self._mqtt._mqttClient.publish("/ls_req", json.dumps(payload), qos=1)

    async def remove_participant(self, thread_id: str, user_id: str):
        """Remove a participant from a group. Only Group admin can perform this action.

        Args:
            thread_id (str): From the Thread to remove a participant. 
            user_id (str): The participant's User Id. 
        """
        payload = {
            "app_id": "772021112871879",
            "payload": json.dumps({
                "epoch_id": int(generate_offline_threading_id()),
                "tasks": [
                    {
                        "label": "140",
                        "payload": json.dumps({
                            "thread_id": int(thread_id),
                            "contact_id": int(user_id),
                            "sync_group": 1
                        }),
                        "queue_name": "remove_participant_v2",
                        "task_id": self._get_task_id(),
                        "failure_count": None
                    }
                ],
                "version_id": "31712138825101068"
            }),
            "request_id": self._get_request_id(),
            "type": 3
        }
        if self._mqtt:
            await self._mqtt._mqttClient.publish("/ls_req", json.dumps(payload), qos=1)

    async def set_thread_admin(self, thread_id: str, user_id: str, is_admin: bool):
        """Grant or revoke admin privilege in a group. Only Group admin can perform this action.

        Args:
            thread_id (str): Thread where the action will be performed.
            user_id (str): The user's Id to Grant/revoke the user 's admin privilege.
            is_admin (bool): Grants The User admin privilege if True else revokes The User's admin privilege.
        """
        payload = {
            "app_id": "772021112871879",
            "payload": json.dumps({
                "epoch_id": int(generate_offline_threading_id()),
                "tasks": [
                    {
                        "label": "25",
                        "payload": json.dumps({
                            "thread_key": int(thread_id),
                            "contact_id": int(user_id),
                            "is_admin": 1 if is_admin else 0,
                            "sync_group": 1
                        }),
                        "queue_name": "admin_status",
                        "task_id": self._get_task_id(),
                        "failure_count": None
                    }
                ],
                "version_id": "31712138825101068"
            }),
            "request_id": self._get_request_id(),
            "type": 3
        }
        if self._mqtt:
            await self._mqtt._mqttClient.publish("/ls_req", json.dumps(payload), qos=1)

    async def change_thread_image(self, thread_id: str, image_id: Optional[int] = None, image_path: Optional[str] = None, image_url: Optional[str] = None):
        """Change a Group's group photo.

        Args:
            thread_id (str): The  Thread's Which group photo will be changed.
            image_id (str): If a uploaded image's Id is available you can use that. 
            image_path (str): change group photo from your local image file.
            image_url (str): Use remote photo url to change group photo.
        """
        if image_path:
            image_da = await self.uploadFiles(file_path=[image_path])
            image_id = image_da[0]
        elif image_url:
            image_da = await self.uploadFiles(file_url=[image_url])
            image_id = image_da[0]
        payload = {
            "app_id": "772021112871879",
            "payload": json.dumps({
                "epoch_id": int(generate_offline_threading_id()),
                "tasks": [
                    {
                        "label": "37",
                        "payload": json.dumps({
                            "thread_key": int(thread_id),
                            "image_id": image_id,
                            "sync_group": 1
                        }),
                        "queue_name": "thread_image",
                        "task_id": self._get_task_id(),
                        "failure_count": None
                    }
                ],
                "version_id": "25137701082502211"
            }),
            "request_id": self._get_request_id(),
            "type": 3
        }
        if self._mqtt:
            await self._mqtt._mqttClient.publish("/ls_req", json.dumps(payload), qos=1)

    async def change_thread_name(self, thread_id: str, name: str):
        """Rename a group chat.
        
        Args: 
            thread_id (str): The Thread to perform the action. 
            name (str): The name to change to. 
        """
        payload = {
            "app_id": "772021112871879",
            "payload": json.dumps({
                "epoch_id": int(generate_offline_threading_id()),
                "tasks": [
                    {
                        "label": "32",
                        "payload": json.dumps({
                            "thread_key": int(thread_id),
                            "thread_name": name,
                            "sync_group": 1
                        }),
                        "queue_name": str(thread_id),
                        "task_id": self._get_task_id(),
                        "failure_count": None
                    }
                ],
                "version_id": "25137701082502211"
            }),
            "request_id": self._get_request_id(),
            "type": 3
        }
        if self._mqtt:
            await self._mqtt._mqttClient.publish("/ls_req", json.dumps(payload), qos=1)

    async def change_thread_theme(self, thread_id: str, theme_id: int):
        """Update a Thread's theme using theme Id. You can get available themeids using the ``fetch_thread_themes()`` method.

        Args: 
            thread_id (str): The Id of the Thread to update its theme. 
            theme_id (str): The Id of the theme to change to.
        """
        payload = {
                "app_id": "772021112871879",
                "payload": json.dumps({
                    "epoch_id": int(generate_offline_threading_id()),
                    "tasks": [
                        {
                            "label": "43",
                            "payload": json.dumps({
                                "thread_key": int(thread_id),
                                "theme_fbid": theme_id,
                                "source": None,
                                "sync_group": 1,
                                "payload": None
                            }),
                            "queue_name": "thread_theme",
                            "task_id": self._get_task_id(),
                            "failure_count": None
                        }
                    ],
                    "version_id": "25137701082502211"
                }),
                "request_id": self._get_request_id(),
                "type": 3
            }
        if self._mqtt:
            await self._mqtt._mqttClient.publish("/ls_req", json.dumps(payload), qos=1)

    async def change_thread_emoji(self, thread_id: str, emoji: str):
        """Set a Thread's quick reaction emoji.

        Args:
            thread_id (str): The Thread which quick reaction to change. 
            emoji (str): The emoji to set to. 
        """
        payload = {
            "app_id": "772021112871879",
            "payload": json.dumps({
                "epoch_id": int(generate_offline_threading_id()),
                "tasks": [
                    {
                        "label": "100003",
                        "payload": json.dumps({
                            "thread_key": int(thread_id),
                            "custom_emoji": emoji,
                            "sync_group": 1
                        }),
                        "queue_name": "thread_quick_reaction",
                        "task_id": self._get_task_id(),
                        "failure_count": None
                    }
                ],
                "version_id": "25137701082502211"
            }),
            "request_id": self._get_request_id(),
            "type": 3
        }
        if self._mqtt:
            await self._mqtt._mqttClient.publish("/ls_req", json.dumps(payload), qos=1)


    async def change_nickname(self, thread_id: str, user_id: str, nickname: str):
        """Set or update nickname of a participant in a thread.

        Args:
            thread_id (str): The Thread wheere to perform the action.
            user_id (str): The user's Id to change thd name.
            nickname (str): Ths nickname you want to give.
        """
        payload = {
            "app_id": "772021112871879",
            "payload": json.dumps({
                "epoch_id": int(generate_offline_threading_id()),
                "tasks": [
                    {
                        "failure_count": None,
                        "label": "44",  # EXACT from capture
                        "payload": json.dumps({
                            "thread_key": int(thread_id),
                            "contact_id": int(user_id),
                            "nickname": nickname,
                            "sync_group": 1
                        }),
                        "queue_name": "thread_participant_nickname",
                        "task_id": self._get_task_id()
                    }
                ],
                "version_id": "9507618899363250"  # EXACT
            }),
            "request_id": self._get_request_id(),
            "type": 3
        }

        if self._mqtt:
            await self._mqtt._mqttClient.publish("/ls_req", json.dumps(payload), qos=1)

    async def mute_thread(self, thread_id: str, mute_forever: bool = False, duration_ms: int = -1):
        """
        Mute both messages and calls in a thread.

        Args:
            thread_id (str): The Thread to mute.
            mute_forever (bool): If mute_forever=True, sets mute_expire_time_ms = -1 (permanent mute) and mutes the Thread for infinite time. If mute_forever=False, provide a duration in ms.
            duration_ms (int): For the time to mute a Thread.
        """
        expire_value = -1 if mute_forever else duration_ms

        payload = {
            "app_id": "772021112871879",
            "payload": json.dumps({
                "epoch_id": int(generate_offline_threading_id()),
                "tasks": [
                    {
                        "failure_count": None,
                        "label": "144",  # Mute messages
                        "payload": json.dumps({
                            "thread_key": int(thread_id),
                            "mailbox_type": 0,
                            "mute_expire_time_ms": expire_value,
                            "sync_group": 1
                        }),
                        "queue_name": str(thread_id),
                        "task_id": self._get_task_id()
                    },
                    {
                        "failure_count": None,
                        "label": "229",  # Mute calls
                        "payload": json.dumps({
                            "thread_key": int(thread_id),
                            "mailbox_type": 0,
                            "mute_calls_expire_time_ms": expire_value,
                            "request_id": None,
                            "sync_group": 1
                        }),
                        "queue_name": str(thread_id),
                        "task_id": self._get_task_id()
                    }
                ],
                "version_id": "9507618899363250"
            }),
            "request_id": self._get_request_id(),
            "type": 3
        }

        if self._mqtt:
            await self._mqtt._mqttClient.publish("/ls_req", json.dumps(payload), qos=1)


    async def restrict_user(self, user_id: str, restrict: bool = True):
        """Restrict or unrestrict a user (hide active status and filter messages without blocking other user).

        Args:
            user_id (str): The User you want to restrict.
            restrict (bool): Restricts The User if True else if restrict value is False unrestricts The User.
        """
        internal_uuid = str(uuid.uuid4())  # Generates UUID like in captured payload

        # Step 1: Send restriction command
        payload1 = {
            "app_id": "772021112871879",
            "payload": json.dumps({
                "epoch_id": int(generate_offline_threading_id()),
                "tasks": [
                    {
                        "failure_count": None,
                        "label": "367",  # Restrict user
                        "payload": json.dumps({
                            "restrictee_id": int(user_id),
                            "request_id": internal_uuid,
                            "messenger_restrict_action": 0 if restrict else 1
                        }),
                        "queue_name": "messenger_restrict",
                        "task_id": self._get_task_id()
                    }
                ],
                "version_id": "9507618899363250"
            }),
            "request_id": self._get_request_id(),
            "type": 3
        }
        if self._mqtt:
            # Send restrict command
            await self._mqtt._mqttClient.publish("/ls_req", json.dumps(payload1), qos=1)

        # Step 2: Cleanup pinned thread reference after restriction
        if restrict and self._mqtt:
            payload2 = {
                "app_id": "772021112871879",
                "payload": json.dumps({
                    "epoch_id": int(generate_offline_threading_id()),
                    "tasks": [
                    {
                        "failure_count": None,
                        "label": "810",  # Remove pinned thread after restrict
                        "payload": json.dumps({
                            "thread_key": int(user_id)  # Using restrictee ID same as captured
                        }),
                        "queue_name": "remove_pinned_thread_on_restrict",
                        "task_id": self._get_task_id()
                    }
                ],
                "version_id": "9507618899363250"
            }),
            "request_id": self._get_request_id(),
            "type": 3
        }

            # Send follow-up pinned removal
            await self._mqtt._mqttClient.publish("/ls_req", json.dumps(payload2), qos=1)

    async def accept_friend_request(self, user_id: int):
        """Accept a friend request using the requester User Id. 

        Args:
            user_id (int): The User friend request to accept.   

        """
        payload = {
            "app_id": "2220391788200892",
            "payload": json.dumps({
                "epoch_id": int(generate_offline_threading_id()),
                "tasks": [
                    {
                        "failure_count": None,
                        "label": "207",  # EXACT from capture
                        "payload": json.dumps({
                            "contact_id": user_id,
                        }),
                        "queue_name": "cpq_v2",
                        "task_id": self._get_task_id()
                    }
                ],
                "version_id": "9507618899363250"
            }),
            "request_id": self._get_request_id(),
            "type": 3
        }
        if self._mqtt:
            await self._mqtt._mqttClient.publish("/ls_req", json.dumps(payload), qos=1)

