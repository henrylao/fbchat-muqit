"""
This is the parser that parses all responses received from mqtt websocket.
We use `msgspec` module to parse the json bytes string directly to dataclasses instead of `dict`. And instead of `dataclass` we use `Struct` from `msgspec` module which is similar to dataclass but lighter.
But using msgspec makes code a bit messy as it is not as flexible as dataclass or attrs.

The responses received from `/ls_resp` topic are parsed separately because thoss are request specific. A response from `/ls_resp` topic is only received when we publish a payload request to `/ls_req` topic.
"""

from __future__ import annotations
import logging
import time


from typing import Any, Callable, Dict, Generator, List, Optional, Set, Tuple
from msgspec import Struct, field, json
from msgspec.json import Decoder

from ...events.dispatcher import EventType
from ...logging.logger import FBChatLogger, get_logger
from ...exception.errors import FBChatError, ParsingError
from ..attachment import (
    Attachment,
    AttachmentType,
    PostAttachment,
    SharedAttachment,
    LocationAttachment,
    ExternalAttachment,
    ProfileAttachment,
    ReelAttachment,
    ProductAttachment,
)
from ..thread import getThreadType
from .custom_type import (
    GenieType,
    DecodedPayloadType,
    ReplyIdType,
    PayloadAttachmentType,
    MentionType,
    Value,
    PostId,
)
from .attachments_deltas import (
    extensibleattachment,
    BlobAttachment,
    StickerAttachment,
    Mercury,
)
from ..timestamps import MarkRead, MarkUnread, ReadReceipt, DeliveryReceipt
from .delta_wrapper import (
    DeltaWrapper,
    Presence,
    ClientPayload,
    NewMessageDelta,
    ClientPayloadDelta,
    Typing,
    FirstFetch,
)
from .group_deltas import (
    AdminTextMessage,
    AdminRemoved,
    AdminAdded,
    ApprovalMode,
    ThreadAction,
    ThreadDelete,
    ThreadFolderMove,
    ThreadMuteSettings,
    ThreadName,
    ThreadNickname,
    ThreadEmoji,
    ThreadMagicWord,
    ThreadTheme,
    ThreadMessageSharing,
    ThreadMessagePin,
    ThreadMessageUnPin,
    ParticipantLeft,
    ParticipantsAdded,
    ApprovalQueue,
    AllAdminText,
)

from .payloads import PayloadReplyMessage
from ..themes import ThemeData
from ..message import (
    Mention,
    Message,
    MessageType,
    ThreadFolder,
    ThreadType,
    MessageReaction,
    Reaction,
)
from ..notifications import (
    FacebookNotifications,
    friendRequestList,
    PokeNotification,
    FriendRequestState,
)

logger = get_logger()


class ParsedEvent(Struct, frozen=True, eq=False):
    eventType: EventType
    args: Tuple


def measure_performance(func):
    """
    A decorator to measure the execution time of a function.
    """

    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        execution_time = end_time - start_time
        print(f"Function '{func.__name__}' executed in {execution_time:.6f} seconds.")
        return result

    return wrapper


def unwrap_to_str(obj: Any) -> str:
    """Returns value from deep nested dict"""
    while isinstance(obj, dict) and obj:
        obj = next(iter(obj.values()))
    return obj if isinstance(obj, str) else (str(obj) if obj is not None else "")


def extract_lat_lon(uri: str) -> tuple[Optional[float], Optional[float]]:
    """Optimized lat/lon extraction with early returns"""
    try:
        # Find the last occurrence more efficiently
        marker_pos = uri.rfind("%7C")
        if marker_pos == -1:
            return None, None
        coords_str = uri[marker_pos + 3 :]
        # Find ampersand position
        amp_pos = coords_str.find("&")
        if amp_pos != -1:
            coords_str = coords_str[:amp_pos]
        # Replace and split in one step
        coords_str = coords_str.replace("%2C", ",")
        coords_parts = coords_str.split(",", 1)
        if len(coords_parts) != 2:
            return None, None
        return float(coords_parts[0]), float(coords_parts[1])
    except (ValueError, IndexError):
        return None, None


class MessageParser:
    def __init__(self, logger: FBChatLogger = get_logger()) -> None:
        self.logger: FBChatLogger = logger

        self.decoder = json.Decoder()
        self.delta_decoder = Decoder(
            type=DeltaWrapper, dec_hook=self.extract_value, strict=False
        )
        self.mercury_decoder = Decoder(
            type=Mercury, dec_hook=self.extract_value, strict=False
        )
        self.typing_decoder = Decoder(type=Typing, strict=False)
        self.presence_decoder = Decoder(type=Presence, strict=False)
        self.mention_decoder = Decoder(type=List[Mention], strict=False)
        self.fbnoti_decoder = Decoder(type=FacebookNotifications, strict=False)
        self.thread_message_decoder = Decoder(
            type=List[VMessageThread], strict=False, dec_hook=self.extract_thread_id
        )
        self.thread_message_atchmnt = Decoder(
            type=VMessageAttachment, strict=False, dec_hook=self.extract_value
        )
        self.themes_decoder = Decoder(
            type=ThemeData, strict=False, dec_hook=self.extract_value
        )

        self.AdminTextArray: Set[str] = AllAdminText
        # Parser mapping
        self._parsers: Dict[str, Callable] = {
            AttachmentType.FACEBOOKREEL: self.parse_reel_extensible,
            AttachmentType.FACEBOOKPOST: self.parse_post_extensible,
            AttachmentType.EXTERNALURL: self.parse_external_extensible,
            AttachmentType.FACEBOOKPROFILE: self.parse_profile_extensible,
            AttachmentType.LOCATION: self.parse_location_extensible,
            AttachmentType.FACEBOOKPRODUCT: self.parse_product_extensible,
            "None": self.parse_story_extensible,
        }
        # attachment type to message type
        self._attach_to_message = {
            AttachmentType.IMAGE: MessageType.IMAGE,
            AttachmentType.VIDEO: MessageType.VIDEO,
            AttachmentType.GIF: MessageType.GIF,
            AttachmentType.STICKER: MessageType.STICKER,
            AttachmentType.FILE: MessageType.FILE,
            AttachmentType.AUDIO: MessageType.AUDIO,
            AttachmentType.LOCATION: MessageType.LOCATION,
            AttachmentType.FACEBOOKPOST: MessageType.FACEBOOK_POST,
            AttachmentType.FACEBOOKREEL: MessageType.FACEBOOK_REEL,
            AttachmentType.FACEBOOKPROFILE: MessageType.FACEBOOK_PROFILE,
            AttachmentType.FACEBOOKGAME: MessageType.FACEBOOK_GAME,
            AttachmentType.FACEBOOKPRODUCT: MessageType.FACEBOOK_PRODUCT,
            AttachmentType.FACEBOOKSTORY: MessageType.FACEBOOK_STORY,
            AttachmentType.EXTERNALURL: MessageType.EXTERNAL_URL,
        }
        # received payload type to evrnt type
        self.admin_text_to_event: Dict[str, EventType] = {
            "joinable_group_link_reset": EventType.THREAD_JOINABLE_LINK_RESET,
            "joinable_group_link_mode_change": EventType.THREAD_JOINABLE_MODE_CHANGE,
            "change_thread_nickname": EventType.THREAD_NICKNAME_CHANGE,
            "change_thread_theme": EventType.THREAD_THEME_CHANGE,
            "change_thread_approval_mode": EventType.THREAD_APPROVAL_MODE_CHANGE,
            "change_thread_quick_reaction": EventType.THREAD_EMOJI_CHANGE,
            "change_thread_admins": EventType.ADMIN_ADDED,
            "magic_words": EventType.THREAD_MAGIC_WORDS_CHANGE,
            "limit_sharing": EventType.THREAD_MESSAGE_SHARING_CHANGE,
            "instant_game_dynamic_custom_update": EventType.THREAD_GAME_CHANGE,
            "pin_messages_v2": EventType.MESSAGE_PINNED,
            "unpin_messages_v2": EventType.MESSAGE_UNPINNED,
        }
        # mapping event type to msgspec Decoder
        # to decode received nested json payload
        self.get_decoder: Dict[EventType, Decoder] = {
            EventType.ADMIN_ADDED: Decoder(type=AdminAdded),
            EventType.MESSAGE_PINNED: Decoder(type=ThreadMessagePin),
            EventType.THREAD_MESSAGE_SHARING_CHANGE: Decoder(type=ThreadMessageSharing),
            EventType.THREAD_MAGIC_WORDS_CHANGE: Decoder(type=ThreadMagicWord),
            EventType.THREAD_NICKNAME_CHANGE: Decoder(type=ThreadNickname),
            EventType.THREAD_THEME_CHANGE: Decoder(type=ThreadTheme),
            EventType.THREAD_EMOJI_CHANGE: Decoder(type=ThreadEmoji),
            EventType.ADMIN_ADDED: Decoder(type=AdminAdded),
            EventType.MESSAGE_PINNED: Decoder(type=ThreadMessagePin),
            EventType.MESSAGE_UNPINNED: Decoder(type=ThreadMessageUnPin),
        }

    def extract_value(self, typ: Any, obj: Any):
        """Custom decoder hook for `msgspec`"""
        if typ is Value and isinstance(obj, (str, dict)):
            return Value(unwrap_to_str(obj))

        elif typ is DecodedPayloadType:
            decoded_payload = self.decode_byte_payload(obj)
            if decoded_payload:
                return DecodedPayloadType((decoded_payload,))
            return DecodedPayloadType((obj,))

        elif typ is ReplyIdType and isinstance(obj, dict):
            return ReplyIdType(obj["dataToMessageId"]["id"])

        elif typ is GenieType and isinstance(obj, dict):
            return (
                GenieType(obj["genie_message"]["__typename"])
                if obj["genie_message"]
                else GenieType(None)
            )

        elif typ is MentionType and isinstance(obj, dict):
            if "prng" in obj:
                return MentionType(self.mention_decoder.decode(obj["prng"]))
            return MentionType()

        elif typ is PayloadAttachmentType:
            if isinstance(obj, str):
                return PayloadAttachmentType((self.mercury_decoder.decode(obj),))
        elif typ is PostId:
            return PostId(obj["id"]) if "id" in obj else PostId("")
        return obj

    def decode_byte_payload(
        self, byte_array: List[int]
    ) -> Optional[ClientPayloadDelta]:
        """Decode byte array payload to JSON"""
        b_array = bytes(byte_array)
        try:
            return json.decode(
                b_array,
                type=ClientPayloadDelta,
                dec_hook=self.extract_value,
                strict=False,
            )
        except Exception as e:
            self.logger.debug(
                f"Failed to decode bytes payload array, payload type: {type(byte_array)} errror: {e} payload: {b_array[:100] if len(b_array) > 100 else b_array}"
            )
            raise ParsingError(f"Failed to decode bytes array error: {e}")

    # ExtensibleAttachments are a bit complex

    def parse_post_extensible(self, data) -> PostAttachment:
        story = data.story_attachment
        post = story.target
        return PostAttachment(
            id=data.legacy_attachment_id,
            title=story.title,
            description=story.description or "",
            post_preview=story.media,  # type: ignore
            post_url=story.url or "",
            post=post,
            source=story.source,
        )

    def parse_story_extensible(self, data: extensibleattachment) -> SharedAttachment:
        return SharedAttachment(
            id=data.legacy_attachment_id,
            title=str(data.story_attachment.title),
            description=(
                str(data.story_attachment.description)
                if data.story_attachment.media
                else None
            ),
            media=data.story_attachment.media,
        )

    def parse_reel_extensible(self, data: extensibleattachment) -> ReelAttachment:
        story = data.story_attachment
        return ReelAttachment(
            id=data.legacy_attachment_id,
            url=story.url or "",
            media=story.media,  # type: ignore
            source=story.source or "",
            video_id=story.target.video_id,  # type: ignore
            title=story.title,
            description=story.description or "",
        )

    def parse_profile_extensible(self, data) -> ProfileAttachment:
        return ProfileAttachment(
            id=data.legacy_attachment_id,
            profile_id=data.story_attachment.target.id,
            profile_name=data.story_attachment.target.name,
            profile_url=data.story_attachment.url or "",
            profile_picture=data.story_attachment.target.picture,
            cover_photo=(
                data.story_attachment.target.cover_photo["photo"]["image"]["uri"]
                if "photo" in data.story_attachment.target.cover_photo
                else None
            ),
        )

    def parse_external_extensible(
        self, data: extensibleattachment
    ) -> ExternalAttachment:
        story = data.story_attachment
        return ExternalAttachment(
            id=data.legacy_attachment_id,
            url=story.url or "",
            media=story.media,
            title=story.title,
            description=story.description,
        )

    def parse_location_extensible(
        self, data: extensibleattachment
    ) -> LocationAttachment:
        """Parse location sharing ExtensibleAttachment"""
        story = data.story_attachment
        lat, long = extract_lat_lon(story.media.preview.url)  # type: ignore
        return LocationAttachment(
            id=data.legacy_attachment_id,
            url=story.url or "",
            media=story.media,  # type: ignore
            address=story.description,
            latitude=lat,  # Would need to parse from map URL if needed
            longitude=long,
            is_live=False,
        )

    def parse_product_extensible(self, data: extensibleattachment) -> ProductAttachment:
        return ProductAttachment(
            id=data.legacy_attachment_id,
            product_name=data.story_attachment.title,
            product_price=str(data.story_attachment.description) or "",
            url=str(data.story_attachment.url) or "",
        )

    def parse_attachment(self, mercury) -> Optional[Attachment]:

        # Direct returns for simple cases
        if mercury.sticker_attachment:
            return mercury.sticker_attachment

        elif mercury.blob_attachment:
            return mercury.blob_attachment

        elif mercury.extensible_attachment:
            # get parser by extensible attachment's genie type
            parser = self._parsers.get(mercury.extensible_attachment.genie_attachment)
            if parser:
                return parser(mercury.extensible_attachment)
        return None

    def parse_t_ms(self, payload) -> Generator[Optional[ParsedEvent]]:
        decoded_delta = self.delta_decoder.decode(payload)
        return (self.parse_deltas(d) for d in decoded_delta.deltas)

    only_decode_notification = (
        b"live_poke",
        b"friending_state_change",
        b"jewel_requests_remove_old",
        b"mobile_requests_count",
    )

    def parse_all(self, topic, payload) -> Optional[ParsedEvent]:
        if self.logger.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("Parsing topic %s (%d bytes)", topic, len(payload))
        if (
            topic == "/thread_typing" and b'"type":"typ"' in payload
        ) or topic == "/orca_typing_notifications":
            eventdata = self.typing_decoder.decode(payload)
            return ParsedEvent(EventType.TYPING, (eventdata,))

        elif topic == "/orca_presence" and b'"list_type"' in payload:
            eventdata = self.presence_decoder.decode(payload)
            return ParsedEvent(EventType.PRESENCE, (eventdata,))

        elif topic == "/legacy_web" and filter(
            lambda x: x in payload, self.only_decode_notification
        ):

            eventdata = self.fbnoti_decoder.decode(payload)
            return self.parse_notifications(eventdata)

        elif b'"syncToken":"1"' in payload:
            json.decode(payload, type=FirstFetch)
            return None

        else:
            if self.logger.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug(
                    "Unknown payload delta from topic %r (%d bytes)",
                    topic,
                    len(payload),
                )
            return None

    def parse_message(self, data, replied_to_message=None) -> Message:
        """
        data (NewMessageDelta | PayloadReplyMessage): NewMessageDelta received when It is a normal message and if the message was reply to another message PayloadReplyMessage is received and `replied to` mesaage data is provided to replied_to_message
        replied_to_message (Message): A `Message` object containing replied to message data.
        """
        attachments = None
        if isinstance(data, NewMessageDelta) and data.attachments:
            attachments = [self.parse_attachment(a.mercury) for a in data.attachments]

        elif isinstance(data, PayloadReplyMessage) and data.attachments:
            attachments = [
                self.parse_attachment(a.mercuryJSON[0]) for a in data.attachments
            ]

        message_type = MessageType.TEXT
        first_attachment = next((a for a in (attachments or []) if a is not None), None)
        if first_attachment is not None:
            message_type = self._attach_to_message.get(
                first_attachment.type, MessageType.TEXT
            )

        thread_folder = ThreadFolder.INBOX
        folder_raw = getattr(data.messageMetadata, "folder", None)
        if folder_raw is not None:
            try:
                thread_folder = ThreadFolder(str(folder_raw))
            except (ValueError, TypeError):
                thread_folder = ThreadFolder.INBOX

        participants_count = (
            len(data.participants) if getattr(data, "participants", None) else 0
        )
        thread_type = ThreadType.GROUP if participants_count >= 3 else ThreadType.USER

        return Message(
            id=data.messageMetadata.id,
            text=data.body or "",
            sender_id=str(data.messageMetadata.sender_id),
            message_type=message_type,
            reaction=[],
            mentions=data.mentions,
            thread_id=data.messageMetadata.thread_id,
            thread_type=thread_type,
            thread_folder=thread_folder,
            thread_participants=data.participants,
            attachments=attachments,
            timestamp=int(data.messageMetadata.timestamp),
            can_unsend=(
                True if data.messageMetadata.unsendType == "Can_Unsend" else False
            ),
            unsent=False,
            replied_to_message=replied_to_message,
        )

    def extract_thread_id(self, typ, obj) -> str:
        return Value(obj["thread_fbid"]) or Value(obj["other_user_id"])

    def parse_thread_message(self, payload: dict[str, Any]) -> List[Message]:
        """Parse meessages from fetched graphql Thread Messages"""
        thread_message = self.thread_message_decoder.decode(json.encode(payload))[0]
        thread_id = str(thread_message.message_thread.thread_key)
        thread_type = getThreadType[thread_message.message_thread.thread_type]
        nodes: List[Dict[str, Any]] = thread_message.message_thread.messages.nodes

        messages = [
            self.parse_message_from_graphql(m, thread_id, thread_type) for m in nodes
        ]
        return messages

    def parse_message_from_graphql(
        self, m: dict[str, Any], thread_id: str, thread_type=ThreadType.UNKNOWN
    ) -> Message:
        """Parse a single fetched grapql message"""
        try:
            return Message(
                id=m["message_id"],
                text=m["message"]["text"],
                sender_id=m["message_sender"]["id"],
                thread_id=thread_id,
                thread_type=thread_type,
                message_type=self.get_from_attachment(m),
                timestamp=m["timestamp_precise"],
                can_unsend=(
                    True
                    if m.get("message_unsendability_status") == "can_unsend"
                    else False
                ),
                unsent=True if m["unsent_timestamp_precise"] == "0" else False,
                reaction=[
                    MessageReaction(
                        id="",
                        reaction=r["reaction"],
                        reactor=r["user"]["id"],
                        thread_id=Value(thread_id),
                        reaction_type=Reaction.ADDED,
                        reacted_message_sender=m["message_sender"]["id"],
                        timestamp=None,
                    )
                    for r in m["message_reactions"]
                ],
                mentions=self.parse_mention(m["message"]["ranges"]),
                thread_folder=ThreadFolder.INBOX,
                thread_participants=None,
                attachments=(
                    [
                        self.parse_attachment(
                            self.thread_message_atchmnt.decode(json.encode(m))
                        )
                    ]
                    if self.has_attachment(m)
                    else None
                ),
            )
        except KeyError as e:
            raise ParsingError(
                f"Couldn't get {e.args[0]} data from message with Id ({m['message_id']})"
            )
        except Exception as e:
            raise FBChatError(
                f"Failed to parse message with Id: {m['message_id']}.",
                original_exception=e,
            )

    def get_from_attachment(self, m) -> MessageType:

        if m["blob_attachments"] != []:
            return self._attach_to_message[
                AttachmentType(m["blob_attachments"][0]["__typename"])
            ]
        elif m["sticker"]:
            return MessageType.STICKER
        elif m["extensible_attachment"]:
            return (
                self._attach_to_message[
                    AttachmentType(
                        m["extensible_attachment"]["story_attachment"]["target"][
                            "__typename"
                        ]
                    )
                ]
                if m["extensible_attachment"]["story_attachment"]["target"]
                else MessageType.TEXT
            )
        else:
            return MessageType.TEXT

    def parse_mention(self, ranges) -> List[Mention] | None:
        if ranges != []:
            return [
                Mention(
                    user_id=m["entity"]["id"], offset=m["offset"], length=m["length"]
                )
                for m in ranges
            ]

    def has_attachment(self, m):
        return m["blob_attachments"] or m["sticker"] or (m["blob_attachments"] != [])

    def parse_notifications(self, eventdata):

        if isinstance(eventdata, friendRequestList):
            return ParsedEvent(EventType.FRIEND_REQUEST_LIST_UPDATE, (eventdata,))
        elif isinstance(eventdata, FriendRequestState):
            return ParsedEvent(EventType.FRIEND_REQUEST_CHANGE, (eventdata,))

        elif isinstance(eventdata, PokeNotification):
            return ParsedEvent(EventType.POKE_NOTIFICATION, (eventdata,))

    def parse_deltas(self, deltas) -> Optional[ParsedEvent]:

        try:

            if isinstance(deltas, NewMessageDelta):
                return ParsedEvent(EventType.MESSAGE, (self.parse_message(deltas),))

            elif isinstance(deltas, ClientPayload):
                data: ClientPayloadDelta = deltas.payload[0]
                # Theme update also sometime updates  quick reaction and magic words
                if data.deltas[0].messageReply:
                    etype: EventType = EventType.MESSAGE
                    if data.deltas[0].replyType == 1:
                        etype = EventType.MESSAGE_BUMP
                    replied_to_message = self.parse_message(
                        data.deltas[0].messageReply.repliedToMessage
                    )
                    main_message = self.parse_message(
                        data.deltas[0].messageReply.message, replied_to_message
                    )
                    return ParsedEvent(etype, (main_message,))

                if data.deltas[0].messageReaction:
                    return ParsedEvent(
                        EventType.MESSAGE_REACTION, (data.deltas[0].messageReaction,)
                    )

                elif data.deltas[0].messageUnsend:
                    return ParsedEvent(
                        EventType.MESSAGE_UNSENT, (data.deltas[0].messageUnsend,)
                    )

                elif data.deltas[0].messageRemove:
                    return ParsedEvent(
                        EventType.MESSAGE_REMOVE, (data.deltas[0].messageRemove,)
                    )

                elif data.deltas[0].muteThread:
                    return ParsedEvent(
                        EventType.THREAD_MUTE, (data.deltas[0].muteThread,)
                    )
                elif data.deltas[0].pageNotification:
                    return ParsedEvent(
                        EventType.PAGE_NOTIFICATION, (data.deltas[0].pageNotification,)
                    )

            elif isinstance(deltas, AdminRemoved):
                return ParsedEvent(EventType.ADMIN_REMOVED, (deltas,))

            elif isinstance(deltas, ParticipantsAdded):
                return ParsedEvent(EventType.PARTICIPANT_JOINED, (deltas,))

            elif isinstance(deltas, ParticipantLeft):
                return ParsedEvent(EventType.PARTICIPANT_LEFT, (deltas,))
            elif isinstance(deltas, ApprovalMode):
                return ParsedEvent(EventType.THREAD_APPROVAL_MODE_CHANGE, (deltas,))
            elif isinstance(deltas, ApprovalQueue):
                return ParsedEvent(EventType.THREAD_APPROVAL_QUEUE, (deltas,))

            elif isinstance(deltas, ThreadName):
                return ParsedEvent(EventType.THREAD_NAME_CHANGE, (deltas,))

            elif isinstance(deltas, ReadReceipt):
                return ParsedEvent(EventType.MESSAGE_SEEN, (deltas,))

            elif isinstance(deltas, DeliveryReceipt):
                return ParsedEvent(EventType.MESSAGE_DELIVERED, (deltas,))

            elif isinstance(deltas, MarkRead):
                return ParsedEvent(EventType.MARK_READ, (deltas,))

            elif isinstance(deltas, MarkUnread):
                return ParsedEvent(EventType.MARK_UNREAD, (deltas,))

            elif isinstance(deltas, ThreadAction):
                return ParsedEvent(EventType.THREAD_ACTION, (deltas,))

            elif isinstance(deltas, ThreadFolderMove):
                return ParsedEvent(EventType.THREAD_FOLDER_MOVE, (deltas,))

            elif isinstance(deltas, ThreadDelete):
                return ParsedEvent(EventType.THREAD_DELETE, (deltas,))

            elif isinstance(deltas, ThreadMuteSettings):
                return ParsedEvent(EventType.THREAD_MUTE_SETTINGS, (deltas,))

            elif isinstance(deltas, AdminTextMessage):
                if (
                    b"remove_admin" in bytes(deltas.untypedData)
                    or deltas.type not in self.AdminTextArray
                ):
                    return None
                evenType = self.admin_text_to_event[deltas.type]
                decodedData = self.get_decoder[evenType].decode(deltas.untypedData)
                return ParsedEvent(evenType, (decodedData, deltas.messageMetadata))

        except Exception as e:
            raise ParsingError(
                f"Failed to parse and dispatch delta: {deltas}", original_exception=e
            )


# below class used to parse fetched responses for thread messages
# usually used when you call fetch_thread_messages() from MessengerClinet class
class VNodes(Struct, frozen=True, eq=False):
    nodes: List


class VThreadMessages(Struct, frozen=True, eq=False):
    messages: VNodes
    thread_key: Value
    thread_type: str


class VMessageThread(Struct, frozen=True, eq=False):
    message_thread: VThreadMessages


class VMessageAttachment(Struct, frozen=True, eq=False):
    extensible_attachment: Optional[extensibleattachment] = None
    sticker_attachment: Optional[StickerAttachment] = field(
        name="sticker", default=None
    )
    blob_attachment: Optional[List[BlobAttachment]] = field(
        name="blob_attachments", default=None
    )
