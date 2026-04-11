import asyncio
import inspect
from typing import Awaitable, Callable, Dict, List, Optional
from enum import Enum
from ..models.deltas.delta_wrapper import Typing

from ..exception.errors import FBChatError
from ..logging.logger import FBChatLogger, get_logger
from ..models.message import Message, MessageReaction, MessageUnsend, MessageRemove
from ..models.messagesData import MessageData
from ..models.deltas.group_deltas import (
    AdminAdded,
    AdminRemoved,
    ApprovalMode,
    ApprovalQueue,
    ThreadDelete,
    ThreadName,
    ThreadNickname,
    ThreadTheme,
    ThreadEmoji,
    ThreadMagicWord,
    ThreadMessagePin,
    ThreadMessageUnPin,
    ThreadMessageSharing,
    ThreadMuteSettings,
    ThreadAction,
    ParticipantsAdded,
    ParticipantLeft,
)

from ..models.deltas.payloads import MuteThread, ChangeViwerStatus, PageNotification

from ..models.notifications import FriendRequestState, PokeNotification

from ..models.timestamps import (
    MarkUnread,
    ReadReceipt,
    MarkRead,
    DeliveryReceipt,
)


class EventType(Enum):
    """Enum for all supported event types"""

    ADMIN_ADDED = "admin_added"
    ADMIN_REMOVED = "admin_removed"

    DISCONNECT = "disconnect"
    ERROR = "error"
    LISTENING = "listening"
    PRESENCE = "presence"
    RECONNECT = "reconnect"

    MESSAGE = "message"
    MESSAGE_UNSENT = "message_unsent"
    MESSAGE_REACTION = "message_reaction"
    MESSAGE_REMOVE = "message_remove"
    MESSAGE_SEEN = "message_seen"
    MESSAGE_DELIVERED = "message_delivered"
    MESSAGE_PINNED = "message_pinned"
    MESSAGE_UNPINNED = "message_unpinned"
    MESSAGE_BUMP = "message_bump"
    MARK_READ = "mark_read"
    MARK_UNREAD = "mark_unread"
    TYPING = "typing"
    THREAD_THEME_CHANGE = "theme_change"
    THREAD_EMOJI_CHANGE = "emoji_change"
    THREAD_NICKNAME_CHANGE = "nickname_change"
    THREAD_APPROVAL_MODE_CHANGE = "approval_mode_change"
    THREAD_MAGIC_WORDS_CHANGE = "magic_words_change"
    THREAD_MESSAGE_SHARING_CHANGE = "message_sharing_change"
    THREAD_GAME_CHANGE = "game_update"
    THREAD_JOINABLE_LINK_RESET = "joinable_link_reset"
    THREAD_JOINABLE_MODE_CHANGE = "joinable_mode_change"
    THREAD_APPROVAL_QUEUE = "approval_queue"
    THREAD_NAME_CHANGE = "thread_name_change"
    THREAD_MUTE_SETTINGS = "thread_mute_settings"
    THREAD_MUTE = "thread_mute"
    THREAD_ACTION = "thread_action"
    THREAD_FOLDER_MOVE = "thread_folder_move"
    THREAD_DELETE = "thread_delete"
    VIEWER_STATUS_CHANGE = "viewer_status_change"
    PARTICIPANT_JOINED = "participant_joined"
    PARTICIPANT_LEFT = "participant_left"

    PAGE_NOTIFICATION = "page_notification"
    POKE_NOTIFICATION = "poke_nofification"
    FRIEND_REQUEST_CHANGE = "friend_request_change"
    """Confirmed/ Removed other's friend request"""
    FRIEND_REQUEST_LIST_UPDATE = "friend_request_list_update"
    """friend request list update"""
    UNKNOWN = "unknown"


EventCallback = Callable[..., Awaitable[None]]


class EventDispatcher:
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._name = ""
        self.logger: FBChatLogger = get_logger()
        self._max_concurrent_handlers = 25
        self._semaphore = asyncio.Semaphore(self._max_concurrent_handlers)
        self._event_listeners: Dict[EventType, List[EventCallback]] = {}

    def event(self, event_type: Optional[EventType] = None):
        """Register an event handler.

        Supports ``@client.event`` and ``@client.event(EventType.X)``.
        """

        # Support ``@client.event`` usage where the decorated function is passed
        # as the first argument.
        if callable(event_type) and not isinstance(event_type, EventType):
            func = event_type  # type: ignore[assignment]
            return self.event(None)(func)  # type: ignore[misc]

        if event_type is not None and not isinstance(event_type, EventType):
            raise FBChatError("event_type must be an EventType")

        def decorator(func: EventCallback) -> EventCallback:
            resolved: EventType

            if event_type is None:
                event_name = func.__name__
                if not event_name.startswith("on_"):
                    raise FBChatError(
                        f"Event handler {event_name} must start with 'on_' or be registered with an EventType"
                    )

                event_type_name = event_name[3:]
                try:
                    resolved = EventType(event_type_name.lower())
                except ValueError as e:
                    raise FBChatError(f"Unknown event type: {event_type_name}") from e
            else:
                resolved = event_type

            self.add_listener(resolved, func)
            return func

        return decorator

    def add_listener(self, event_type: EventType, callback: EventCallback) -> None:
        """
        Add an event listener for a specific event.

        Args:
            event_type (EventType): The type of the event you want to add listener for.
            callback (EventCallback): The listener function you want to add. Note, that the function must be ``async``.
        """
        # Add an event listener
        if event_type not in self._event_listeners:
            self._event_listeners[event_type] = []
        self._event_listeners[event_type].append(callback)

    def remove_listener(self, event_type: EventType, callback: EventCallback) -> bool:
        """
        Remove an event listener for a specific event.

        Args:
            event_type (EventType): The type of the event you want to remove listener for.
            callback (EventCallback): The listener function you want to remove.
        """
        # Remove an event listener
        if event_type in self._event_listeners:
            try:
                self._event_listeners[event_type].remove(callback)
                return True
            except ValueError:
                return False
        return False

    async def dispatch(self, event_name: EventType, *args, **kwargs) -> None:
        tasks = []

        listeners = self._event_listeners.get(event_name, [])

        # method-based handler
        method_name = f"on_{event_name.value}"
        if hasattr(self, method_name):
            listeners = list(listeners) + [getattr(self, method_name)]

        for listener in listeners:
            tasks.append(
                asyncio.create_task(
                    self._safe_execute(listener, event_name, *args, **kwargs)
                )
            )

        if tasks:
            await asyncio.gather(*tasks)

    async def _safe_execute(self, listener, event_name, *args, **kwargs):
        async with self._semaphore:
            try:
                if inspect.iscoroutinefunction(listener):
                    await listener(*args, **kwargs)
                else:
                    # sync listener safety
                    await asyncio.to_thread(listener, *args, **kwargs)
            except Exception as e:
                self.logger.error(
                    f"Error in event listener for {event_name}: {e}", exc_info=True
                )

    async def on_listening(self):
        """Called when the client starts listening to events"""
        self.logger.info(f"Client ({self._name}) started listening to Events!")

    async def on_admin_added(self, event_data: AdminAdded, message: MessageData):
        """
        Called when an admin is added to a messenger group.

        Args:
            event_data (AdminsAdded): Receives a ``AdminsAdded`` object contains added admin id.
            message (MessageData): A `MessageData` object contains the message information meta data.
        """
        self.logger.info(
            f"{message.sender_id} has promoted {event_data.aded_admin} to admin in thread {message.thread_id}"
        )

    async def on_admin_removed(self, event_data: AdminRemoved):
        """Called when an admin is removed from admin role.

        Args:
            event_data (AdminRemoved): Receives a ``AdminRemoved`` object.
        """
        self.logger.info(
            f"{event_data.messageMetadata.sender_id} has demoted {event_data.removed_admins} from admin role in Thread ({event_data.messageMetadata.thread_id})"
        )

    async def on_approval_mode_change(self, event_data: ApprovalMode):
        """Called when Approval mode is changed in a group.

        Args:
            event_data (ApprovalMode): Receives a ``ApprovalMode`` object.
        """
        self.logger.info(f"Approval mode is {event_data.mode}")

    async def on_approval_queue(self, event_data: ApprovalQueue):
        """Called when an user requests to join a group or invited by another participant of the group or disapproved a User's join request but approved join request information is not received.

        Args:
            event_data (ApprovalQueue): Receives a ``ApprovalQueue`` object.
        """
        self.logger.info(
            f"{event_data.requester_id} has requested to join the thread {event_data.messageMetadata.thread_id}"
        )

    async def on_message_delivered(self, event_data: DeliveryReceipt):
        """Called when a message is successfully delivered to a thread.

        Args:
            event_data (DeliveryReceipt): Receives a ``DeliveryReceipt`` object with delivery information.
        """
        self.logger.info(
            f"The message {event_data.message_id} is delivered to Thread ({event_data.thread_id})"
        )

    async def on_mark_read(self, event_data: MarkRead):
        """Called when client marks a thread as read

        Args:
            event_data (MarkRead): Receives a ``MarkRead`` object with read information.
        """
        self.logger.info(f"The Thread {event_data.thread_ids} marked as Read.")

    async def on_mark_unread(self, event_data: MarkUnread):
        """Called when client marks a thread as unread

        Args:
            event_data (MarkUnread): Receives a ``MarkUnread`` object with read information.
        """
        self.logger.info(f"The Thread {event_data.thread_ids} marked as Unread.")

    async def on_message_removed(self, event_data: MessageRemove):
        """Called when the client removes a message for only itself
        Args:
            event_data (MessageRemove): A ``MessageRemove`` with additional information.
        """
        self.logger.info(f"Message {event_data.ids} has been removed for only client")

    async def on_message(self, event_data: Message):
        """Called when a message received in messenger

        Args:
            event_data (Message): A ``Message`` object contains the message information.
        """
        self.logger.info(
            f"{event_data.sender_id} has sent a message to thread {event_data.thread_id}"
        )

    async def on_message_unsent(self, event_data: MessageUnsend):
        """Called when a message is unsent by a User.

        Args:
            event_data (MessageUnsend): A ``MessageUnsend`` object with the unsent message information.
        """
        self.logger.info(f"{event_data.sender_id} unsent the message {event_data.id}")

    async def on_message_reaction(self, event_data: MessageReaction):
        """Called when an user reacts to a message

        Args:
            event_data (MessageReaction): Receives a ``MessageReaction`` object.

        """
        self.logger.info(
            f"{event_data.reactor} {f'reacted with {event_data.reaction} to' if not event_data.reaction_type.value else f'removed reaction {event_data.reaction} from'}the message {event_data.id}"
        )

    async def on_message_seen(self, event_data: ReadReceipt):
        """Called when message is seen by an User.

        Args:
            event_data (ReadReceipt): Receives a ``ReadReceipt`` object.
        """

        self.logger.info(
            f"{event_data.user_id} has seen messages in thread {event_data.thread_id}"
        )

    async def on_message_pinned(
        self, event_data: ThreadMessagePin, message: MessageData
    ):
        """Called when a message gets pinned in a thread

        Args:
            event_data (ThreadMessagePin): Receives a ``ThreadMessagePin`` object.
            message (MessageData): A ``MessageData`` object with message data.
        """
        self.logger.info(message.adminText)

    async def on_message_unpinned(
        self, event_data: ThreadMessageUnPin, message: MessageData
    ):
        """Called when a message gets unpinned in a thread

        Args:
            event_data (ThreadMessagePin): Receives a ``ThreadMessagePin`` object.
            message (MessageData): A ``MessageData` object with message data.
        """
        self.logger.info(message.adminText)

    async def on_magic_words_change(
        self, event_data: ThreadMagicWord, message: MessageData
    ):
        """Called when Magic words are added or removed in a group

        Args:
            event_data (UpdatedMagicWords): Receives a ``UpdatedMagicWords`` object.
            message (MessageData): A ``MessageData`` object with message data.
        """
        self.logger.info(
            f"Magic words {f'{event_data.emoji} added to' if int(event_data.new_magic_word_count) else f'{event_data.theme_name} {event_data.emoji} removed from'} thread {message.thread_id}"
        )

    async def on_thread_mute(self, event_data: MuteThread):
        """Called when the client mutes a thread community/group.

        Args:
            event_data (MuteThread): Receives a ``MuteThread`` object.
        """
        self.logger.info(
            f"Thread ({event_data.thread_id}) has muted for {'infinite time' if event_data.mute_until == -1 else event_data.mute_until}"
        )

    async def on_thread_mute_settings(self, event_data: ThreadMuteSettings):
        """Called when the client mutes a Thread.

        Args:
            event_data (ThreadMuteSettings): Receives a ``ThreadMuteSettings`` object.
        """
        self.logger.info(
            f"{event_data.thread_id} has been muted by {event_data.user_id} for {event_data.expire_time} ms"
        )

    async def on_participant_joined(self, event_data: ParticipantsAdded):
        """Called when an user joins a messenger group/community

        Args:
            event_data (ParticipantsAdded): Receives a ``ParticipantsAdded`` object.
        """
        self.logger.info(event_data.messageMetadata.adminText)

    async def on_participant_left(self, event_data: ParticipantLeft):
        """Called when an user leaves or removed from a messenger group/community

        Args:
            event_data (ParticipantLeft): Receives a ``ParticipantLeft`` object.
        """
        self.logger.info(event_data.messageMetadata.adminText)

    async def on_page_notification(self, event_data: PageNotification):
        """Called when a message is sent to a page that Client created.

        Args:
            event_data (PageNotification): Receives a ``PageNotification`` object.
        """
        self.logger.info(
            f"{event_data.sender_id} has a message to the Page ({event_data.page_id}) {event_data.page_name}"
        )

    async def on_typing(self, event_data: Typing):
        """Called When someone starts/stops typing

        Args:
            event_data (Typing): Receives a ``Typing`` object.
        """
        self.logger.info(
            f"{event_data.sender_id} {'is typing' if event_data.state else 'stopped typing'} in thread {event_data.thread_id}"
        )

    async def on_thread_action(self, event_data: ThreadAction):
        """Called when client moves a Thread to archive/inbox.

        Args:
            event_data (ThreadAction): Receives a ``ThreadAction`` object.
        """
        self.logger.info(
            f"Action on Thread: {event_data.thread_id} Action type: {event_data.action}"
        )

    async def on_thread_delete(self, event_data: ThreadDelete):
        """Called when client deletes a Thread.

        Args:
            event_data (ThreadDelete): Receives a ``ThreadDelete`` object.
        """
        self.logger.info(
            f"Client ({event_data.user_id}) has deleted Threads {event_data.thread_ids}"
        )

    async def on_theme_change(self, event_data: ThreadTheme, message: MessageData):
        """Called when a thread's theme is changed.

        Args:
            event_data (ThreadTheme): Receives a ``ThreadTheme`` object.
            message (MessageData): A ``MessageData`` object with message information.
        """
        self.logger.info(
            f"Thread's ({message.thread_id}) theme changed to {event_data.theme_name} and Thread emoji changed to {event_data.theme_emoji}"
        )

    async def on_thread_name_change(self, event_data: ThreadName):
        """Called when a thread's name is changed

        Args:
            event_data (UpdatedThreadName): Receives a ``UpdatedThreadName`` object.
        """

        self.logger.info(
            f"{event_data.messageMetadata.sender_id} has changed thread's ({event_data.messageMetadata.thread_id}) name to {event_data.name}"
        )

    async def on_emoji_change(self, event_data: ThreadEmoji, message: MessageData):
        """Called when a thread's quick reaction emoji is changed.

        Args:
            event_data (UpdatedThreadEmoji): Receives a ``UpdatedThreadEmoji`` object.
        """

        self.logger.info(
            f"Thread's {message.thread_id} quick reaction emoji has changed to {event_data.emoji}"
        )

    async def on_nickname_change(
        self, event_data: ThreadNickname, message: MessageData
    ):
        """
        Called when a User's nickname is changed in a thread.

        Args:
            event_data (ThreadNickname): Receives a ``ThreadNickname`` object with user nickname change info.
        """
        self.logger.info(
            f" User ({event_data.participant_id}) nickname changed to '{event_data.nickname}' in Thread ({message.thread_id})"
        )

    async def on_message_sharing_change(
        self, event_data: ThreadMessageSharing, message: MessageData
    ):
        """
        Called when message sharing permission is changed in a thread.

        Args:
            event_data (ThreadMessageSharing): Receives a ``ThreadMessageSharing`` object.
            message (MessageData): Receives a ``MessageData`` object with extra message info.
        """
        self.logger.info(
            f"{event_data.sender_name} has updated message sharing mode to '{event_data.mode}' in Thread ({message.thread_id})"
        )

    async def on_viewer_status_change(self, event_data: ChangeViwerStatus):
        """Called when a Thread gets blocked on Facebook/Messenger

        Args:
            event_data (ChangeViwerStatus): Receives a ``ChangeViwerStatus`` object.
        """
        self.logger.info(
            f"{event_data.thread_id} is blocked on {'Facebook' if event_data.is_facebook_blocked else 'Messenger'} by {event_data.user_id}"
        )

    async def on_friend_request_change(self, event_data: FriendRequestState):
        """Called when a friend request is confirmed/rejected or a friend request is sent by the Client.

        Args:
            event_data (FriendRequestState): Receives a ``FriendRequestState`` object.
        """
        self.logger.info(
            f"User's ({event_data.user_id}) friend request has been {event_data.action}ed "
        )

    async def on_poke_nofification(self, event_data: PokeNotification):
        """Called when a user pokes the Client.

        Args:
            event_data (PokeNotification): Receives a ``PokeNotification`` object.
        """
        self.logger.info(f"{event_data.user_poked} poked Client.")
