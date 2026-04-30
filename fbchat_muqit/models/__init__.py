"""
For Type annotation you can import all models from here.
"""

from .thread import ThreadType, ThreadFolder, Thread
from .user import User

from .attachment import (
        Attachment,
        AttachmentType,
        ImageAttachment,
        VideoAttachment,
        GifAttachment,
        StickerAttachment,
        AudioAttachment,
        FileAttachment,

        LocationAttachment,
        PostAttachment,
        SharedAttachment,
        ReelAttachment,
        ProfileAttachment,
        ProductAttachment,
        ExternalAttachment,

        Post,
        Author,
        GroupInfo,
        Media,
        Image,
        Dimension
        )

from .thread_actions import (
        ApprovalMode,
        ApprovalQueue,
        AdminAdded,
        AdminRemoved,
        ApprovedUser,
        addedParticipant,
        ChangeViwerStatus,
        JoinableMode,
        ParticipantsAdded,
        ParticipantLeft,
        ThreadAction,
        ThreadDelete,
        ThreadEmoji,
        ThreadFolderMove,
        ThreadMagicWord,
        ThreadMessagePin,
        ThreadMessageUnPin,
        ThreadMessageSharing,
        ThreadMuteSettings,
        ThreadName,
        ThreadNickname,
        ThreadTheme,
        MuteThread,
        ForcedFetch
    )

from .typing import Typing

from .timestamps import ReadReceipt, DeliveryReceipt, MarkFolderSeen, MarkRead, MarkUnread

from .message import Message, Mention, Mentions, MessageType, MessageRemove, MessageReaction, MessageUnsend, EmojiSize, Reaction

from .messagesData import MessageData

from .notifications import PokeNotification, FriendRequestState, friendUpdated, friendRequestList, PageNotification

from .presence import Presence, UserStatus

from .themes import Theme, AlternativeTheme, Asset

from .mqtt_response.search_message import MessageSearchResult, MessageSearchStatus



__all__ = [
    # Thread related
    "ThreadType", "ThreadFolder", "Thread",
    
    # User
    "User",
    
    # Attachments
    "Attachment",
    "AttachmentType",
    "ImageAttachment",
    "VideoAttachment",
    "GifAttachment",
    "StickerAttachment",
    "AudioAttachment",
    "FileAttachment",
    "LocationAttachment",
    "PostAttachment",
    "SharedAttachment",
    "ReelAttachment",
    "ProfileAttachment",
    "ProductAttachment",
    "ExternalAttachment",
    "Post",
    "Author",
    "GroupInfo",
    "Media",
    "Image",
    "Dimension",
    
    # Thread Actions
    "ApprovalMode",
    "ApprovalQueue",
    "AdminAdded",
    "AdminRemoved",
    "ApprovedUser",
    "addedParticipant",
    "ChangeViwerStatus",
    "JoinableMode",
    "ParticipantsAdded",
    "ParticipantLeft",
    "ThreadAction",
    "ThreadDelete",
    "ThreadEmoji",
    "ThreadFolderMove",
    "ThreadMagicWord",
    "ThreadMessagePin",
    "ThreadMessageUnPin",
    "ThreadMessageSharing",
    "ThreadMuteSettings",
    "ThreadName",
    "ThreadNickname",
    "ThreadTheme",
    "MuteThread",
    "ForcedFetch",
    
    # Typing
    "Typing",
    
    # Timestamps
    "ReadReceipt",
    "DeliveryReceipt",
    "MarkFolderSeen",
    "MarkRead",
    "MarkUnread",
    
    # Messages
    "Message",
    "Mention",
    "Mentions",
    "MessageType",
    "MessageRemove",
    "MessageReaction",
    "MessageUnsend",
    "Reaction",
    "EmojiSize",
    
    # Message Data
    "MessageData",
    
    # Notifications
    "PokeNotification",
    "PageNotification",
    "FriendRequestState",
    "friendUpdated",
    "friendRequestList",
    
    # Presence
    "Presence",
    "UserStatus",
    
    # Themes
    "Theme",
    "AlternativeTheme",
    "Asset",

    # Message Response 
    "MessageSearchStatus",
    "MessageSearchResult",
]

__title__ = "fbchat-muqit"
__version__ = "1.2.2"
__description__ = "A powerful Facebook Messenger Chat API to interact with Facebook and Messenger. Easy to use fast and efficient library"

__copyright__ = "Copyright 2024 by Muhammad MuQiT"

__license__ = "GPL-V3.0"

__author__ = "Muhammad MuQiT"
__email__ = "togashiyuuta1111@gmail.com"

