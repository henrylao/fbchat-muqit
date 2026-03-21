from types import SimpleNamespace

from fbchat_muqit.events.dispatcher import EventDispatcher
from fbchat_muqit.models.attachment import AttachmentType, FileAttachment
from fbchat_muqit.models.deltas.custom_type import MentionType, Value
from fbchat_muqit.models.deltas.delta_wrapper import NewMessageDelta
from fbchat_muqit.models.deltas.parser import MessageParser
from fbchat_muqit.models.message import MessageType
from fbchat_muqit.models.thread import ThreadFolder, ThreadType


def _make_message_metadata(*, folder: str) -> SimpleNamespace:
    """Avoid constructing :class:`MessageData` (msgspec reserves ``id`` for struct metadata)."""
    return SimpleNamespace(
        id="mid",
        sender_id="123",
        folder=Value(folder),
        timestamp=1710000000000,
        thread_id=Value("456"),
        adminText="",
        unsendType="unknown",
    )


def test_attachment_file_maps_to_message_type_file():
    dispatcher = EventDispatcher()
    parser = MessageParser(dispatcher.logger)
    assert parser._attach_to_message[AttachmentType.FILE] == MessageType.FILE


def test_parse_message_uses_folder_and_heuristic_thread_type():
    dispatcher = EventDispatcher()
    parser = MessageParser(dispatcher.logger)

    delta_user = NewMessageDelta(
        messageMetadata=_make_message_metadata(folder="ARCHIVE"),  # type: ignore[arg-type]
        body="hi",
        attachments=[],
        mentions=MentionType(),
        participants=(1, 2),
    )
    msg_user = parser.parse_message(delta_user)
    assert msg_user.thread_folder == ThreadFolder.ARCHIVE
    assert msg_user.thread_type == ThreadType.USER

    delta_group = NewMessageDelta(
        messageMetadata=_make_message_metadata(folder="INBOX"),  # type: ignore[arg-type]
        body="hi",
        attachments=[],
        mentions=MentionType(),
        participants=(1, 2, 3, 4),
    )
    msg_group = parser.parse_message(delta_group)
    assert msg_group.thread_folder == ThreadFolder.INBOX
    assert msg_group.thread_type == ThreadType.GROUP


def test_parse_message_file_attachment_sets_message_type_file():
    dispatcher = EventDispatcher()
    parser = MessageParser(dispatcher.logger)

    file_attachment = FileAttachment(
        download_url="https://example.com/file.bin",
    )

    class _Mercury:
        extensible_attachment = None
        sticker_attachment = None
        blob_attachment = file_attachment

    class _Raw:
        mercury = _Mercury()
        id = "att1"

    delta = NewMessageDelta(
        messageMetadata=_make_message_metadata(folder="INBOX"),  # type: ignore[arg-type]
        body=None,
        attachments=[_Raw()],  # type: ignore[list-item]
        mentions=MentionType(),
        participants=(1, 2),
    )

    msg = parser.parse_message(delta)
    assert msg.message_type == MessageType.FILE
