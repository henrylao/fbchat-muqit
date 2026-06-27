"""Tests for null-safety fixes in the GraphQL message parser.

Covers edge cases where Facebook's GraphQL API returns null for fields
the parser previously assumed were always arrays or objects.
"""

from fbchat_muqit.events.dispatcher import EventDispatcher
from fbchat_muqit.models.attachment import AttachmentType
from fbchat_muqit.models.deltas.custom_type import Value
from fbchat_muqit.models.deltas.parser import MessageParser
from fbchat_muqit.models.message import MessageType
from fbchat_muqit.models.message import Reaction
from fbchat_muqit.models.thread import ThreadType


def _make_message_dict(**overrides):
    """Build a minimal GraphQL message dict matching the parser's expected shape."""
    base = {
        "message_id": "mid.test123",
        "message": {"text": "hello", "ranges": []},
        "message_sender": {"id": "111"},
        "timestamp_precise": "1700000000000",
        "unsent_timestamp_precise": "0",
        "message_unsendability_status": "can_unsend",
        "message_reactions": [],
        "blob_attachments": [],
        "sticker": None,
        "extensible_attachment": None,
    }
    base.update(overrides)
    return base


def _get_parser():
    return MessageParser(EventDispatcher().logger)


# ── parse_message_from_graphql ──────────────────────────────────


def test_parse_message_with_null_message_field():
    """System messages may have no 'message' key — should not crash."""
    parser = _get_parser()
    m = _make_message_dict(message=None)
    msg = parser.parse_message_from_graphql(m, "999", ThreadType.GROUP)
    assert msg.text == ""
    assert msg.sender_id == "111"


def test_parse_message_with_missing_message_key():
    """Even if 'message' is absent entirely, should not crash."""
    parser = _get_parser()
    m = _make_message_dict()
    del m["message"]
    msg = parser.parse_message_from_graphql(m, "999", ThreadType.GROUP)
    assert msg.text == ""


def test_parse_message_with_null_reactions():
    """Reactions may be null for messages with no reactions."""
    parser = _get_parser()
    m = _make_message_dict(message_reactions=None)
    msg = parser.parse_message_from_graphql(m, "999", ThreadType.GROUP)
    assert msg.reaction == []


def test_parse_message_with_null_blob_attachments():
    """blob_attachments may be null instead of empty list."""
    parser = _get_parser()
    m = _make_message_dict(blob_attachments=None)
    msg = parser.parse_message_from_graphql(m, "999", ThreadType.GROUP)
    assert msg.message_type == MessageType.TEXT


def test_parse_message_with_null_ranges():
    """message.ranges may be null instead of empty list."""
    parser = _get_parser()
    m = _make_message_dict(message={"text": "hi", "ranges": None})
    msg = parser.parse_message_from_graphql(m, "999", ThreadType.GROUP)
    assert msg.mentions is None


def test_parse_message_with_populated_reactions():
    """Normal reactions should still parse correctly."""
    parser = _get_parser()
    m = _make_message_dict(
        message_reactions=[
            {"reaction": "\u2764", "user": {"id": "222"}},
            {"reaction": "\ud83d\ude04", "user": {"id": "333"}},
        ]
    )
    msg = parser.parse_message_from_graphql(m, "999", ThreadType.GROUP)
    assert len(msg.reaction) == 2
    assert msg.reaction[0].reaction == "\u2764"
    assert msg.reaction[0].reactor == "222"
    assert msg.reaction[0].reaction_type == Reaction.ADDED


# ── get_from_attachment ──────────────────────────────────────────


def test_get_from_attachment_null_blob():
    """null blob_attachments should fall through to TEXT."""
    parser = _get_parser()
    m = _make_message_dict(blob_attachments=None)
    assert parser.get_from_attachment(m) == MessageType.TEXT


def test_get_from_attachment_empty_blob():
    """empty blob_attachments list should fall through to TEXT."""
    parser = _get_parser()
    m = _make_message_dict(blob_attachments=[])
    assert parser.get_from_attachment(m) == MessageType.TEXT


def test_get_from_attachment_sticker():
    """Sticker present should return STICKER."""
    parser = _get_parser()
    m = _make_message_dict(sticker={"id": "123"})
    assert parser.get_from_attachment(m) == MessageType.STICKER


def test_get_from_attachment_unknown_extensible_type():
    """Unknown extensible_attachment target type should fall back to TEXT."""
    parser = _get_parser()
    m = _make_message_dict(
        extensible_attachment={
            "story_attachment": {
                "target": {"__typename": "SomeFutureNewType"}
            }
        }
    )
    assert parser.get_from_attachment(m) == MessageType.TEXT


def test_get_from_attachment_instagram_media():
    """InstagramMediaAttachmentLink should map to the INSTAGRAM_MEDIA type."""
    from fbchat_muqit.models.attachment import AttachmentType
    assert hasattr(AttachmentType, "INSTAGRAM_MEDIA")
    assert AttachmentType.INSTAGRAM_MEDIA == "InstagramMediaAttachmentLink"


# ── _safe_parse_attachments ─────────────────────────────────────


def test_safe_parse_attachments_no_attachment():
    """No attachment data should return None."""
    parser = _get_parser()
    m = _make_message_dict()
    assert parser._safe_parse_attachments(m) is None


def test_safe_parse_attachments_null_fields():
    """All null attachment fields should return None, not crash."""
    parser = _get_parser()
    m = _make_message_dict(blob_attachments=None, sticker=None, extensible_attachment=None)
    assert parser._safe_parse_attachments(m) is None


# ── parse_mention ────────────────────────────────────────────────


def test_parse_mention_null_ranges():
    """null ranges should return None."""
    parser = _get_parser()
    assert parser.parse_mention(None) is None


def test_parse_mention_empty_ranges():
    """empty ranges list should return None."""
    parser = _get_parser()
    assert parser.parse_mention([]) is None


def test_parse_mention_valid_ranges():
    """Valid ranges should parse into Mention objects."""
    parser = _get_parser()
    ranges = [
        {"entity": {"id": "444"}, "offset": 0, "length": 5},
        {"entity": {"id": "555"}, "offset": 10, "length": 3},
    ]
    mentions = parser.parse_mention(ranges)
    assert len(mentions) == 2
    assert mentions[0].user_id == "444"
    assert mentions[0].offset == 0
    assert mentions[0].length == 5
