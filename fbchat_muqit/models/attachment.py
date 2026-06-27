from __future__ import annotations

from typing import Optional, List, Union
from enum import Enum
from msgspec import Struct, field
from .deltas.custom_type import Value, PostId

class AttachmentType(str, Enum):
    IMAGE = "MessageImage"
    VIDEO = "MessageVideo" 
    AUDIO = "MessageAudio"
    GIF = "MessageAnimatedImage"
    STICKER = "Sticker"
    FILE = "MessageFile"
    LOCATION = "MessageLocation"
    SHARE = "ExtensibleMessageAttachment"
    FACEBOOKPOST = "Story"
    EXTERNALURL = "ExternalUrl"
    FACEBOOKPROFILE = "User"
    FACEBOOKSTORY = "source:fb_story"
    FACEBOOKREEL = "Video"
    FACEBOOKPRODUCT = "CommerceProductItemShare"
    FACEBOOKGAME = "MessengerBusinessMessage"
    MessengerCommunityInviteLink = "XFBMessengerCommunityInviteLink"
    INSTAGRAM_MEDIA = "InstagramMediaAttachmentLink"

#---------- Class Attributes of main classes ------------#

class Dimension(Struct, eq=False, frozen=True):
    """width and height of image"""
    height: int = field(name="x")
    """height of the image"""
    width: int = field(name="y")
    """weight of the image"""

class Image(Struct, eq=False, frozen=True):
    """An Image url with extra info."""
    url: str = field(name="uri")
    """url of the image"""
    height: int = 0
    """height of the image"""
    width: int = 0
    """width of the image"""

class Media(Struct, eq=False, frozen=True):
    preview: Image = field(name="image")
    """Thumbnail's information."""
    id: str = ""
    """The id of the picture/video"""
    is_playable: bool = False
    """True if It's an video otherwise False"""
    playable_url: Optional[str] = None
    """The url to play the video."""
    playable_duration: int = 0
    """The duration of the video sometimes it may be show 0 if it fails to get the duration of the video."""
    type: Optional[str] = field(name="__typename", default=None)
    """Can be an Image/Video/Audio"""



class Author(Struct, eq=False, frozen=True, tag="User", tag_field="__typename"):
    """Profile details of the author of the shared attachment"""
    id: str
    """The Facebook uid of the Post Author (user, page)"""
    name: str
    """Name of the Facebook """
    picture: Image = field(name="profile_picture")
    """Profile picture information of the `Author`."""
    url: str = field(name="url", default="")
    """The `Author`'s profile url"""
    cover_photo: dict = {}
    """The Author's profile cover photo sometimes may not be provided."""

class GroupInfo(Struct, frozen=True, eq=False):
    """Minimum Group information"""
    name: str
    """Group name"""
    url: str 
    """Group's url"""

class Post(Struct, eq=False, frozen=True, tag="Story", tag_field="__typename"):
    """Shared Facebook Post's information"""
    author: List[Author] = field(name="actors")
    """The `Author` of the post (Page, User e.g.)"""
    feedback_id: PostId = field(name="feedback")
    """Encoded post id can be used for reacting, sharing, commenting etc."""
    post_id: str = ""
    """Facebook post id of the `Post`"""
    creation_time: int = 0
    """The time post was published"""
    description: Optional[Value] = None
    """Description of the post"""
    title: Optional[Value] = None
    """Title of the post."""
    group: Optional[GroupInfo] = field(name="to", default=None)
    """If it is a Group post, some info maybe included"""





#------------- Message Attachment ----------# 

class ImageAttachment(Struct, eq=False, frozen=True, tag="MessageImage", tag_field="__typename"):
    """Image attachment with preview and dimensions."""
    id: str = field(name="legacy_attachment_id")
    """The Image Id of the Attachment. The Id can be used to send picture without uploading to Facebook server."""
    filename: str
    """The filename of the Image."""
    preview: Image
    """The preview of the image."""
    large_preview: Image
    """The preview of the image for desktop."""
    thumbnail: Image
    """The original Image probably."""
    original_dimensions: Dimension
    """The height and weight of the image."""
    original_extension: str
    """The extension of the Image, such as jpg, png"""
    render_as_sticker: bool
    """wether sent as Sticker or not"""
    type: AttachmentType = AttachmentType.IMAGE
    """Type of the attachment."""


    
class VideoAttachment(Struct, eq=False, frozen=True, tag="MessageVideo", tag_field="__typename"):
    """Video attachment with duration and preview.""" 
    id: str = field(name="legacy_attachment_id")
    """The Id of the video."""
    filename: str
    """filename of the video"""
    playable_url: str
    """The url for playing the video also can be used to download it."""
    preview: Image = field(name="chat_image")
    """Preview image of the video."""
    large_preview: Image = field(name="large_image")
    """Preview image of the video for desktop."""
    playable_duration: int = field(name="playable_duration_in_ms")
    """Duration of the video."""
    original_dimensions: Dimension
    """Height and Width of the Video."""
    type: AttachmentType = AttachmentType.VIDEO
    """Type of the Video."""

class GifAttachment(Struct, eq=False, frozen=True, tag="MessageAnimatedImage", tag_field="__typename"):
    id: str = field(name="legacy_attachment_id")
    """Gif Id of the Attachment."""
    filename: str
    """Filename of the Gif."""
    animated_image: Image
    """The gif information including its url."""
    preview_image: Image
    """Preview of the gif."""
    original_dimensions: Dimension
    """The Dimension of the gif."""
    type: AttachmentType = AttachmentType.GIF


class StickerAttachment(Struct, eq=False, frozen=True, tag="Sticker", tag_field="__typename"):
    """Sticker attachment with Its information."""
    id: str
    """The Id of the `Sticker`"""
    url: str
    """The url of the `Sticker`"""
    pack: Value
    """The Pack id of the `Sticker` if the `Sticker` is part of a Pack."""
    label: str
    """The label/words used for this `Sticker`"""
    frame_count: int
    """The frame count of the `Sticker` If It's an animated `Sticker`"""
    frame_rate: int
    """The frame rate of the `Sticker`"""
    frames_per_row: int
    """Animated Sticker's frame per row"""
    frames_per_column: int
    """Animated Sticker's frame per column"""
    width: int
    """Width of the Sticker"""
    height: int
    """Height of the Sticker"""
    sprite_image: Optional[Image]
    """Sprite sheet of the Sticker"""
    sprite_image_2x: Optional[Image]
    """Sprite sheet of the Sticker for desktop screen"""
    padded_sprite_image_2x: Optional[Image]
    """Padded Sprite Sheet"""
    type: AttachmentType = AttachmentType.STICKER
    """Type of the Attachment."""


class AudioAttachment(Struct, eq=False, frozen=True, tag="MessageAudio", tag_field="__typename"):
    """Audio attachment"""
    filename: str
    """filename of the Audio"""
    playable_url: str
    """Playable url of the Audio"""
    playable_duration: int  = field(name="playable_duration_in_ms")
    """Duration of the Audio message."""
    is_voicemail: bool = False
    """wether It is voicemail"""
    type: AttachmentType = AttachmentType.AUDIO
    """Type of the Attachment."""
    

class FileAttachment(Struct, eq=False, frozen=True, tag="MessageFile", tag_field="__typename"):
    """General file attachment. It can be any kind of file such as pdf, txt, html, docs etc."""
    download_url: Optional[str] = field(name="url")
    """The url of the file can be used to download"""
    mimetype: Optional[str] = None
    """Type of the file such as pdf, txt etc."""
    is_malicious: Optional[bool] = None
    """Wether the file is detected as malicious or not. Not really trustable :)"""
    type: AttachmentType = AttachmentType.FILE
    """Type of the Attachment."""
  



#-------------- ExtensibleAttachment ---------------#

class LocationAttachment(Struct, eq=False, frozen=True):
    # MessageLocation from `genie_message`
    """Location attachment with coordinates."""
    id: str
    """Id of the Attachment"""
    url: str
    """Location's url"""
    media: Media
    """The preview of the Location Attachment."""
    address: Optional[str] = None
    """Address of the Location"""
    latitude: Optional[float] = None
    """Latitude of the Location"""
    longitude: Optional[float] = None
    """Longitude of the Location"""
    is_live: bool = False 
    """True if the Location is live Location."""
    type: AttachmentType = AttachmentType.LOCATION
    """Type of the attachment"""


class PostAttachment(Struct, eq=False, frozen=False):
    # Story type from `genie_message`
    """Represents a Facebook Post Attachment shared to Messenger"""
    id: str 
    """The id of the Attachment"""
    title: str
    """The title of the Facebook Post."""
    description: str
    """The description of the Facebook Post"""
    post_preview: Media
    """The Post preview details"""
    post_url: str 
    """The url of the Facebook Post"""
    post: Post 
    """The extra information about the Post"""
    source: Optional[str] = None
    """source of the post such as Page name or User's name etc."""
    type = AttachmentType.FACEBOOKPOST
    """Type of the Attachment"""

class SharedAttachment(Struct, eq=False, frozen=True):
    # None tyoe from `genie_message`
    """Represents Unknown Attachment shared to Messenger"""
    id: str 
    """The Attachment Id."""
    title: Optional[str]
    """The title of the Shared Attachment"""
    description: Optional[str]
    """The description of the Shared Attachment"""
    media: Optional[Media]
    """Extra information about the shared Attachment."""
    type = AttachmentType.FACEBOOKSTORY
    """Attachment Type"""

class ReelAttachment(Struct, eq=False, frozen=False):
    # Video type from `genie_message`
    """Represents A Facebook Reel Shared to Messenger. Any Facebook video shared in both Facebook and Instagram is also counted as Reel."""
    id: str
    """Id of the Reel Attachment."""
    url: str
    """Url of the Reel"""
    media: Media
    """Extra information about the Reel"""
    source: str
    """source can be the Reel's author profile name or Page name"""
    video_id: str
    """The Video Id of the Reel."""
    title: str
    """The title of the Reel."""
    description: Optional[str] = None
    """Reels description."""
    type = AttachmentType.FACEBOOKREEL
    """Type of Attachment."""

class ProfileAttachment(Struct, eq=False, frozen=True): 
    # User type from `genie_message`
    """Facebook profile share attachment"""
    id: str
    """Attachment Id"""
    profile_id: str
    """Shared User's Facebook uid."""
    profile_name: str
    """Shared User's profile name"""
    profile_url: str
    """Url of the Shared profile"""
    profile_picture: Image
    """profile picture of the Shared User's"""
    cover_photo: Optional[Image] = None
    """Cover photo of the Shared User"""
    type = AttachmentType.FACEBOOKPROFILE
    """Attachment Type"""

class ExternalAttachment(Struct, eq=False, frozen=True):
    # ExternalUrl from `genie_message`
    """Any Link not related to Facebook Meta is ExternalUrl Attachment.such as YT link, website link etc."""
    id: str
    """Attachment Id"""
    url: str
    """Url of the Attachment"""
    media: Optional[Media]
    """The shared url's content information Such as if any Youtube video url will inlcude video thumbnail, name, duration details."""
    title: Optional[str] = None
    """Title of the The url content"""
    description: Optional[str] = None
    """description of the url source's content"""
    type = AttachmentType.EXTERNALURL
    """Attachment Type."""

class ProductAttachment(Struct, frozen=True, eq=False):
    id: str
    """Attachment Id."""
    product_name: str
    """The name of the Shared Facebook Product"""
    product_price: str
    """The product price (can be in any currency)"""
    url: str 
    """Url to the product"""
    type = AttachmentType.FACEBOOKPRODUCT
    """Attachment Type"""






# -------------- Dummy Types --------------------
class ExtUrl(Struct, eq=False, frozen=True, tag="ExternalUrl", tag_field="__typename"):
    id: str = ""

class MsgLoc(Struct, eq=False, frozen=True, tag="MessageLocation", tag_field="__typename"):
    id: str = ""

class VideoTarget(Struct, eq=False, frozen=True, tag="Video", tag_field="__typename"):
    id: str = ""
    video_id: str = ""

class MediaContainer(Struct, eq=False, frozen=True, tag="MediaContainerMediaSet", tag_field="__typename"):
    id: str = ""

class FBProduct(Struct, eq=False, frozen=True, tag="CommerceProductItemShare", tag_field="__typename"):
    id: str = ""

class FBGame(Struct, eq=False, frozen=True, tag="MessengerBusinessMessage", tag_field="__typename"):
    """"""
    id: str = ""
    message: str = ""

# only Post and Author type is used
TargetType = Union[Post, Author, ExtUrl, MsgLoc, VideoTarget, MediaContainer, FBProduct, FBGame, None]

# --------------------- End Dummy Types -----------------


BlobAttachment = Union[ImageAttachment, VideoAttachment, GifAttachment, FileAttachment, AudioAttachment]

Attachment = Union[ImageAttachment, VideoAttachment, GifAttachment, StickerAttachment, FileAttachment, AudioAttachment, PostAttachment, SharedAttachment, ReelAttachment, ProfileAttachment, ExternalAttachment, None]


