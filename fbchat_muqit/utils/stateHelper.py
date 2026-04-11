import random
import re
import json
from typing import Any, List

import aiofiles

from aiohttp import ClientSession, CookieJar
from aiohttp_socks import ProxyConnector
from typing import Tuple, Optional
from yarl import URL
from http.cookies import SimpleCookie

from ..logging.logger import get_logger
from ..exception.errors import FBChatError, ValidationError

logger = get_logger()


def get_session(
    cookie_jar: Optional[CookieJar] = None, proxy: Optional[str] = None
) -> ClientSession:
    if proxy:
        connector = ProxyConnector.from_url(proxy)
    else:
        connector = None

    return ClientSession(cookie_jar=cookie_jar, connector=connector)


def load_json_cookies(json_path: str) -> CookieJar:
    """Load and format cookies from fbstate.json for aiohttp or requests sessions."""
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not data or not isinstance(data, list):
        raise FBChatError("Invalid fbstate format. Expected a list of cookie objects.")

    key_name = "key" if "key" in data[0] else "name"

    jar = CookieJar()
    for cookie in data:
        name = cookie.get(key_name)
        value = cookie.get("value")
        path = cookie.get("path", "/")
        if not name or value is None:
            continue

        # Prepare separate SimpleCookie for each domain
        for domain_url in (
            "https://www.facebook.com",
            "https://www.messenger.com",
            "https://rupload-ccu1-2.up.facebook.com",
        ):
            domain = ".messenger.com" if "messenger" in domain_url else ".facebook.com"
            if "rupload" in domain_url:
                domain = "up.facebook.com"
            sc = SimpleCookie()
            sc[name] = value
            sc[name]["domain"] = domain  # Facebook accepts dot prefix
            sc[name]["path"] = path
            if "expires" in cookie:
                sc[name]["expires"] = cookie["expires"]
            # stores cookiex for each url
            jar.update_cookies(sc, URL(domain_url))

    logger.info(f" ✅ Succssfully loaded Cookies into Jar from {json_path}")
    return jar


async def load_json_cookies_async(json_path: str) -> CookieJar:
    """Load cookies from JSON path without blocking the event loop."""
    async with aiofiles.open(json_path, "r", encoding="utf-8") as f:
        raw = await f.read()
    data = json.loads(raw)

    if not data or not isinstance(data, list):
        raise FBChatError("Invalid fbstate format. Expected a list of cookie objects.")

    key_name = "key" if "key" in data[0] else "name"

    jar = CookieJar()
    for cookie in data:
        name = cookie.get(key_name)
        value = cookie.get("value")
        path = cookie.get("path", "/")
        if not name or value is None:
            continue

        for domain_url in (
            "https://www.facebook.com",
            "https://www.messenger.com",
            "https://rupload-ccu1-2.up.facebook.com",
        ):
            domain = ".messenger.com" if "messenger" in domain_url else ".facebook.com"
            if "rupload" in domain_url:
                domain = "up.facebook.com"
            sc = SimpleCookie()
            sc[name] = value
            sc[name]["domain"] = domain
            sc[name]["path"] = path
            if "expires" in cookie:
                sc[name]["expires"] = cookie["expires"]
            jar.update_cookies(sc, URL(domain_url))

    logger.info(f" ✅ Succssfully loaded Cookies into Jar from {json_path} (async)")
    return jar


def cookie_list_to_jar(data: List[dict[str, Any]]) -> CookieJar:
    """Build a :class:`CookieJar` from a list of cookie dicts (fbstate format)."""
    if not data or not isinstance(data, list):
        raise FBChatError("Invalid cookie data. Expected a list of cookie objects.")

    key_name = "key" if "key" in data[0] else "name"

    jar = CookieJar()
    for cookie in data:
        name = cookie.get(key_name)
        value = cookie.get("value")
        path = cookie.get("path", "/")
        if not name or value is None:
            continue

        for domain_url in (
            "https://www.facebook.com",
            "https://www.messenger.com",
            "https://rupload-ccu1-2.up.facebook.com",
        ):
            domain = ".messenger.com" if "messenger" in domain_url else ".facebook.com"
            if "rupload" in domain_url:
                domain = "up.facebook.com"
            sc = SimpleCookie()
            sc[name] = value
            sc[name]["domain"] = domain
            sc[name]["path"] = path
            if "expires" in cookie:
                sc[name]["expires"] = cookie["expires"]
            jar.update_cookies(sc, URL(domain_url))

    return jar


def dump_jar_to_cookie_list(jar: CookieJar) -> List[dict[str, Any]]:
    """Serialize a :class:`CookieJar` to fbstate-compatible cookie dicts."""
    out: List[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for url in (
        "https://www.facebook.com",
        "https://www.messenger.com",
        "https://rupload-ccu1-2.up.facebook.com",
    ):
        filtered = jar.filter_cookies(URL(url))
        for name, morsel in filtered.items():
            dom = str(morsel.get("domain", "") or "")
            key = (name, morsel.value, dom)
            if key in seen:
                continue
            seen.add(key)
            d: dict[str, Any] = {
                "name": name,
                "value": morsel.value,
                "path": morsel.get("path", "/"),
            }
            if morsel.get("expires"):
                d["expires"] = morsel["expires"]
            out.append(d)
    return out


def get_user_id(session: ClientSession) -> str:
    cookies = session.cookie_jar.filter_cookies(URL("https://www.facebook.com"))
    CookiesDict = {name: cookie.value for name, cookie in cookies.items()}
    return str(CookiesDict.get("c_user"))


def client_id_factory() -> str:
    return hex(int(random.random() * 2**31))[2:]


def save_html(html):
    with open("./test.html", "w") as f:
        f.write(html)


def extract_tokens_from_html(html: str) -> Tuple:
    """Extracts fb_dtsg, client_revision, mqttAppID etc. from HTML response to Login in facebook"""

    fb_dtsg = re.search(r'"DTSGInitialData".*?"token":"(.*?)"', html)
    if fb_dtsg:
        fb_dtsg = fb_dtsg.group(1)
    else:
        raise ValidationError("'fb_dtsg' token not found.")

    pattern = r'"DTSGInitData"(?:\s*,\s*\[\])?(?:\s*,\s*)\{[^}]*"async_get_token"\s*:\s*"([^"]+)"[^}]*\}'

    fb_dtsg_ag = re.search(pattern, html)
    if fb_dtsg_ag:
        fb_dtsg_ag = fb_dtsg_ag.group(1)
    else:
        raise ValidationError("'async_get_token' not found.")

    lsd_token = re.search(
        r'"LSD"\s*,\s*\[\s*\]\s*,\s*\{\s*"token"\s*:\s*"([A-Za-z0-9_-]+)"', html
    )
    if lsd_token:
        lsd_token = lsd_token.group(1)

    jazoest = "2" + str(sum(ord(c) for c in fb_dtsg))
    jazoest_async = "2" + str(sum(ord(c) for c in fb_dtsg_ag))

    clientRevision = re.search(r'client_revision":(\d+)', html)
    if clientRevision:
        clientRevision = int(clientRevision.group(1))

    clientID = re.search(
        r'\["MqttWebDeviceID".*?"clientID"\s*:\s*"([a-f0-9\-]+)"', html
    )
    if clientID:
        clientID = clientID.group(1)

    mqttAppID = re.search(r'\["MqttWebConfig".*?"appID"\s*:\s*(\d+)', html)
    if mqttAppID:
        mqttAppID = mqttAppID.group(1)

    userAppID = re.search(r'\["CurrentUserInitialData".*?"APP_ID"\s*:\s*"(\d+)"', html)
    if userAppID:
        userAppID = userAppID.group(1)

    # Extracting Mqtt endpoint for facebook
    mqttEndpoint = re.search(
        r'"endpoint"\s*:\s*"([^"]*?region=([a-zA-Z0-9_-]+)[^"]*)"', html
    )
    if not mqttEndpoint:
        raise ValueError("Mqtt Endpoint not found!")
    endpoint = mqttEndpoint.group(1).encode().decode("unicode_escape")
    region = mqttEndpoint.group(2)

    user_name = re.search(r'"NAME"\s*:\s*"([^"]+)"', html)
    if user_name:
        user_name = user_name.group(1)
        logger.debug(f"User name: {user_name}")

    logger.debug(f"fb dtag: {fb_dtsg}")  # used for post requests
    logger.debug(f"fb dtsg async: {fb_dtsg_ag}")  # used for get requests
    logger.debug(f"LSD token: {lsd_token}")
    logger.debug(f"jazoest: {jazoest}")  # used for post requests
    logger.debug(f"jazoest async: {jazoest_async}")  # used for get request
    logger.debug(f"client revision: {clientRevision}")
    logger.debug(f"client id(uuid): {clientID}")
    logger.debug(f"mqttAppID: {mqttAppID}")
    logger.debug(f"User App_ID: {userAppID}")
    logger.debug(f"Mqtt Endpoint URL: {endpoint}")
    logger.debug(f"Region: {region}")
    return (
        fb_dtsg,
        fb_dtsg_ag,
        lsd_token,
        jazoest,
        jazoest_async,
        clientRevision,
        clientID,
        mqttAppID,
        userAppID,
        endpoint,
        region,
        user_name,
    )
