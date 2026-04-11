"""
State Management for fbchat-muqit

This module provides a State class that handles
Facebook authentication, session management, and API communication.
"""

from __future__ import annotations

import aiofiles
import aiohttp
import asyncio
import time

from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from os.path import basename
from typing import Dict, Optional, List, Any, Tuple
from yarl import URL
from aiohttp import ClientSession, CookieJar
from puremagic import from_string

# fbchat-muqit imports
from .graphql import GraphQLProcessor
from .utils.stateHelper import (
    load_json_cookies_async,
    cookie_list_to_jar,
    dump_jar_to_cookie_list,
    get_user_id,
    extract_tokens_from_html,
    client_id_factory,
    get_session,
)
from .logging.logger import get_logger, FBChatLogger
from .exception.errors import (
    FBChatError,
    AuthenticationError,
    ResponseError,
    SessionExpiredError,
    FacebookAPIError,
    NetworkError,
    handle_exceptions,
)
from .utils.utils import (
    now,
    decimal_to_base36,
    mimetype_to_key,
    prefix_url,
    get_jsmods_require,
    generate_message_id,
    generate_offline_threading_id,
)

__all__ = ["State"]


def generate_download_session() -> ClientSession:
    return ClientSession()


@dataclass
class State:
    """
    Modern State class for Facebook session management.

    Handles authentication, session state, token management, and API communication.
    """

    # User information
    user_id: str = field()
    user_name: str = field()
    _host: str = field()

    # Authentication tokens
    _fb_dtsg: str = field()
    _fb_dtsg_ag: str = field()
    _lsd: str = field()
    _jazoest: str = field()
    _jazoest_async: str = field()
    _revision: Optional[int] = field()
    _mqttClientID: str = field()
    _mqttAppID: str = field()
    _userAppID: str = field()

    # MQTT connection details
    _endpoint: str = field()
    _region: str = field()
    _client_id: str = field(default_factory=client_id_factory)

    # Session management
    _session: ClientSession = field(default_factory=get_session)
    _counter: int = field(default=0)
    _is_logged: bool = field(default=False)
    _jar: Optional[CookieJar] = field(default=None)

    # Auto-refresh configuration
    _last_refresh: float = field(default_factory=time.time)
    _refresh_interval: int = field(default=3600)  # 1 hour
    _auto_refresh_enabled: bool = field(default=True)

    # Logging
    _logger: FBChatLogger = field(default_factory=get_logger)
    _graphql: GraphQLProcessor = GraphQLProcessor()

    # extra session
    _download_session: ClientSession = field(default_factory=generate_download_session)

    # Headers and configuration
    _userAgent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
    )

    BASE_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/137.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }
    ALLHEADERS = {
        "get": {
            **BASE_HEADERS,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Sec-Fetch-User": "?1",
            "Sec-Fetch-Site": "same-origin",
            # no Content-Type for GET!
        },
        "post": {
            **BASE_HEADERS,
            "Accept": "*/*",
            "Content-Type": "application/x-www-form-urlencoded",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        },
        "upload": {
            **BASE_HEADERS,
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Accept": "*/*",
            # Let aiohttp set multipart Content-Type automatically
        },
        "publish_post": {
            **BASE_HEADERS,
            "Accept": "*/*",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    }

    def __post_init__(self):
        """Initialize the State instance."""
        # do I need to change them here or it will be automatically changed
        # i'm not sure since we aee doing `**BASE_HEADERS`
        self.BASE_HEADERS["User-Agent"] = self._userAgent
        self.ALLHEADERS["get"]["User-Agent"] = self._userAgent
        self.ALLHEADERS["post"]["User-Agent"] = self._userAgent
        self.ALLHEADERS["upload"]["User-Agent"] = self._userAgent

        # Start auto-refresh task if enabled
        # if self._auto_refresh_enabled:
        #     asyncio.create_task(self._auto_refresh_loop())

    @property
    def is_refresh_needed(self) -> bool:
        """Check if tokens need refreshing based on time interval."""
        return (time.time() - self._last_refresh) > self._refresh_interval

    def get_params(self) -> Dict[str, Any]:
        """Get standard necessary parameters for Facebook API requests."""
        self._counter += 1

        params = {
            # "av": self.user_id,
            "__user": self.user_id,
            "__a": "1",
            "__req": decimal_to_base36(self._counter),
            "__rev": self._revision,
            "fb_dtsg": self._fb_dtsg,
            "jazoest": self._jazoest,
        }

        self._logger.trace(f"Generated request params for counter {self._counter}")
        return params

    def build_headers(
        self,
        url: str,
        request_type: str = "get",
        graphql_data: Dict = dict(),
        user_agent: Optional[str] = None,
    ) -> dict:
        """
        Dynamically build headers for a given URL and request type.
        - request_type: "get", "post", "upload", "graphql", etc.
        """
        parsed = URL(url)
        host = parsed.host or "www.facebook.com"
        base_url = f"https://{host}"

        headers = self.ALLHEADERS.get(request_type, {}).copy()

        # Dynamically adjust host-related headers
        headers.update(
            {
                "Host": host,
                "Origin": base_url,
                "Referer": f"{base_url}/",
            }
        )

        if user_agent:
            headers["User-Agent"] = user_agent

        if "/api/graphql" in url and "fb_api_req_friendly_name" in graphql_data:
            headers["X-Fb-Friendly-Name"] = graphql_data["fb_api_req_friendly_name"]
            headers["X-Fb-Lsd"] = self._lsd

        # Adjust for Messenger
        if "messenger.com" in host:
            headers["Origin"] = "https://www.messenger.com"
            headers["Referer"] = "https://www.messenger.com/"

        # Adjust for uploads
        elif "rupload" in host or "upload" in request_type:
            headers.pop("Content-Type", None)  # multipart handled automatically
            headers["Accept"] = "*/*"

        # Adjust for mobile domain
        if host.startswith("m."):
            headers["Origin"] = "https://m.facebook.com"
            headers["Referer"] = "https://m.facebook.com/"

        return headers

    @classmethod
    @handle_exceptions(AuthenticationError)
    async def from_json_cookies(
        cls,
        json_cookies_path: str,
        user_agent: Optional[str] = None,
        proxy: Optional[str] = None,
    ) -> State:
        """
        Create State instance from saved Facebook cookies JSON file.

        Args:
            json_cookies_path: Path to the saved Facebook cookies JSON file
            user_agent: Optional custom User-Agent string
            proxy: Optional proxy URL

        Returns:
            State instance with authenticated session

        Raises:
            AuthenticationError: If authentication fails
        """
        logger = get_logger()
        logger.info(f"Creating Session from cookies file: {json_cookies_path}")

        try:
            jar: CookieJar = await load_json_cookies_async(json_cookies_path)
            session: ClientSession = get_session(jar, proxy)
            return await cls.login(session, jar, user_agent)
        except Exception as e:
            logger.error(f"Failed to create State from cookies: {e}")
            raise AuthenticationError(
                f"Failed to load session from cookies: {e}",
                details={"cookies_path": json_cookies_path},
            ) from e

    @classmethod
    @handle_exceptions(AuthenticationError)
    async def from_cookie_list(
        cls,
        cookies: List[Dict[str, Any]],
        user_agent: Optional[str] = None,
        proxy: Optional[str] = None,
    ) -> "State":
        """
        Restore :class:`State` from serialized cookie list (fbstate format).
        Use with :meth:`dump_session_snapshot` for persistence without a cookie file.
        """
        jar = cookie_list_to_jar(cookies)
        session: ClientSession = get_session(jar, proxy)
        return await cls.login(session, jar, user_agent)

    def dump_session_snapshot(self) -> Dict[str, Any]:
        """Export cookies for JSON/Redis storage; restore via :meth:`from_cookie_list`."""
        if self._session is None or self._session.cookie_jar is None:
            raise FBChatError("No active session to serialize")
        return {
            "version": 1,
            "cookies": dump_jar_to_cookie_list(self._session.cookie_jar),
        }

    @classmethod
    @handle_exceptions(AuthenticationError)
    async def login(
        cls,
        session: ClientSession,
        jar: Optional[CookieJar],
        user_agent: Optional[str] = None,
    ) -> State:
        """
        Create State instance from existing aiohttp session.

        Args:
            session (ClientSession): Authenticated aiohttp ClientSession
            jar (CookieJar): Cookie jar from the session

        Returns:
            State (State): instance with session data

        Raises:
            AuthenticationError: If session is invalid or extraction fails
        """
        logger = get_logger()
        try:
            user_id = get_user_id(session)
            logger.debug(f"Extracted user ID: {user_id}")

            # Initial URL and host determination
            url = "https://www.facebook.com/"
            host = "www.facebook.com"

            logger.debug(f"Making initial request to: {url}")

            headers = cls.ALLHEADERS["get"]
            headers["User-Agent"] = user_agent or headers["User-Agent"]
            headers["Host"] = host
            headers["Origin"] = url.removesuffix("/")
            headers["Referer"] = url

            async with session.get(
                url, headers=headers, allow_redirects=False
            ) as response:
                if 300 <= response.status < 400:
                    # Handle redirects to different Facebook domains
                    location = response.headers.get("Location")
                    if location:
                        host = str(URL(str(location)).host)
                        url = f"https://{host}/"
                        origin = f"https://{host}"

                        logger.debug(f"Redirected to: {host}")

                        # Update session headers for new host
                        headers.update({"Host": host, "Origin": origin, "Referer": url})

                        response = await session.get(url, headers=headers)
                else:
                    logger.debug(
                        f"Direct response from {host}, status: {response.status}"
                    )

                if response.status != 200:
                    raise NetworkError(
                        f"Failed to fetch Facebook page: HTTP {response.status}",
                        error_code=str(response.status),
                    )

                html_content = await response.text()
                logger.debug(f"Received HTML content: {len(html_content)} characters")

                # Save HTML for debugging if needed
                # save_html(html_content)

            # Extract tokens and configuration from HTML
            logger.debug("Extracting tokens from HTML content")
            (
                fb_dtsg,
                fb_dtsg_ag,
                lsd,
                jazoest,
                jazoest_async,
                client_revision,
                mqttClientID,
                mqttAppID,
                userAppID,
                endpoint,
                region,
                user_name,
            ) = extract_tokens_from_html(html_content)

            # Create State instance
            state = cls(
                user_id=user_id,
                user_name=user_name,
                _host=host,
                _fb_dtsg=fb_dtsg,
                _fb_dtsg_ag=fb_dtsg_ag,
                _lsd=lsd,
                _jazoest=jazoest,
                _jazoest_async=jazoest_async,
                _revision=client_revision,
                _mqttClientID=mqttClientID,
                _mqttAppID=mqttAppID,
                _userAppID=userAppID,
                _endpoint=endpoint,
                _region=region,
                _session=session,
                _is_logged=True,
                _jar=jar,
                _last_refresh=time.time(),
                _userAgent=user_agent or cls.BASE_HEADERS["User-Agent"],
            )

            logger.debug("Extracted tokens and State instance created successfully")
            return state

        except Exception as e:
            logger.error(f"Failed to create State from session: {e}")
            if isinstance(e, FBChatError):
                raise
            raise AuthenticationError(
                f"Failed to extract session data: {e}", original_exception=e
            ) from e

    def is_logged_in(self) -> bool:
        """Check if the session is currently logged in."""
        return self._is_logged

    def get_cookies(self) -> Dict[str, str]:
        """Get current session cookies."""
        cookies = self._session.cookie_jar.filter_cookies(
            URL("https://www.facebook.com")
        )
        return {name: cookie.value for name, cookie in cookies.items()}

    @handle_exceptions(AuthenticationError)
    async def _refresh(self) -> None:
        """
        Refresh authentication tokens and session state.

        Raises:
            AuthenticationError: If refresh fails
        """
        self._logger.info("Refreshing session tokens and state")

        try:
            # Create new State from current session
            new_state = await State.login(
                session=self._session, jar=self._jar, user_agent=self._userAgent
            )

            # Update current instance with new data
            self.user_id = new_state.user_id
            self.user_name = new_state.user_name
            self._host = self._host
            self._fb_dtsg = new_state._fb_dtsg
            self._lsd = new_state._lsd
            self._jazoest = new_state._jazoest
            self._revision = new_state._revision
            self._mqttClientID = new_state._mqttClientID
            self._mqttAppID = new_state._mqttAppID
            self._userAppID = new_state._userAppID
            self._endpoint = new_state._endpoint
            self._region = new_state._region
            self._counter = new_state._counter
            self._is_logged = new_state._is_logged
            self._jar = new_state._jar
            self._last_refresh = time.time()
            self._userAgent = new_state._userAgent or self.BASE_HEADERS["User-Agent"]

            self._logger.info("Session refresh completed successfully")

        except Exception as e:
            self._logger.error(f"Session refresh failed: {e}")
            self._is_logged = False
            raise AuthenticationError(
                f"Failed to refresh session: {e}", original_exception=e
            ) from e

    async def _auto_refresh_loop(self) -> None:
        """Background task for automatic token refresh."""
        self._logger.debug("Starting auto-refresh loop")

        while self._auto_refresh_enabled and self._is_logged:
            try:
                await asyncio.sleep(300)  # Check every 5 minutes

                if self.is_refresh_needed:
                    self._logger.info("Auto-refresh triggered")
                    await self._refresh()

            except asyncio.CancelledError:
                self._logger.debug("Auto-refresh loop cancelled")
                break
            except Exception as e:
                self._logger.error(f"Auto-refresh loop error: {e}")
                await asyncio.sleep(60)  # Wait before retrying

    def enable_auto_refresh(self, interval: int = 3600) -> None:
        """
        Enable automatic token refresh.

        Args:
            interval: Refresh interval in seconds (default: 1 hour)
        """
        self._refresh_interval = interval
        self._auto_refresh_enabled = True
        self._logger.info(f"Auto-refresh enabled with {interval}s interval")

        # Start the loop if not already running
        asyncio.create_task(self._auto_refresh_loop())

    def disable_auto_refresh(self) -> None:
        """Disable automatic token refresh."""
        self._auto_refresh_enabled = False
        self._logger.debug("Auto-refresh disabled")

    @handle_exceptions(NetworkError)
    async def _check_request(self, response: aiohttp.ClientResponse) -> str:
        """
        Check HTTP response and extract content.

        Args:
            response: aiohttp response object

        Returns:
            Response content as string

        Raises:
            NetworkError: If request failed
        """
        # Check HTTP status code
        if response.status == 404:
            raise NetworkError(
                f"HTTP 404: Invalid URL or ID. Status: {response.status}",
                error_code=str(response.status),
            )
        elif 400 <= response.status < 600:
            raise NetworkError(
                f"HTTP error: {response.status}", error_code=str(response.status)
            )

        # Get response content
        content = await response.text(encoding="utf-8")

        if not content:
            raise NetworkError("Empty response received")

        self._logger.debug(
            f"Request successful: {response.status}, content: {len(content)} chars"
        )
        return content

    @handle_exceptions(FacebookAPIError)
    async def _get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict] = None,
        error_retries: int = 3,
    ) -> Dict[str, Any]:
        """
        Perform GET request to Facebook API.

        Args:
            url: Request URL
            params: Query parameters
            error_retries: Number of retry attempts

        Returns:
            Parsed JSON response

        Raises:
            FacebookAPIError: If API returns an error
        """

        full_url = prefix_url(url, self._host)
        if params is None:
            params = {}

        p = self.get_params()
        if "fb_dtsg_ag" in params:
            p.pop("fb_dtsg")
            p.pop("jazoest")
        p.update(params)
        params = p
        # Configure headers
        headers = self.build_headers(full_url)

        self._logger.log_api_request("GET", full_url, data=params)

        try:
            async with self._session.get(
                full_url, params=params, data=data, headers=headers
            ) as response:
                content = await self._check_request(response)

            json_data = self._graphql.process_normal_response(content)

            # Check for payload errors using modern GraphQL processor
            self._graphql.handle_payload_error(json_data)

            self._logger.log_api_response(response.status, full_url)
            return json_data

        except FBChatError as e:
            if error_retries > 0 and isinstance(
                e, (SessionExpiredError, FacebookAPIError)
            ):
                self._logger.warning(
                    f"Request failed, retrying... ({error_retries} attempts left)"
                )
                await self._refresh()
                return await self._get(url, params, error_retries=error_retries - 1)
            raise

    @handle_exceptions(FacebookAPIError)
    async def _post(
        self,
        url: str,
        data: Dict[str, Any],
        files: Optional[Dict[str, Tuple[str, Any, str]]] = None,
        as_graphql: bool = False,
        error_retries: int = 3,
        raw: bool = False,
        headers: dict | None = None,
        no_response: bool = False,
        params: Dict | None = None,
        header_type="post",
    ) -> Any:
        """
        Perform POST request to Facebook API.

        Args:
            url: Request URL
            data: POST data
            files: File attachments
            as_graphql: Whether to process as GraphQL response
            error_retries: Number of retry attempts

        Returns:
            Parsed response data

        Raises:
            FacebookAPIError: If API returns an error
        """
        if data:
            data.update(self.get_params())
        if params:
            params.update(self.get_params())
        full_url = prefix_url(url, self._host)
        data = data or {}

        if files:
            copied_headers = self.build_headers(full_url, "upload")
        else:
            copied_headers = self.build_headers(
                full_url, header_type, graphql_data=data
            )

        if headers:
            copied_headers.update(headers)

        # Handle file uploads
        if files:
            form_data = aiohttp.FormData()
            for key, value in data.items():
                form_data.add_field(key, str(value))
            for k, (filename, file_obj, content_type) in files.items():
                form_data.add_field(
                    k, file_obj, filename=filename, content_type=content_type
                )

            data = form_data  # type: ignore

            # Let aiohttp handle Content-Type for multipart
            # copied_headers.pop("Content-Type", None)

        self._logger.log_api_request(
            "POST", full_url, data=data if not files else "multipart"
        )
        try:
            async with self._session.post(
                full_url, data=data, headers=copied_headers, params=params
            ) as response:

                if raw:
                    return await response.read()
                elif no_response:
                    return
                content = await self._check_request(response)

            # Process response based on type
            if as_graphql:
                result = self._graphql.process_response(content)
            else:
                result = self._graphql.process_normal_response(content)
                self._graphql.handle_payload_error(result)

            self._logger.log_api_response(response.status, full_url)
            return result

        except FBChatError as e:
            if error_retries > 0 and isinstance(
                e, (SessionExpiredError, FacebookAPIError)
            ):
                self._logger.warning(
                    f"Request failed, retrying... ({error_retries} attempts left)"
                )
                # await self._refresh()
                # return await self._post(url, data, files, as_graphql, error_retries - 1)
                self._logger.info(f"error: {e}")
            raise

    async def _option(self, url: str, params: Dict, headers: Dict):
        """Right now this function only used during facebook video upload."""
        params.update(self.get_params())
        new_header = self.build_headers(url, "upload")
        new_header.update(headers)
        try:
            async with self._session.options(
                url, params=params, headers=new_header
            ) as response:
                if response.status == 200:
                    self._logger.debug("Successfully sent `OPTIONS` request.")
                else:
                    self._logger.debug(
                        f"`OPTIONS` requests wasn't successfull. Response Status: {response.status}"
                    )
        except Exception as e:
            FBChatError("Error while sending `OPTION` request.", original_exception=e)

    @handle_exceptions(FacebookAPIError)
    async def _payload_post(
        self,
        url: str,
        data: Dict[str, Any],
        files: Optional[Dict[str, Tuple[str, Any, str]]] = None,
    ) -> Dict[str, Any]:
        """
        Perform POST request and extract payload from response.

        Args:
            url: Request URL
            data: POST data
            files: File attachments

        Returns:
            Payload data from response

        Raises:
            FacebookAPIError: If payload is missing or invalid
        """
        response = await self._post(url, data=data, files=files)

        try:
            return response["payload"]
        except (KeyError, TypeError) as e:
            raise FacebookAPIError(
                "Missing or invalid payload in response", details={"response": response}
            ) from e

    @handle_exceptions(FacebookAPIError)
    async def _graphql_requests(self, *queries) -> List[Optional[Dict[str, Any]]]:
        """
        Execute multiple GraphQL queries in a batch.

        Args:
            queries: GraphQL query objects

        Returns:
            List of query results

        Raises:
            FacebookAPIError: If GraphQL request fails
        """

        data = {
            "method": "GET",
            "response_format": "json",
            "queries": self._graphql.queries_to_json(*queries),
        }

        self._logger.debug(f"Executing {len(queries)} GraphQL queries")
        return await self._post("/api/graphqlbatch/", data, as_graphql=True)

    @handle_exceptions(FacebookAPIError)
    async def _upload(
        self,
        files: List[Tuple[str, Any, str]],
        voice_clip: bool = True,
        full_data: bool = False,
    ) -> List[int]:
        """
        Upload files to Facebook.

        Args:
            files: List of (filename, file_object, content_type) tuples
            voice_clip: Whether files are voice clips

        Returns:
            List of (file_id, content_type) tuples

        Raises:
            FacebookAPIError: If upload fails
        """
        file_dict = {f"upload_{i}": file_data for i, file_data in enumerate(files)}
        data = {"voice_clip": "true" if voice_clip else "false"}

        self._logger.info(f"Uploading {len(files)} files (voice_clip: {voice_clip})")

        json_response = await self._post(
            "https://upload.facebook.com/ajax/mercury/upload.php",
            data=data,
            files=file_dict,
        )
        json_response = json_response["payload"]

        if len(json_response["metadata"]) != len(files):
            raise FacebookAPIError(
                "Some files could not be uploaded",
                details={"response": json_response, "files_count": len(files)},
            )

        self._logger.debug(
            f"Successfully uploaded {len(json_response['metadata'])}/{len(files)} files"
        )

        # Extract file IDs and types

        # extract only Id

        if full_data:
            results = []
            for data in json_response["metadata"].values():
                file_id = data[mimetype_to_key(data["filetype"])]
                file_type = data["filetype"]
                filename = data["filename"]
                results.append((file_id, file_type, filename))

            return results

        return [next(iter(i.values())) for i in json_response["metadata"].values()]

    async def download_file(self, url, filename):
        async with self._download_session.get(url) as resp:
            resp.raise_for_status()
            async with aiofiles.open(filename, "wb") as f:
                async for chunk in resp.content.iter_chunked(1024 * 64):
                    await f.write(chunk)
        self._logger.info(f"Downloaded: {filename}")

    @handle_exceptions(FacebookAPIError)
    async def send_request(self, data: Dict[str, Any]) -> Tuple[str, str]:
        # not used anymore
        """
        Send a message request to Facebook.

        Args:
            data: Message data

        Returns:
            Tuple of (message_id, thread_id)

        Raises:
            FacebookAPIError: If sending fails
        """
        offline_threading_id = generate_offline_threading_id()

        # Prepare message data
        data.update(
            {
                "client": "mercury",
                "author": f"fbid:{self.user_id}",
                "timestamp": now(),
                "source": "source:chat:web",
                "offline_threading_id": offline_threading_id,
                "message_id": offline_threading_id,
                "threading_id": generate_message_id(self._client_id),
                "ephemeral_ttl_mode": "0",
            }
        )

        self._logger.debug(
            f"Sending message request for thread: {data.get('thread_fbid', 'unknown')}"
        )

        json_response = await self._post("/messaging/send/", data)

        # Update fb_dtsg token if received
        fb_dtsg = get_jsmods_require(json_response, 2)
        if fb_dtsg:
            self._fb_dtsg = fb_dtsg
            self._logger.trace("Updated fb_dtsg token from response")

        # Extract message IDs
        try:
            message_ids = [
                (action["message_id"], action["thread_fbid"])
                for action in json_response["payload"]["actions"]
                if "message_id" in action
            ]

            if len(message_ids) != 1:
                self._logger.warning(
                    f"Got {len(message_ids)} message IDs back: {message_ids}"
                )

            if not message_ids:
                raise FacebookAPIError(
                    "No message IDs found in response",
                    details={"response": json_response},
                )

            message_id, thread_id = message_ids[0]
            self._logger.debug(
                f"Message sent successfully: {message_id} in thread {thread_id}"
            )
            return message_id, thread_id

        except (KeyError, IndexError, TypeError) as e:
            raise FacebookAPIError(
                "Failed to extract message ID from response",
                details={"response": json_response},
            ) from e

    @asynccontextmanager
    async def get_files_from_paths(self, file_paths: List[str]):
        """
        Context manager for handling file uploads.

        Args:
            file_paths: List of file paths to upload

        Yields:
            List of (filename, file_object, content_type) tuples
        """
        files = []
        for file_path in file_paths:
            async with aiofiles.open(file_path, "rb") as f:
                file_obj = await f.read()
            content_type = from_string(file_obj, True)
            filename = basename(file_path)

            files.append((filename, file_obj, content_type))

        yield files

        # finally:
        #     # Ensure all files are closed
        #     for filename, file_obj, content_type in files:
        #         try:
        #             file_obj
        #         except Exception as e:
        #             self._logger.warning(f"Failed to close file {filename}: {e}")

    async def get_files_from_urls(self, file_urls) -> List[Tuple[str, bytes, str]]:
        files = []
        async with aiohttp.ClientSession() as session:
            for file_url in file_urls:
                async with session.get(file_url) as response:
                    if response.status != 200:
                        raise ResponseError(
                            error_code=str(response.status),
                            message=f"Failed to fetch {file_url}",
                        )
                    file_name = basename(file_url).split("?")[0].split("#")[0]
                    content = await response.read()  # Read the content as bytes
                    content_type = response.headers.get("Content-Type") or from_string(
                        content, True
                    )
                    files.append(
                        (
                            file_name,
                            content,  # Use bytes, not StreamReader
                            content_type,
                        )
                    )
        return files

    async def close(self) -> None:
        """Clean up resources and close connections."""
        self._logger.info("Closing State session")

        # Disable auto-refresh
        self.disable_auto_refresh()

        # Close session
        if not self._download_session.closed:
            await self._download_session.close()

        if self._session and not self._session.closed:
            await self._session.close()

        self._is_logged = False
        self._logger.info("State session closed")

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
