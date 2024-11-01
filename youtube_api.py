import logging
import os
import json
from datetime import datetime, timedelta
from isodate import parse_duration

import aiohttp
import aiofiles
import asyncio
import async_timeout
from zoneinfo import ZoneInfo

from homeassistant.helpers.storage import Store
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.components import persistent_notification
from homeassistant.exceptions import ConfigEntryNotReady

from .const import DOMAIN, CONF_MAX_REGULAR_VIDEOS, CONF_MAX_SHORT_VIDEOS, DEFAULT_MAX_REGULAR_VIDEOS, DEFAULT_MAX_SHORT_VIDEOS, WEBHOOK_ID

_LOGGER = logging.getLogger(__name__)

class QuotaExceededException(Exception):
    pass

class YouTubeAPI:
    def __init__(self, api_key, client_id, client_secret, refresh_token=None, channel_ids=None, hass=None, config_entry=None):
        self.api_key = api_key
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.channel_ids = list(channel_ids) if channel_ids else []  # Convert to list and ensure not None
        self.initial_channel_ids = self.channel_ids.copy()  # Store initial copy
        _LOGGER.critical(f"YouTubeAPI initialized with {len(self.channel_ids)} channel IDs")
        self.channel_ids = channel_ids if channel_ids else []
        self.initial_channel_ids = self.channel_ids.copy()
        self.hass = hass
        self.config_entry = config_entry
        self._store = Store(hass, 1, f"{DOMAIN}.youtube_data")
        self.session = None
        self.access_token = None
        self.token_expiration = None
        self.backoff_time = 15
        self.max_backoff_time = 3600
        self.quota_reset_time = None
        self.current_quota = 0
        self.fetched_video_ids = set()
        self.last_sync_time = None
        self.last_fetch_time = None
        self.subscriptions = None
        self.last_subscription_update = None
        self.channel_playlist_mapping = {}
        self.quota_reset_task = None
        self.subscription_renewal_task = None
        self.last_subscription_renewal_time = None
        self.playlist_cache = {}  
        self.channel_subscription_times = {}
        self.fetched_video_ids_lock = asyncio.Lock()
        self.video_processing_semaphore = asyncio.Semaphore(10)  # Limit concurrent video processing

        _LOGGER.debug(f"Initializing YouTubeAPI with current_quota: {self.current_quota}, quota_reset_time: {self.quota_reset_time}")
        # _LOGGER.critical(f"YouTubeAPI initialized with channel_ids: {self.channel_ids}")
        # _LOGGER.critical(f"YouTubeAPI initial quota: {self.current_quota}, initial reset time: {self.quota_reset_time}")
        # _LOGGER.critical(f"YouTubeAPI initialized with channel_ids: {self.channel_ids}, channel_subscription_times: {self.channel_subscription_times}")

    def _process_channel_ids(self, channel_ids):
        if isinstance(channel_ids, list):
            return [ch.strip() for ch in channel_ids if isinstance(ch, str) and ch.strip()]
        elif isinstance(channel_ids, str):
            return [ch.strip() for ch in channel_ids.split(',') if ch.strip()]
        else:
            return []

    async def _load_persistent_data(self):
        _LOGGER.debug("Loading persistent data")
        stored_data = await self._store.async_load()
        if stored_data:
            self.current_quota = stored_data.get("current_quota", 0)
            quota_reset_str = stored_data.get("quota_reset_time")
            if quota_reset_str:
                parsed_quota_reset_time = datetime.fromisoformat(quota_reset_str)
                if parsed_quota_reset_time.tzinfo is None:
                    self.quota_reset_time = parsed_quota_reset_time.replace(tzinfo=ZoneInfo("UTC"))
                else:
                    self.quota_reset_time = parsed_quota_reset_time.astimezone(ZoneInfo("UTC"))
                
                current_time = datetime.now(ZoneInfo("UTC"))
                _LOGGER.debug(f"Loaded quota_reset_time (UTC): {self.quota_reset_time}, Current time (UTC): {current_time}")
                if current_time >= self.quota_reset_time:
                    _LOGGER.info("Stored quota reset time has passed. Resetting quota.")
                    self.current_quota = 0
                    self.quota_reset_time = None
            else:
                _LOGGER.debug("No quota_reset_time found in stored data. Leaving it as None.")
                self.quota_reset_time = None
            self.fetched_video_ids = set(stored_data.get("fetched_video_ids", []))
            self.refresh_token = stored_data.get("refresh_token", self.refresh_token)
            self.access_token = stored_data.get("access_token")
            self.subscriptions = stored_data.get("subscriptions", [])
            last_sub_update = stored_data.get("last_subscription_update")
            self.last_subscription_update = datetime.fromisoformat(last_sub_update).astimezone(ZoneInfo("UTC")) if last_sub_update else None
            last_sync = stored_data.get("last_sync_time")
            self.last_sync_time = datetime.fromisoformat(last_sync).astimezone(ZoneInfo("UTC")) if last_sync else None
            self.channel_playlist_mapping = stored_data.get("channel_playlist_mapping", {})
            self.channel_ids = stored_data.get("channel_ids", [])
            self.playlist_cache = stored_data.get("playlist_cache", {})
            token_expiration = stored_data.get("token_expiration")
            self.token_expiration = datetime.fromisoformat(token_expiration).astimezone(ZoneInfo("UTC")) if token_expiration else None
            self.channel_subscription_times = {k: datetime.fromisoformat(v).replace(tzinfo=ZoneInfo("UTC")) 
                                            for k, v in stored_data.get("channel_subscription_times", {}).items()}
            self.last_subscription_renewal_time = datetime.fromisoformat(stored_data.get("last_subscription_renewal_time")).astimezone(ZoneInfo("UTC")) if stored_data.get("last_subscription_renewal_time") else None
            
            if self.config_entry:
                coordinator_data = stored_data.get("coordinator_data")
                if coordinator_data:
                    coordinator = self.hass.data.get(DOMAIN, {}).get(f"{self.config_entry.entry_id}_coordinator")
                    if coordinator:
                        coordinator.data = coordinator_data
                    else:
                        _LOGGER.error(f"Coordinator not found for entry {self.config_entry.entry_id}.")
                        return False
        else:
            _LOGGER.debug("No persistent data found. Starting with default values.")
            self.current_quota = 0
            self.quota_reset_time = None
            self.playlist_cache = {}
            self.channel_subscription_times = {}
            self.last_subscription_renewal_time = None
            self.subscriptions = []
            self.fetched_video_ids = set()
            self.channel_ids = []
            self.channel_playlist_mapping = {}
            self.access_token = None

        _LOGGER.debug(f"Final current_quota after loading persistent data: {self.current_quota}")

        self.subscriptions = self.subscriptions or []
        self.channel_ids = self.channel_ids or []
        self.fetched_video_ids = self.fetched_video_ids or set()
        self.channel_playlist_mapping = self.channel_playlist_mapping or {}
        self.playlist_cache = self.playlist_cache or {}
        self.channel_subscription_times = self.channel_subscription_times or {}

        _LOGGER.debug(f"Loaded subscriptions count: {len(self.subscriptions)}")
        _LOGGER.debug(f"Loaded channel_ids count: {len(self.channel_ids)}")
        _LOGGER.debug(f"Loaded fetched_video_ids count: {len(self.fetched_video_ids)}")
        _LOGGER.debug(f"Loaded channel_playlist_mapping count: {len(self.channel_playlist_mapping)}")

    async def _save_persistent_data(self):
        data = {
            "current_quota": self.current_quota,
            "quota_reset_time": self.quota_reset_time.isoformat() if self.quota_reset_time else None,
            "fetched_video_ids": list(self.fetched_video_ids),
            "refresh_token": self.refresh_token,
            "channel_ids": self.channel_ids,
            "subscriptions": self.subscriptions,
            "last_subscription_update": self.last_subscription_update.isoformat() if self.last_subscription_update else None,
            "last_sync_time": self.last_sync_time.isoformat() if self.last_sync_time else None,
            "channel_playlist_mapping": self.channel_playlist_mapping,
            "playlist_cache": self.playlist_cache,
            "token_expiration": self.token_expiration.isoformat() if self.token_expiration else None,
            "channel_subscription_times": {k: v.isoformat() for k, v in self.channel_subscription_times.items()},
            "last_subscription_renewal_time": self.last_subscription_renewal_time.isoformat() if self.last_subscription_renewal_time else None,
            "webhook_id": WEBHOOK_ID,
            "last_webhook_time": self.last_webhook_time.isoformat() if hasattr(self, 'last_webhook_time') else None,
            "access_token": self.access_token
        }

        if self.config_entry:
            data.update({
                "max_regular_videos": self.config_entry.options.get(CONF_MAX_REGULAR_VIDEOS, DEFAULT_MAX_REGULAR_VIDEOS),
                "max_short_videos": self.config_entry.options.get(CONF_MAX_SHORT_VIDEOS, DEFAULT_MAX_SHORT_VIDEOS),
            })
            coordinator = self.hass.data.get(DOMAIN, {}).get(f"{self.config_entry.entry_id}_coordinator")
            if coordinator and hasattr(coordinator, 'data'):
                data.update(coordinator.data)

        await self._store.async_save(data)

    def _calculate_quota_reset_time(self):
        pacific = ZoneInfo("America/Los_Angeles")
        now_pacific = datetime.now(pacific)
        next_reset_pacific = now_pacific.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        quota_reset_time_utc = next_reset_pacific.astimezone(ZoneInfo("UTC"))
        _LOGGER.debug(f"Calculated quota reset time (PST): {next_reset_pacific}, in UTC: {quota_reset_time_utc}")
        return quota_reset_time_utc

    async def schedule_quota_reset(self):
        _LOGGER.debug("Entering schedule_quota_reset method")
        if self.quota_reset_task and not self.quota_reset_task.done():
            _LOGGER.debug("Quota reset task is already scheduled.")
            return

        self.quota_reset_time = self._calculate_quota_reset_time()
        await self._save_persistent_data()
        
        _LOGGER.info(f"Scheduling quota reset at {self.quota_reset_time} (UTC)")

        async def reset_quota():
            while True:
                try:
                    now = datetime.now(ZoneInfo("UTC"))
                    sleep_time = (self.quota_reset_time - now).total_seconds()
                    if sleep_time < 0:
                        self.quota_reset_time = self._calculate_quota_reset_time()
                        sleep_time = (self.quota_reset_time - now).total_seconds()
                    
                    _LOGGER.debug(f"Waiting {sleep_time} seconds until quota reset at {self.quota_reset_time}")
                    await asyncio.sleep(sleep_time)
                    
                    self.current_quota = 0
                    _LOGGER.info(f"Quota reset to 0 at {datetime.now(ZoneInfo('UTC'))}")
                    await self._save_persistent_data()
                    
                    self.hass.bus.async_fire("youtube_quota_reset")
                    _LOGGER.debug("Fired 'youtube_quota_reset' event")
                    
                    self.quota_reset_time = self._calculate_quota_reset_time()
                    _LOGGER.info(f"Next quota reset scheduled for {self.quota_reset_time}")
                except asyncio.CancelledError:
                    _LOGGER.warning("Quota reset task was cancelled.")
                    break
                except Exception as e:
                    _LOGGER.error(f"Error during quota reset: {e}", exc_info=True)
                    await asyncio.sleep(60)  # Retry after a short delay

        self.quota_reset_task = asyncio.create_task(reset_quota())
        _LOGGER.debug("Quota reset task has been created and scheduled.")

    async def schedule_subscription_renewal(self, callback_url):
        _LOGGER.debug("Entering schedule_subscription_renewal method")
        if self.subscription_renewal_task and not self.subscription_renewal_task.done():
            _LOGGER.debug("Subscription renewal task is already scheduled.")
            return

        async def renew_subscription():
            while True:
                try:
                    now = datetime.now(ZoneInfo("UTC"))
                    if self.last_subscription_renewal_time:
                        time_since_last_renewal = now - self.last_subscription_renewal_time
                        sleep_time = max(0, (timedelta(hours=23, minutes=50) - time_since_last_renewal).total_seconds())
                    else:
                        sleep_time = 0  # Renew immediately if never renewed before

                    _LOGGER.debug(f"Sleeping for {sleep_time} seconds until next subscription renewal.")
                    await asyncio.sleep(sleep_time)

                    # Renew subscription
                    _LOGGER.debug("Renewing PubSubHubbub subscription")
                    await self.subscribe_to_pubsub(callback_url)
                    self.last_subscription_renewal_time = datetime.now(ZoneInfo("UTC"))
                    await self._save_persistent_data()
                    _LOGGER.info(f"Subscription renewed at {self.last_subscription_renewal_time}")

                except asyncio.CancelledError:
                    _LOGGER.warning("Subscription renewal task was cancelled.")
                    break
                except Exception as e:
                    _LOGGER.error(f"Error during subscription renewal: {e}")
                    await asyncio.sleep(300)  # Retry after 5 minutes

        if not self.last_subscription_renewal_time or (datetime.now(ZoneInfo("UTC")) - self.last_subscription_renewal_time) > timedelta(hours=23, minutes=50):
            _LOGGER.info("Initiating immediate subscription renewal")
            await self.subscribe_to_pubsub(callback_url)
            self.last_subscription_renewal_time = datetime.now(ZoneInfo("UTC"))
            await self._save_persistent_data()

        self.subscription_renewal_task = asyncio.create_task(renew_subscription())
        _LOGGER.debug("Subscription renewal task has been created and scheduled.")

    async def initialize(self):
        try:
            _LOGGER.debug(f"Initializing YouTubeAPI. Starting quota: {self.current_quota}")
            _LOGGER.critical(f"Starting initialize with channel_ids: {len(self.channel_ids)}")

            self.access_token = None
            self.token_expiration = None
            
            # Store initial channels before loading persistent data
            initial_channels = self.channel_ids.copy() if self.channel_ids else []
            _LOGGER.critical(f"Stored initial channels: {len(initial_channels)} channels")
            
            # Load persistent data
            stored_data = await self._store.async_load()
            if stored_data:
                # Load everything except channel_ids
                self.current_quota = stored_data.get("current_quota", 0)
                quota_reset_str = stored_data.get("quota_reset_time")
                if quota_reset_str:
                    self.quota_reset_time = datetime.fromisoformat(quota_reset_str).replace(tzinfo=ZoneInfo("UTC"))
                self.fetched_video_ids = set(stored_data.get("fetched_video_ids", []))
                self.refresh_token = stored_data.get("refresh_token", self.refresh_token)
                # ... other data loading
                
            # Always restore initial channels if they exist
            if initial_channels:
                _LOGGER.critical("Restoring initial channels")
                self.channel_ids = initial_channels
                await self._save_persistent_data()
                
            _LOGGER.critical(f"Channel IDs after initialization: {len(self.channel_ids)} channels")
            
            if self.current_quota is None:
                self.current_quota = 0
                _LOGGER.debug("Setting current_quota to 0 as it was None")

            _LOGGER.debug(f"After loading persistent data - Quota: {self.current_quota}, Reset time: {self.quota_reset_time}")
            _LOGGER.debug(f"Processed channel_ids: {self.channel_ids}")
            
            # Check if quota reset time has passed
            current_time = datetime.now(ZoneInfo("UTC"))
            if self.quota_reset_time and current_time >= self.quota_reset_time:
                _LOGGER.info("Quota reset time has passed. Resetting quota.")
                self.current_quota = 0
                self.quota_reset_time = self._calculate_quota_reset_time()
                await self._save_persistent_data()
            
            # Reset quota if it's above 10000
            if self.current_quota > 10000:
                _LOGGER.warning(f"Stored quota ({self.current_quota}) exceeds maximum. Resetting to 0.")
                self.current_quota = 0
                await self._save_persistent_data()
            
            _LOGGER.debug("Ensuring valid OAuth token")
            valid_token = await self.ensure_valid_token()
            if not valid_token:
                _LOGGER.error("Failed to ensure valid OAuth token during initialization")
                return False

            self.access_token = valid_token
            _LOGGER.debug("OAuth token validated successfully")

            # Schedule quota reset
            asyncio.create_task(self.schedule_quota_reset())
            _LOGGER.debug("Quota reset task scheduled")

            return True

        except Exception as e:
            _LOGGER.error(f"Unexpected error during initialization: {e}", exc_info=True)
            return False

    async def get_authorization_url(self):
        try:
            base_url = "https://accounts.google.com/o/oauth2/auth"
            redirect_uri = "urn:ietf:wg:oauth:2.0:oob"
            scope = "https://www.googleapis.com/auth/youtube.readonly"
            
            params = {
                "client_id": self.client_id,
                "redirect_uri": redirect_uri,
                "response_type": "code",
                "scope": scope,
                "access_type": "offline",
            }

            authorization_url = f"{base_url}?client_id={params['client_id']}&redirect_uri={params['redirect_uri']}&response_type={params['response_type']}&scope={params['scope']}&access_type={params['access_type']}"

            _LOGGER.debug("Authorization URL generated: %s", authorization_url)
            return authorization_url
        except Exception as e:
            _LOGGER.error(f"Error generating authorization URL: {e}")
            raise

    async def perform_oauth2_flow(self, auth_code):
        token_url = "https://oauth2.googleapis.com/token"
        redirect_uri = "urn:ietf:wg:oauth:2.0:oob"

        _LOGGER.warning(f"Attempting to exchange auth code. Length of code: {len(auth_code)}")
        _LOGGER.warning(f"Auth code (first/last 4 chars): {auth_code[:4]}...{auth_code[-4:]}")

        data = {
            "code": auth_code,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(token_url, data=data) as response:
                    if response.status == 200:
                        result = await response.json()
                        self.refresh_token = result.get("refresh_token")
                        self.access_token = result.get("access_token")
                        _LOGGER.debug("Refresh token obtained: %s", self.refresh_token)
                        return self.refresh_token
                    else:
                        error_message = await response.text()
                        _LOGGER.error("Failed to exchange auth code: %s, error: %s", response.status, error_message)
                        raise ValueError("OAuth token exchange failed")
        except aiohttp.ClientError as err:
            _LOGGER.error("Error during OAuth2 flow: %s", err)
            return None

    async def get_recent_videos(self):
        # _LOGGER.critical("Entering get_recent_videos method.")
        
        current_time = datetime.now(ZoneInfo("UTC"))
        if self.quota_reset_time and current_time >= self.quota_reset_time:
            self.current_quota = 0
            self.quota_reset_time = self._calculate_quota_reset_time()
            await self._save_persistent_data()
            _LOGGER.info(f"Quota reset to 0. Next reset time: {self.quota_reset_time}")
        
        if self.current_quota >= 10000:
            # _LOGGER.critical(f"Quota exceeded; skipping API calls until reset time at {self.quota_reset_time}.")
            message = f"API quota exceeded. Further attempts will resume after the quota resets at {self.quota_reset_time}"
            persistent_notification.async_create(
                self.hass,
                message,
                title="YouTube API Quota Exceeded",
                notification_id="youtube_quota_exceeded"
            )
            raise QuotaExceededException(message)

        valid_token = await self.ensure_valid_token()
        if not valid_token:
            _LOGGER.critical("Failed to ensure valid access token; aborting API calls.")
            return {"data": [], "shorts_data": []}

        self.access_token = valid_token

        videos = {"data": [], "shorts_data": []}
        try:
            async with aiohttp.ClientSession() as session:
                headers = {"Authorization": f"Bearer {self.access_token}"}
                _LOGGER.debug("Fetching subscriptions")
                subscriptions = await self._fetch_subscriptions(session, headers)
                _LOGGER.debug(f"Fetched {len(subscriptions)} subscriptions")

                if subscriptions:
                    _LOGGER.debug("Fetching uploads playlists")
                    uploads_playlists = await self._fetch_uploads_playlists(session, headers, subscriptions)
                    _LOGGER.debug(f"Fetched {len(uploads_playlists)} uploads playlists")
                    _LOGGER.debug("Fetching activities")
                    videos = await self._fetch_activities(session, headers, uploads_playlists)
                    _LOGGER.debug(f"Fetched {len(videos.get('data', []))} regular videos and {len(videos.get('shorts_data', []))} shorts")
                else:
                    _LOGGER.debug("No subscriptions found")

            _LOGGER.critical(f"Fetched a total of {len(videos.get('data', []))} regular videos and {len(videos.get('shorts_data', []))} shorts across all subscriptions")
            # _LOGGER.debug(f"Videos returned from get_recent_videos: {videos}")
            return videos
        except QuotaExceededException as qee:
            _LOGGER.warning(f"Quota exceeded during video fetch: {qee}")
            raise
        except aiohttp.ClientError as err:
            _LOGGER.critical(f"Network error fetching subscribed videos: {err}")
            return {"data": [], "shorts_data": []}
        except Exception as e:
            _LOGGER.error(f"Unexpected error in get_recent_videos: {e}", exc_info=True)
            return {"data": [], "shorts_data": []}

    async def get_video_by_id(self, video_id):
        _LOGGER.debug(f"Attempting to fetch video details for ID: {video_id}")
        if self.current_quota >= 10000:
            message = f"API quota exceeded. Further attempts will resume after the quota resets at {self.quota_reset_time}"
            _LOGGER.warning(message)
            raise QuotaExceededException(message)
        
        await self.ensure_valid_token()
        url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,contentDetails,statistics,liveStreamingDetails&id={video_id}&key={self.api_key}"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=10) as response:
                    response_text = await response.text()
                    if response.status == 200:
                        data = json.loads(response_text)
                        items = data.get("items", [])
                        if items:
                            stored_data = await self._store.async_load()
                            oldest_video_date = None
                            if stored_data:
                                all_videos = stored_data.get('data', []) + stored_data.get('shorts_data', [])
                                publish_dates = [video.get('snippet', {}).get('publishedAt') for video in all_videos if video.get('snippet')]
                                if publish_dates:
                                    oldest_video_date = min(datetime.fromisoformat(date.rstrip('Z')).replace(tzinfo=ZoneInfo("UTC")) 
                                                    for date in publish_dates if date)
                            
                            item = items[0]
                            video_published_at_str = item['snippet']['publishedAt']
                            video_published_at = datetime.strptime(video_published_at_str, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=ZoneInfo("UTC"))

                            # Check for live stream with expanded criteria
                            live_broadcast_content = item['snippet'].get('liveBroadcastContent', 'none')
                            duration = item['contentDetails'].get('duration', 'PT0M0S')
                            live_streaming_details = item.get('liveStreamingDetails', {})
                            
                            # Get live streaming times
                            actual_start_time = live_streaming_details.get('actualStartTime')
                            actual_end_time = live_streaming_details.get('actualEndTime')
                            scheduled_start_time = live_streaming_details.get('scheduledStartTime')
                            
                            # Determine if this is a live stream
                            is_live_stream = (live_broadcast_content in ['live', 'upcoming'] or 
                                            duration == 'P0D' or 
                                            actual_start_time is not None or
                                            scheduled_start_time is not None)

                            # Set to True to exclude streams that haven't started yet globally
                            EXCLUDE_FUTURE_LIVE_STREAMS = True
                            
                            # Set live status based on stream state
                            live_status = ""
                            if is_live_stream:
                                if actual_start_time and not actual_end_time:
                                    live_status = "🔴 LIVE&thinsp;&thinsp;(streaming)"
                                    _LOGGER.warning(f"Live stream detected for video {video_id} - Currently streaming")
                                elif actual_end_time:
                                    live_status = "📴 Stream ended"
                                    _LOGGER.warning(f"Live stream detected for video {video_id} - Stream ended at {actual_end_time}")
                                elif scheduled_start_time:
                                    if EXCLUDE_FUTURE_LIVE_STREAMS:
                                        _LOGGER.debug(f"Skipping scheduled stream {video_id} - Not yet started")
                                        return None
                                    live_status = "⏰ Streaming soon"
                                    _LOGGER.warning(f"Live stream detected for video {video_id} - Stream scheduled for {scheduled_start_time}")

                            if not is_live_stream:
                                midnight = datetime.now(ZoneInfo("America/Los_Angeles")).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(ZoneInfo("UTC"))
                                if video_published_at < midnight:
                                    return None
                                if oldest_video_date and video_published_at < oldest_video_date:
                                    return None
                                
                            self.current_quota += 3

                            if is_live_stream and not actual_end_time:
                                runtime = ""
                            else:
                                try:
                                    duration_timedelta = parse_duration(duration)
                                    runtime = str(max(1, int(duration_timedelta.total_seconds() / 60)))
                                except (ValueError, TypeError):
                                    _LOGGER.warning(f"Invalid duration for video {video_id}: {duration}")
                                    runtime = ""

                            video_data = {
                                'id': item['id'],
                                'snippet': item['snippet'],
                                'contentDetails': item['contentDetails'],
                                'statistics': item.get('statistics', {}),
                                'runtime': runtime,
                                'genres': ', '.join(' '.join(word.capitalize() for word in tag.split()) for tag in item['snippet'].get('tags', [])[:2]),
                                # 'genres': ', '.join(item['snippet'].get('tags', [])[:2]),
                                'live_status': live_status
                            }
                                
                            return video_data
                        else:
                            _LOGGER.warning(f"No video details found for video ID: {video_id}")
                            return None
                    elif response.status == 403:
                        await self._handle_quota_exceeded(response)
                    elif response.status == 401:
                        _LOGGER.warning("Access token expired or unauthorized during video details fetch. Attempting to refresh token.")
                        refresh_result = await self.refresh_oauth_token()
                        if refresh_result:
                            headers["Authorization"] = f"Bearer {self.access_token}"
                            return await self.get_video_by_id(video_id)
                        else:
                            _LOGGER.error("Failed to refresh access token after 401 error during video details fetch.")
                    else:
                        _LOGGER.error(f"Failed to fetch video details for {video_id}. Status: {response.status}, Error: {response_text}")
        except Exception as e:
            _LOGGER.error(f"Exception while fetching video details for {video_id}: {e}")
        return None

    async def _fetch_subscriptions(self, session, headers):
        current_time = datetime.now(ZoneInfo("UTC"))
        if self.quota_reset_time and current_time >= self.quota_reset_time:
            self.current_quota = 0
            await self._save_persistent_data()
            _LOGGER.info(f"Quota reset to 0. Next reset time: {self.quota_reset_time}")
        
        if self.subscriptions and self.last_subscription_update and (current_time - self.last_subscription_update).days < 1:
            _LOGGER.info("Using cached subscriptions data")
            return self.subscriptions

        _LOGGER.info("Fetching fresh subscriptions data from API")
        subscriptions = []
        page_token = None
        while True:
            url = f"https://www.googleapis.com/youtube/v3/subscriptions?part=snippet&mine=true&maxResults=50&key={self.api_key}"
            if page_token:
                url += f"&pageToken={page_token}"

            try:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.current_quota += 1  # subscriptions.list costs 1 unit
                        _LOGGER.debug(f"Quota incremented by 1 unit. Current quota usage: {self.current_quota}")
                        await self._save_persistent_data()
                        channel_ids = [item['snippet']['resourceId']['channelId'] for item in data.get('items', [])]
                        subscriptions.extend(channel_ids)

                        page_token = data.get('nextPageToken')
                        if not page_token:
                            break
                    elif response.status == 401:
                        _LOGGER.warning("Access token expired or unauthorized during subscriptions fetch. Attempting to refresh token.")
                        refresh_result = await self.refresh_oauth_token()
                        if refresh_result:
                            headers["Authorization"] = f"Bearer {self.access_token}"
                            continue
                        else:
                            _LOGGER.error("Failed to refresh access token after 401 error during subscriptions fetch.")
                            break
                    elif response.status == 403:
                        await self._handle_quota_exceeded(response)
                        break
                    else:
                        _LOGGER.error(f"Failed to fetch subscriptions. Status: {response.status}")
                        break
            except Exception as e:
                _LOGGER.error(f"Error fetching subscriptions: {e}")
                break

        self.subscriptions = subscriptions
        self.last_subscription_update = current_time
        await self._save_persistent_data()
        return subscriptions

    async def _fetch_activities(self, session, headers, uploads_playlists):
        regular_videos = []
        shorts = []
        stored_data = await self._store.async_load()
        oldest_video_date = None
        
        if stored_data:
            all_videos = stored_data.get('data', []) + stored_data.get('shorts_data', [])
            publish_dates = [video.get('snippet', {}).get('publishedAt') for video in all_videos if video.get('snippet')]
            if publish_dates:
                oldest_video_date = min(datetime.fromisoformat(date.rstrip('Z')).replace(tzinfo=ZoneInfo("UTC")) 
                                    for date in publish_dates if date)

        all_video_ids = set()
        video_id_to_item = {}

        for channel_id, playlist_id in list(uploads_playlists.items())[:50]:
            if self.current_quota >= 9900:
                _LOGGER.warning("Approaching quota limit. Stopping fetch.")
                break

            playlist_url = f"https://www.googleapis.com/youtube/v3/playlistItems?part=snippet,contentDetails&maxResults=50&playlistId={playlist_id}&key={self.api_key}"

            retry_count = 0
            max_retries = 3
            while retry_count < max_retries:
                try:
                    async with async_timeout.timeout(60):
                        async with session.get(playlist_url, headers=headers) as response:
                            if response.status == 200:
                                data = await response.json()
                                self.current_quota += 1
                                await self._save_persistent_data()
                                
                                for item in data.get('items', []):
                                    video_published_at_str = item['snippet']['publishedAt']
                                    video_published_at = datetime.strptime(video_published_at_str, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=ZoneInfo("UTC"))

                                    midnight = datetime.now(ZoneInfo("America/Los_Angeles")).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(ZoneInfo("UTC"))
                                    if video_published_at >= midnight and (not oldest_video_date or video_published_at >= oldest_video_date):
                                        video_id = item['snippet']['resourceId']['videoId']
                                        if video_id not in self.fetched_video_ids:
                                            all_video_ids.add(video_id)
                                            video_id_to_item[video_id] = item
                                break
                            elif response.status == 403:
                                quota_exceeded = await self._handle_quota_exceeded(response)
                                if quota_exceeded:
                                    raise QuotaExceededException()
                                break
                            elif response.status == 401:
                                refresh_result = await self.refresh_oauth_token()
                                if refresh_result:
                                    headers["Authorization"] = f"Bearer {self.access_token}"
                                    retry_count += 1
                                    continue
                                else:
                                    break
                            else:
                                retry_count += 1
                except asyncio.TimeoutError:
                    retry_count += 1
                    if retry_count < max_retries:
                        await asyncio.sleep(1)
                    continue
                except Exception as e:
                    _LOGGER.error(f"Error fetching playlist {playlist_id}: {e}")
                    break

            await asyncio.sleep(0.1)

        video_ids_to_check = list(all_video_ids)
        batch_size = 50
        for i in range(0, len(video_ids_to_check), batch_size):
            batch = video_ids_to_check[i:i + batch_size]
            videos_url = f"https://www.googleapis.com/youtube/v3/videos?part=contentDetails,snippet,statistics,liveStreamingDetails&id={','.join(batch)}&key={self.api_key}"
            try:
                async with async_timeout.timeout(60):
                    async with session.get(videos_url, headers=headers) as response:
                        if response.status == 200:
                            data = await response.json()
                            items = data.get('items', [])
                            if not items:
                                continue
                            self.current_quota += 2
                            await self._save_persistent_data()
                            for item in items:
                                video_id = item['id']
                                duration = item['contentDetails'].get('duration', 'PT0M0S')
                                
                                # Check if it's a live stream - expanded check
                                live_broadcast_content = item['snippet'].get('liveBroadcastContent', 'none')
                                live_streaming_details = item.get('liveStreamingDetails', {})
                                is_live_stream = (live_broadcast_content in ['live', 'upcoming'] or 
                                                duration == 'P0D' or 
                                                live_streaming_details.get('actualStartTime') is not None or
                                                live_streaming_details.get('scheduledStartTime') is not None)
                                
                                if is_live_stream:
                                    # Add to regular videos if it's a live stream
                                    async with self.fetched_video_ids_lock:
                                        if video_id not in self.fetched_video_ids:
                                            self.fetched_video_ids.add(video_id)
                                            live_status = ""
                                            runtime = ""
                                            if live_streaming_details.get('actualStartTime') and not live_streaming_details.get('actualEndTime'):
                                                live_status = "🔴 LIVE&thinsp;&thinsp;(streaming)"
                                                _LOGGER.warning(f"Live stream detected for video {video_id} - Currently streaming")
                                            elif live_streaming_details.get('actualEndTime'):
                                                live_status = "📴 Stream ended"
                                                _LOGGER.warning(f"Live stream detected for video {video_id} - Stream ended at {live_streaming_details.get('actualEndTime')}")
                                                try:
                                                    duration_timedelta = parse_duration(duration)
                                                    duration_seconds = int(duration_timedelta.total_seconds())
                                                    runtime = str(max(1, int(duration_seconds / 60)))
                                                except (ValueError, TypeError):
                                                    runtime = ""
                                            elif live_streaming_details.get('scheduledStartTime'):
                                                live_status = "⏰ Streaming soon"
                                                _LOGGER.warning(f"Live stream detected for video {video_id} - Stream scheduled")
                                            video_data = {
                                                'id': video_id,
                                                'snippet': item.get('snippet', {}),
                                                'contentDetails': item['contentDetails'],
                                                'duration': duration,
                                                'runtime': runtime,
                                                'live_status': live_status
                                            }
                                            regular_videos.append(video_data)
                                else:
                                    try:
                                        duration_timedelta = parse_duration(duration)
                                        duration_seconds = int(duration_timedelta.total_seconds())
                                    except (ValueError, TypeError):
                                        duration_seconds = 0

                                    is_short = duration_seconds <= 60 and not is_live_stream
                                    async with self.fetched_video_ids_lock:
                                        if video_id not in self.fetched_video_ids:
                                            self.fetched_video_ids.add(video_id)
                                            if is_short:
                                                playlist_item = video_id_to_item.get(video_id, {})
                                                short_video_data = {
                                                    'id': video_id,
                                                    'snippet': playlist_item.get('snippet', {}),
                                                    'contentDetails': item['contentDetails'],
                                                    'duration': duration
                                                }
                                                shorts.append(short_video_data)
                                            else:
                                                video_data = {
                                                    'id': video_id,
                                                    'snippet': playlist_item.get('snippet', {}),
                                                    'contentDetails': item['contentDetails'],
                                                    'statistics': {'viewCount': '0', 'likeCount': '0', 'commentCount': '0'},
                                                    'duration': duration,
                                                    'runtime': str(max(1, int(duration_seconds / 60)))
                                                }
                                                regular_videos.append(video_data)
                        elif response.status == 403:
                            quota_exceeded = await self._handle_quota_exceeded(response)
                            if quota_exceeded:
                                raise QuotaExceededException()
                        elif response.status == 401:
                            refresh_result = await self.refresh_oauth_token()
                            if refresh_result:
                                headers["Authorization"] = f"Bearer {self.access_token}"
                                continue
                            else:
                                break
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                _LOGGER.error(f"Error processing video batch: {e}")
                continue

            await asyncio.sleep(0.1)

        return {"data": regular_videos, "shorts_data": shorts}

    async def _handle_quota_exceeded(self, response):
        # _LOGGER.critical("Entering _handle_quota_exceeded method")
        error = await response.json()
        _LOGGER.critical(f"Full error response: {error}")
        if error.get("error", {}).get("errors"):
            reason = error["error"]["errors"][0].get("reason")
            _LOGGER.critical(f"Error reason: {reason}")
            if reason == "quotaExceeded":
                self.quota_reset_time = self._calculate_quota_reset_time()
                self.current_quota = 10000
                _LOGGER.critical(f"YouTube API quota exceeded. Quota reset scheduled at {self.quota_reset_time}")
                await self._save_persistent_data()
                message = f"YouTube API quota exceeded. Further attempts will resume after the quota resets at {self.quota_reset_time}"
                persistent_notification.async_create(
                    self.hass,
                    message,
                    title="YouTube API Quota Exceeded",
                    notification_id="youtube_quota_exceeded"
                )
                return True
            else:
                _LOGGER.critical(f"Received error, but not quota exceeded. Reason: {reason}")
        _LOGGER.critical("Exiting _handle_quota_exceeded method")
        return False

    async def fetch_video_details(self, video_ids, session, headers):
        # _LOGGER.debug(f"Fetching video details for {len(video_ids)} videos")
        if not video_ids:
            _LOGGER.debug("No video IDs provided. Skipping video details fetch.")
            return []

        detailed_videos = []
        batch_size = 50
        batched_video_ids = [video_ids[i:i + batch_size] for i in range(0, len(video_ids), batch_size)]

        for batch in batched_video_ids:
            batch_videos_url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,contentDetails,statistics&id={','.join(batch)}&key={self.api_key}"

            try:
                async with session.get(batch_videos_url, headers=headers, timeout=60) as response:
                    if response.status == 200:
                        videos_result = await response.json()
                        items = videos_result.get("items", [])
                        if not items:
                            _LOGGER.warning(f"No video details found for video IDs: {batch}")
                            continue
                        self.current_quota += 1  # videos.list costs 1 unit
                        _LOGGER.debug(f"Quota incremented by 1 unit. Current quota usage: {self.current_quota}")
                        await self._save_persistent_data()
                        for item in items:
                            video_id = item['id']
                            duration = item['contentDetails'].get('duration', 'PT0M0S')
                            try:
                                duration_timedelta = parse_duration(duration)
                                runtime = max(1, int(duration_timedelta.total_seconds() / 60))
                            except (ValueError, TypeError):
                                _LOGGER.warning(f"Invalid duration for video {video_id}: {duration}")
                                runtime = 1

                            video_data = {
                                'id': video_id,
                                'snippet': item['snippet'],
                                'contentDetails': item['contentDetails'],
                                'statistics': item['statistics'],
                                'runtime': runtime,
                                'genres': ', '.join(' '.join(word.capitalize() for word in tag.split()) for tag in item['snippet'].get('tags', [])[:2])
                                # 'genres': ', '.join(item['snippet'].get('tags', [])[:2])
                            }
                            detailed_videos.append(video_data)
                    elif response.status == 403:
                        error_content = await response.text()
                        _LOGGER.critical(f"Received 403 status. Error content: {error_content}")
                        quota_exceeded = await self._handle_quota_exceeded(response)
                        if quota_exceeded:
                            _LOGGER.critical("Quota exceeded. Stopping fetch.")
                            return detailed_videos
                    elif response.status == 401:
                        _LOGGER.warning("Access token expired or unauthorized during video details fetch. Attempting to refresh token.")
                        refresh_result = await self.refresh_oauth_token()
                        if refresh_result:
                            headers["Authorization"] = f"Bearer {self.access_token}"
                            continue
                        else:
                            _LOGGER.error("Failed to refresh access token after 401 error during video details fetch.")
                            return detailed_videos
                    else:
                        error_message = await response.text()
                        _LOGGER.error(f"Failed to fetch video details batch. Status: {response.status}, error: {error_message}")
                        continue
            except asyncio.TimeoutError:
                _LOGGER.error("Timeout while fetching video details for batch. Continuing with the next batch.")
                continue
            except Exception as e:
                _LOGGER.error(f"Unexpected error in fetch_video_details: {e}")
                continue

        _LOGGER.debug(f"Fetched details for {len(detailed_videos)} videos")
        return detailed_videos

    async def subscribe_to_pubsub(self, callback_url):
        _LOGGER.critical("Entering subscribe_to_pubsub method")
        # _LOGGER.critical(f"Current channel_subscription_times: {self.channel_subscription_times}")
        # _LOGGER.critical(f"Number of channel_ids: {len(self.channel_ids)}, Number of cached subscriptions: {len(self.channel_subscription_times)}")
        hub_url = "https://pubsubhubbub.appspot.com/subscribe"

        if not self.channel_ids:
            _LOGGER.critical("No channel IDs are defined for subscription.")
            return False

        if not callback_url:
            _LOGGER.critical("Callback URL is not defined.")
            return False

        current_time = datetime.now(ZoneInfo("UTC"))
        # _LOGGER.critical(f"Current time: {current_time}, Quota reset time: {self.quota_reset_time}, Current quota: {self.current_quota}")
        
        if self.current_quota >= 10000:
            _LOGGER.critical(f"API quota exceeded. Skipping PubSubHubbub subscription until reset at {self.quota_reset_time}.")
            return False

        # _LOGGER.critical(f"Starting PubSubHubbub subscription process. Channel IDs: {self.channel_ids}")

        subscription_results = []
        channels_to_subscribe = 0
        for channel_id in self.channel_ids:
            channel_id = channel_id.strip()
            last_subscription_time = self.channel_subscription_times.get(channel_id)
            
            if last_subscription_time:
                time_since_last_subscription = (current_time - last_subscription_time).total_seconds()
                # _LOGGER.critical(f"Channel {channel_id} last subscribed {time_since_last_subscription} seconds ago")
            else:
                time_since_last_subscription = None  # Ensure else block has an indented statement

            if last_subscription_time and (current_time - last_subscription_time).total_seconds() < 86000:  # 23 hours and 53 minutes
                # _LOGGER.critical(f"Subscription for channel {channel_id} is still valid. Skipping.")
                subscription_results.append((channel_id, True))
                continue

            channels_to_subscribe += 1
            topic_url = f"https://www.youtube.com/xml/feeds/videos.xml?channel_id={channel_id}"
            data = {
                'hub.callback': callback_url,
                'hub.topic': topic_url,
                'hub.verify': 'async',
                'hub.mode': 'subscribe',
                'hub.lease_seconds': '86400'
            }

            # _LOGGER.critical(f"Attempting to subscribe to channel {channel_id} with data: {data}")

            try:
                async with aiohttp.ClientSession() as session:
                    # _LOGGER.critical(f"Sending POST request to {hub_url} for channel {channel_id}")
                    async with session.post(hub_url, data=data, timeout=60) as response:
                        response_text = await response.text()
                        # _LOGGER.critical(f"Received response for channel {channel_id}: Status {response.status}, Body: {response_text}")
                        if response.status == 202:
                            # _LOGGER.critical(f"Successfully subscribed to PubSubHubbub for channel {channel_id}")
                            self.channel_subscription_times[channel_id] = current_time
                            subscription_results.append((channel_id, True))
                        else:
                            _LOGGER.critical(f"Failed to subscribe to PubSubHubbub for channel {channel_id}. Status: {response.status}, Error: {response_text}")
                            subscription_results.append((channel_id, False))
            except Exception as e:
                _LOGGER.critical(f"Unexpected error in subscribe_to_pubsub for channel {channel_id}: {str(e)}")
                subscription_results.append((channel_id, False))

        _LOGGER.critical(f"PubSubHubbub subscription process completed. Subscribed to {channels_to_subscribe} channels out of {len(self.channel_ids)}")
        _LOGGER.critical(f"Subscription results summary: {len([r for r in subscription_results if r[1]])} successes, {len([r for r in subscription_results if not r[1]])} failures")
        _LOGGER.critical(f"Final channel_subscription_times: {self.channel_subscription_times}")

        await self._save_persistent_data()
        return subscription_results

    async def unsubscribe_from_pubsub(self, callback_url):
        _LOGGER.debug("Entering unsubscribe_from_pubsub method")
        hub_url = "https://pubsubhubbub.appspot.com/subscribe"

        if not self.channel_ids:
            _LOGGER.error("No channel IDs are defined for unsubscription.")
            return

        if not callback_url:
            _LOGGER.error("Callback URL is not defined.")
            return

        # Ensure channel_ids is always a list
        for channel_id in self.channel_ids:
            channel_id = channel_id.strip()
            topic_url = f"https://www.youtube.com/xml/feeds/videos.xml?channel_id={channel_id}"

            data = {
                'hub.callback': callback_url,
                'hub.topic': topic_url,
                'hub.verify': 'async',
                'hub.mode': 'unsubscribe',
            }

            _LOGGER.debug(f"Attempting to unsubscribe from channel {channel_id} with data: {data}")

            for attempt in range(3):
                try:
                    async with aiohttp.ClientSession() as session:
                        _LOGGER.debug(f"Sending POST request to {hub_url} for channel {channel_id} (Attempt {attempt + 1}/3)")
                        async with session.post(hub_url, data=data, timeout=60) as response:
                            response_text = await response.text()
                            # _LOGGER.debug(f"Received response for channel {channel_id}: Status {response.status}, Body: {response_text}")

                            if response.status == 202:
                                _LOGGER.info(f"Successfully unsubscribed from PubSubHubbub for channel {channel_id}")
                                break  # Exit retry loop on success
                            elif 500 <= response.status < 600:
                                # Transient server error, retry
                                _LOGGER.error(f"Transient error unsubscribing from PubSubHubbub for channel {channel_id}. Status: {response.status}, Error: {response_text}")
                            elif response.status == 403 and 'quotaExceeded' in response_text:
                                self.quota_reset_time = self._calculate_quota_reset_time()
                                _LOGGER.warning(f"API quota exceeded. Unsubscription attempts will not proceed until {self.quota_reset_time}")
                                return  # Exit method as quota is exceeded
                            else:
                                # Non-retriable error
                                _LOGGER.error(f"Failed to unsubscribe from PubSubHubbub for channel {channel_id}. Status: {response.status}, Error: {response_text}")
                                break  # Exit retry loop on non-retriable error
                except asyncio.TimeoutError:
                    _LOGGER.error(f"Timeout error while unsubscribing from PubSubHubbub for channel {channel_id} (Attempt {attempt + 1}/3)")
                except aiohttp.ClientError as err:
                    _LOGGER.error(f"Network error unsubscribing from PubSubHubbub for channel {channel_id}: {err} (Attempt {attempt + 1}/3)")
                except Exception as e:
                    _LOGGER.error(f"Unexpected error in unsubscribe_from_pubsub for channel {channel_id}: {e} (Attempt {attempt + 1}/3)")

                if attempt < 2:
                    _LOGGER.debug(f"Retrying unsubscription for channel {channel_id} after 5 seconds")
                    await asyncio.sleep(5)  # Wait before retrying

        _LOGGER.debug("PubSubHubbub unsubscription process completed")
        await self._save_persistent_data()

    async def refresh_oauth_token(self):
        _LOGGER.debug("Entering refresh_oauth_token method")
        token_url = "https://oauth2.googleapis.com/token"
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token",
        }

        try:
            async with aiohttp.ClientSession() as session:
                _LOGGER.debug(f"Sending refresh token request to {token_url}")
                async with session.post(token_url, data=data, timeout=10) as response:
                    _LOGGER.debug(f"Received response with status {response.status}")
                    if response.status == 200:
                        result = await response.json()
                        new_access_token = result.get("access_token")
                        expires_in = result.get("expires_in", 3600)
                        if not new_access_token:
                            raise ValueError("Received 200 status but no access token in response")
                        self.access_token = new_access_token
                        self.token_expiration = datetime.now(ZoneInfo("UTC")) + timedelta(seconds=expires_in)
                        await self._save_persistent_data()
                        _LOGGER.debug("Successfully refreshed access token")
                        return new_access_token
                    else:
                        error_text = await response.text()
                        raise ValueError(f"Failed to refresh access token. Status: {response.status}, Error: {error_text}")
        except Exception as e:
            _LOGGER.error(f"Error refreshing OAuth token: {str(e)}")
            self.access_token = None
            self.token_expiration = None
            await self._save_persistent_data()
            raise

    async def ensure_valid_token(self):
        try:
            _LOGGER.debug("Entering ensure_valid_token method")
            if not self.refresh_token:
                _LOGGER.error("No refresh token available")
                return None
                
            if self.token_expiration and datetime.now(ZoneInfo("UTC")) < self.token_expiration - timedelta(minutes=5):
                if self.access_token:
                    _LOGGER.debug("Existing token is still valid")
                    return self.access_token
                    
            _LOGGER.debug("Token expired or not set, attempting to refresh")
            return await self.refresh_oauth_token()
        except Exception as e:
            _LOGGER.error(f"Failed to ensure valid token: {str(e)}")
            try:
                if self.refresh_token:
                    _LOGGER.debug("Attempting token refresh after error")
                    return await self.refresh_oauth_token()
            except Exception as refresh_error:
                _LOGGER.error(f"Failed to refresh token after error: {str(refresh_error)}")
            return None
        
    async def fetch_user_channels(self, refresh_token):
        current_time = datetime.now(ZoneInfo("UTC"))
        _LOGGER.warning(f"Config entry during fetch_user_channels: {self.config_entry}")
        _LOGGER.warning("Debugging call stack - methods being called during fetch_user_channels")
        
        if self.quota_reset_time and current_time >= self.quota_reset_time:
            _LOGGER.warning("Resetting quota during fetch_user_channels")
            self.current_quota = 0
            await self._save_persistent_data()
            _LOGGER.info(f"Quota reset to 0. Next reset time: {self.quota_reset_time}")
        
        max_retries = 3
        retry_delay = 5  # seconds
        channel_ids = []

        for attempt in range(max_retries):
            try:
                # Step 1: Obtain a new access token using the refresh token
                _LOGGER.warning(f"Attempt {attempt + 1}: Starting token refresh")
                async with aiohttp.ClientSession() as session:
                    token_url = "https://oauth2.googleapis.com/token"
                    data = {
                        'client_id': self.client_id,
                        'client_secret': self.client_secret,
                        'refresh_token': refresh_token,
                        'grant_type': 'refresh_token'
                    }

                    _LOGGER.warning("Attempting to get access token")
                    async with session.post(token_url, data=data, timeout=10) as token_response:
                        if token_response.status != 200:
                            error_text = await token_response.text()
                            _LOGGER.warning(f"Token response error: Status {token_response.status} - {error_text}")
                            raise Exception(f"Failed to refresh access token: {token_response.status} - {error_text}")

                        token_data = await token_response.json()
                        access_token = token_data.get('access_token')
                        _LOGGER.warning("Successfully obtained access token")

                        if not access_token:
                            _LOGGER.warning("No access token in response data")
                            raise ValueError("No access token received in response")

                    _LOGGER.warning("Starting subscription fetch")
                    headers = {
                        'Authorization': f'Bearer {access_token}',
                        'Accept': 'application/json',
                    }

                    _LOGGER.warning(f"Current quota before API call: {self.current_quota}")
                    _LOGGER.warning(f"Quota reset time: {self.quota_reset_time}")

                    async with aiohttp.ClientSession() as session:
                        next_page_token = None
                        while True:
                            url = f'https://www.googleapis.com/youtube/v3/subscriptions?part=snippet&mine=true&maxResults=50'
                            if next_page_token:
                                url += f'&pageToken={next_page_token}'

                            _LOGGER.warning(f"Making API request to: {url}")
                            async with session.get(url, headers=headers, timeout=10) as response:
                                _LOGGER.warning(f"API response status: {response.status}")
                                if response.status == 200:
                                    data = await response.json()
                                    _LOGGER.warning(f"API response received. Items count: {len(data.get('items', []))}")
                                    _LOGGER.warning("About to increment quota")
                                    try:
                                        old_quota = self.current_quota
                                        self.current_quota += 5
                                        _LOGGER.warning(f"Quota incremented from {old_quota} to {self.current_quota}")
                                    except Exception as quota_error:
                                        _LOGGER.error(f"Error incrementing quota: {quota_error}")
                                        raise
                                    
                                    await self._save_persistent_data()
                                    channel_ids.extend([item['snippet']['resourceId']['channelId'] for item in data.get('items', [])])
                                    next_page_token = data.get('nextPageToken')
                                    if not next_page_token:
                                        _LOGGER.warning("No more pages to fetch")
                                        break
                                elif response.status == 401:
                                    _LOGGER.warning("Access token expired or unauthorized during channel fetch")
                                    refresh_result = await self.refresh_oauth_token()
                                    if refresh_result:
                                        _LOGGER.warning("Token refreshed successfully, continuing fetch")
                                        headers['Authorization'] = f'Bearer {self.access_token}'
                                        continue
                                    else:
                                        _LOGGER.error("Failed to refresh access token after 401 error")
                                        break
                                elif response.status == 403:
                                    _LOGGER.warning("Received 403 response, checking for quota exceeded")
                                    quota_exceeded = await self._handle_quota_exceeded(response)
                                    if quota_exceeded:
                                        raise QuotaExceededException(f"API quota exceeded. Further attempts will resume after the quota resets at {self.quota_reset_time}")
                                else:
                                    error_text = await response.text()
                                    _LOGGER.warning(f"Unexpected response: Status {response.status} - {error_text}")
                                    raise Exception(f"Failed to fetch subscriptions: {response.status} - {error_text}")

                        _LOGGER.warning(f"Fetch complete. Total channels found: {len(channel_ids)}")
                        if not channel_ids:
                            _LOGGER.warning("No subscribed channels found for the user")

                        return channel_ids

            except QuotaExceededException as qee:
                _LOGGER.warning(f"Quota exceeded during user channels fetch: {qee}")
                raise
            except Exception as e:
                _LOGGER.error(f"Error in fetch_user_channels (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    _LOGGER.warning(f"Retrying in {retry_delay} seconds")
                    await asyncio.sleep(retry_delay)
                else:
                    raise

        _LOGGER.error(f"Failed to fetch user channels after {max_retries} attempts")
        return channel_ids

    async def _fetch_uploads_playlists(self, session, headers, channel_ids):
        current_time = datetime.now(ZoneInfo("UTC"))
        if self.quota_reset_time and current_time >= self.quota_reset_time:
            self.current_quota = 0
            await self._save_persistent_data()
            _LOGGER.info(f"Quota reset to 0. Next reset time: {self.quota_reset_time}")
        
        uploads_playlists = {}
        channels_to_remove = []
        channels_to_fetch = [cid for cid in channel_ids if cid not in self.channel_playlist_mapping]
        
        for i in range(0, len(channels_to_fetch), 50):
            batch = channels_to_fetch[i:i+50]
            channels_url = f"https://www.googleapis.com/youtube/v3/channels?part=contentDetails&id={','.join(batch)}&key={self.api_key}"

            try:
                async with session.get(channels_url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.current_quota += 1  # channels.list costs 1 unit
                        _LOGGER.debug(f"Quota incremented by 1 unit. Current quota usage: {self.current_quota}")
                        await self._save_persistent_data()
                        for item in data.get('items', []):
                            channel_id = item['id']
                            uploads_playlist_id = item['contentDetails']['relatedPlaylists']['uploads']
                            uploads_playlists[channel_id] = uploads_playlist_id
                            self.channel_playlist_mapping[channel_id] = uploads_playlist_id
                        missing_channels = set(batch) - set(item['id'] for item in data.get('items', []))
                        channels_to_remove.extend(missing_channels)
                    elif response.status == 403:
                        await self._handle_quota_exceeded(response)
                        break
                    elif response.status == 401:
                        _LOGGER.warning("Access token expired or unauthorized during channels fetch. Attempting to refresh token.")
                        refresh_result = await self.refresh_oauth_token()
                        if refresh_result:
                            headers["Authorization"] = f"Bearer {self.access_token}"
                            continue
                        else:
                            _LOGGER.error("Failed to refresh access token after 401 error during channels fetch.")
                            break
                    else:
                        error_content = await response.text()
                        _LOGGER.error(f"Failed to fetch channels. Status: {response.status}, Error: {error_content}")
                        _LOGGER.error(f"Problematic URL: {channels_url}")
            except Exception as e:
                _LOGGER.error(f"Error fetching channels: {str(e)}")
                _LOGGER.error(f"Problematic URL: {channels_url}")
                break

        if channels_to_remove:
            _LOGGER.warning(f"Removing non-existent channels: {channels_to_remove}")
            self.channel_ids = [cid for cid in self.channel_ids if cid not in channels_to_remove]
            await self._save_persistent_data()

        return {**self.channel_playlist_mapping, **uploads_playlists}

    async def _is_subscription_channel(self, channel_id, session, headers):
        if self.quota_reset_time and datetime.now(ZoneInfo("UTC")) < self.quota_reset_time:
            _LOGGER.info(f"API quota exceeded. Skipping channel check for {channel_id} until reset at {self.quota_reset_time}.")
            return False

        # Fetch channel details with brandingSettings
        url = f"https://www.googleapis.com/youtube/v3/channels?part=brandingSettings,topicDetails&id={channel_id}&key={self.api_key}"
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    self.current_quota += 5  # channels.list with part=brandingSettings,topicDetails costs 5 units
                    _LOGGER.debug(f"Quota incremented by 5 units. Current quota usage: {self.current_quota}")
                    await self._save_persistent_data()
                    data = await response.json()
                    items = data.get('items', [])
                    if items:
                        # Check for keywords in channel title
                        branding_settings = items[0].get('brandingSettings', {})
                        channel_title = branding_settings.get('channel', {}).get('title', '').lower()
                        if 'music' in channel_title or 'movies' in channel_title:
                            return True
                        # Check for specific topic categories
                        topic_details = items[0].get('topicDetails', {})
                        topic_categories = topic_details.get('topicCategories', [])
                        for category in topic_categories:
                            if 'music' in category.lower() or 'film' in category.lower():
                                return True
                elif response.status == 403:
                    quota_exceeded = await self._handle_quota_exceeded(response)
                    if quota_exceeded:
                        raise QuotaExceededException(f"API quota exceeded. Further attempts will resume after the quota resets at {self.quota_reset_time}")
                elif response.status == 401:
                    _LOGGER.warning("Access token expired or unauthorized during channel check. Attempting to refresh token.")
                    refresh_result = await self.refresh_oauth_token()
                    if refresh_result:
                        headers["Authorization"] = f"Bearer {self.access_token}"
                        return await self._is_subscription_channel(channel_id, session, headers)
                    else:
                        _LOGGER.error("Failed to refresh access token after 401 error during channel check.")
                else:
                    _LOGGER.warning(f"Failed to fetch details for channel {channel_id}. Status: {response.status}")
        except QuotaExceededException:
            raise
        except Exception as e:
            _LOGGER.error(f"Unexpected error checking subscription channel {channel_id}: {e}")
        
        return False

