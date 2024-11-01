import logging
import os
import io
import json
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from homeassistant.components.sensor import SensorEntity
from homeassistant.helpers.storage import Store
from homeassistant.helpers.update_coordinator import CoordinatorEntity, DataUpdateCoordinator, UpdateFailed
from homeassistant.core import HomeAssistant, callback
from homeassistant.components import persistent_notification
from .const import DOMAIN, CONF_MAX_REGULAR_VIDEOS, CONF_MAX_SHORT_VIDEOS, DEFAULT_MAX_REGULAR_VIDEOS, DEFAULT_MAX_SHORT_VIDEOS
from dateutil import parser
import async_timeout
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from .youtube_api import YouTubeAPI, QuotaExceededException
import numpy as np
from PIL import Image, ImageDraw
import aiofiles
import isodate
from isodate import parse_duration
import re

_LOGGER = logging.getLogger(__name__)

def remove_black_bars(img, threshold=10):
    # Convert the image to grayscale and analyze the non-black areas
    gray = img.convert('L')
    gray_np = np.array(gray)
    
    # Detect content boundaries
    rows = np.any(gray_np > threshold, axis=1)
    cols = np.any(gray_np > threshold, axis=0)
    ymin, ymax = np.where(rows)[0][[0, -1]]
    xmin, xmax = np.where(cols)[0][[0, -1]]
    
    # Crop the image to remove black bars
    cropped = img.crop((xmin, ymin, xmax+1, ymax+1))
    
    return cropped

def replace_white_border_with_black(img, threshold=250):
    """
    Replace any white or near-white strip at the bottom of the image with black.
    """
    # Convert image to RGB if not already
    if img.mode != 'RGB':
        img = img.convert('RGB')
    
    img_np = np.array(img)
    
    # Check the bottom rows of pixels to see if they are white (or near-white)
    bottom_row = img_np[-1, :, :]  # The last row (bottom strip)
    if np.all(bottom_row > threshold):  # If all pixels in the bottom row are near white
        # Replace the bottom strip with black
        draw = ImageDraw.Draw(img)
        width, height = img.size
        # Draw a black rectangle at the bottom
        draw.rectangle([(0, height - 10), (width, height)], fill=(0, 0, 0))  # Adjust 10px as needed
    
    return img

async def process_image_to_portrait(hass: HomeAssistant, url: str, video_id) -> str:
    save_dir = hass.config.path("www", "youtube_thumbnails")
    os.makedirs(save_dir, exist_ok=True)

    # Define file name based on video ID
    if isinstance(video_id, dict):
        file_name = f"{video_id.get('videoId', '')}_portrait.jpg"
    else:
        file_name = f"{video_id}_portrait.jpg"
    file_path = os.path.join(save_dir, file_name)

    # Fetch the image from the URL
    if not url:
        _LOGGER.error(f"No URL provided for video_id: {video_id}")
        return ""

    session = async_get_clientsession(hass)
    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                _LOGGER.error(f"Failed to download image from URL: {url} with status code: {resp.status}")
                return url
            image_data = await resp.read()
    except Exception as e:
        _LOGGER.error(f"Exception occurred while fetching image from URL: {url}, error: {e}")
        return ""

    try:
        # Open the original image from the fetched data
        with Image.open(io.BytesIO(image_data)) as img:
            # No saving of original image
            
            # Remove black bars from the image
            img_no_black_bars = remove_black_bars(img)

            # Crop the image to the target 2:3 ratio (portrait mode)
            width, height = img_no_black_bars.size
            target_ratio = 2 / 3

            if width / height > target_ratio:
                # Crop width if too wide
                new_width = int(height * target_ratio)
                left = (width - new_width) // 2
                right = left + new_width
                img_cropped = img_no_black_bars.crop((left, 0, right, height))
            else:
                # Crop height if too tall
                new_height = int(width / target_ratio)
                top = (height - new_height) // 2
                bottom = top + new_height
                img_cropped = img_no_black_bars.crop((0, top, width, bottom))

            # Further modify the image: Remove 10% from both left and right
            crop_percentage = 0.1
            crop_x = int(img_cropped.width * crop_percentage)
            crop_y = int(crop_x * (img_cropped.height / img_cropped.width))
            img_further_cropped = img_cropped.crop((crop_x, crop_y, img_cropped.width - crop_x, img_cropped.height - crop_y))

            # Resize to the final portrait size (480x720)
            img_resized = img_further_cropped.resize((480, 720), Image.LANCZOS)

            # Replace any white border at the bottom with black
            img_final = replace_white_border_with_black(img_resized)

            # Save the modified image to file
            img_byte_arr = io.BytesIO()
            img_final.save(img_byte_arr, format='JPEG', quality=95)
            img_byte_arr = img_byte_arr.getvalue()

            async with aiofiles.open(file_path, 'wb') as out_file:
                await out_file.write(img_byte_arr)

    except Exception as e:
        _LOGGER.error(f"Error processing image for video_id {video_id}: {e}", exc_info=True)
        return "/local/youtube_thumbnails/default_thumbnail.jpg"

    return f"/local/youtube_thumbnails/{file_name}"

async def process_image_to_fanart(hass: HomeAssistant, video_id, thumbnails: dict) -> str:
    save_dir = hass.config.path("www", "youtube_thumbnails")
    os.makedirs(save_dir, exist_ok=True)

    if isinstance(video_id, dict):
        file_name = f"{video_id.get('videoId', '')}_fanart.jpg"
    else:
        file_name = f"{video_id}_fanart.jpg"
    file_path = os.path.join(save_dir, file_name)

    url = thumbnails.get('maxres', {}).get('url')
    if not url:
        url = thumbnails.get('standard', {}).get('url') or thumbnails.get('high', {}).get('url')

    if not url:
        _LOGGER.error(f"No suitable thumbnail found for video_id: {video_id}")
        return ""

    session = async_get_clientsession(hass)
    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                _LOGGER.error(f"Failed to download image from URL: {url} with status code: {resp.status}")
                return url
            image_data = await resp.read()
    except Exception as e:
        _LOGGER.error(f"Exception occurred while fetching image from URL: {url}, error: {e}")
        return ""

    try:
        with Image.open(io.BytesIO(image_data)) as img:
            img_no_black_bars = remove_black_bars(img)
            width, height = img_no_black_bars.size

            target_ratio = 16 / 9
            if abs(width / height - target_ratio) > 0.01:
                if width / height > target_ratio:
                    new_height = int(width / target_ratio)
                    top = (height - new_height) // 2
                    img_cropped = img_no_black_bars.crop((0, top, width, top + new_height))
                else:
                    new_width = int(height * target_ratio)
                    left = (width - new_width) // 2
                    img_cropped = img_no_black_bars.crop((left, 0, left + new_width, height))
            else:
                img_cropped = img_no_black_bars

            img_resized = img_cropped.resize((1280, 720), Image.LANCZOS)
            img_byte_arr = io.BytesIO()
            img_resized.save(img_byte_arr, format='JPEG', quality=95)
            img_byte_arr = img_byte_arr.getvalue()

            async with aiofiles.open(file_path, 'wb') as out_file:
                await out_file.write(img_byte_arr)

    except Exception as e:
        _LOGGER.error(f"Error processing image for video_id {video_id}: {e}", exc_info=True)
        return "/local/youtube_thumbnails/default_thumbnail.jpg"

    return f"/local/youtube_thumbnails/{file_name}"

async def async_setup_entry(hass: HomeAssistant, entry, async_add_entities):
    """Set up YouTube Recently Added sensor platform."""
    # Use the existing coordinator from hass.data
    coordinator = hass.data[DOMAIN][entry.entry_id + "_coordinator"]

    sensor = YouTubeRecentlyAddedSensor(coordinator)
    shorts_sensor = YouTubeRecentlyAddedShortsSensor(coordinator)

    async_add_entities([sensor, shorts_sensor], True)
    _LOGGER.debug("YouTube Recently Added sensors set up completed.")

class YouTubeDataUpdateCoordinator(DataUpdateCoordinator):
    def __init__(self, hass: HomeAssistant, youtube, entry):
        super().__init__(
            hass,
            _LOGGER,
            name="YouTube Recently Added",
            update_interval=timedelta(hours=6),
        )
        self.youtube = youtube
        self.hass = hass
        self.data = {"data": [], "shorts_data": []}
        self.config_entry = entry
        self.store = Store(hass, 1, f"{DOMAIN}.{entry.entry_id}")
        self.last_webhook_time = None

    async def async_setup(self):
        stored_data = await self.store.async_load()
        _LOGGER.critical(f"Loaded stored_data: {stored_data}")  # Add this line
        if stored_data:
            self.data = {
                "data": stored_data.get("data", []),
                "shorts_data": stored_data.get("shorts_data", [])
            }
            _LOGGER.critical(f"Initialized self.data: {self.data}")  # Add this line
            self.last_webhook_time = stored_data.get("last_webhook_time")
            if self.last_webhook_time:
                self.last_webhook_time = datetime.fromisoformat(self.last_webhook_time)

    async def handle_webhook_update(self, video_id, is_deleted):
        if is_deleted:
            self.data["data"] = [v for v in self.data.get("data", []) if v["id"] != video_id]
            self.data["shorts_data"] = [v for v in self.data.get("shorts_data", []) if v["id"] != video_id]
            await self.store.async_save(self.data)
        else:
            video_data = await self.youtube.get_video_by_id(video_id)
            if video_data:
                duration_seconds = parse_duration(video_data['contentDetails'].get('duration', 'PT0M0S')).total_seconds()
                if video_data.get('live_status'):
                    video_data['runtime'] = ""
                else:
                    runtime = max(1, int(duration_seconds / 60))
                    video_data['runtime'] = str(runtime)

                # Process poster and fanart
                thumbnails = video_data['snippet'].get('thumbnails', {})
                video_data['fanart'] = await process_image_to_fanart(self.hass, video_id, thumbnails)
                poster_url = thumbnails.get('maxres', {}).get('url') or thumbnails.get('high', {}).get('url')
                if poster_url:
                    video_data['poster'] = await process_image_to_portrait(self.hass, poster_url, video_id)
                else:
                    video_data['poster'] = "/local/youtube_thumbnails/default_poster.jpg"
                
                if duration_seconds <= 60 and not video_data.get('live_status'):
                    target_list = 'shorts_data'
                    max_videos = DEFAULT_MAX_SHORT_VIDEOS
                    if self.config_entry:
                        max_videos = self.config_entry.options.get(CONF_MAX_SHORT_VIDEOS, DEFAULT_MAX_SHORT_VIDEOS)
                else:
                    target_list = 'data'
                    max_videos = DEFAULT_MAX_REGULAR_VIDEOS
                    if self.config_entry:
                        max_videos = self.config_entry.options.get(CONF_MAX_REGULAR_VIDEOS, DEFAULT_MAX_REGULAR_VIDEOS)
                
                # Ensure the target list exists
                if target_list not in self.data:
                    self.data[target_list] = []
                
                # Remove existing entry if present
                self.data[target_list] = [v for v in self.data[target_list] if v['id'] != video_data['id']]
                # Insert new video at the beginning
                self.data[target_list].insert(0, video_data)
                # Sort and limit the list
                self.data[target_list] = sorted(self.data[target_list], key=lambda x: x.get('airdate', ''), reverse=True)[:max_videos]
                await self.store.async_save(self.data)

        self.async_set_updated_data(self.data)

    async def async_request_refresh(self, video_id=None, is_deleted=False):
        """Trigger a data refresh, possibly for a specific video."""
        if video_id:
            self.force_update = True
            self.webhook_video_id = video_id
            self.is_deleted = is_deleted
            await self.async_refresh()
        else:
            await super().async_request_refresh()
    
    def is_quota_exceeded(self):
        current_time = datetime.now(ZoneInfo("UTC"))
        _LOGGER.debug(f"Checking if quota is exceeded. Current quota: {self.youtube.current_quota}, Reset time: {self.youtube.quota_reset_time}")
        is_exceeded = self.youtube.current_quota >= 10000
        _LOGGER.debug(f"Quota exceeded: {is_exceeded}")
        return is_exceeded

    def _validate_video_data(self, video):
        if isinstance(video, str):
            _LOGGER.debug(f"Received string instead of video data object: {video}")
            return False
            
        required_fields = ['id', 'snippet', 'contentDetails']
        if not all(field in video for field in required_fields):
            video_id = video.get('id', 'Unknown')
            _LOGGER.debug(f"Video ID {video_id} is missing required fields.")
            return False

        duration_iso = video['contentDetails'].get('duration', 'PT0M0S')
        try:
            duration_timedelta = isodate.parse_duration(duration_iso)
            duration_seconds = int(duration_timedelta.total_seconds())
            _LOGGER.debug(f"Video ID {video['id']} has duration_seconds: {duration_seconds}")
        except (ValueError, TypeError) as e:
            _LOGGER.debug(f"Video ID {video.get('id', 'Unknown')} has invalid duration format: {e}")
            return False

        # Exclude videos with duration <= 0
        if duration_seconds <= 0:
            _LOGGER.debug(f"Video ID {video['id']} excluded due to non-positive duration: {duration_seconds}")
            return False

        if video.get('live_status'):
            _LOGGER.debug(f"Video ID {video['id']} is a live stream.")
            video['runtime'] = ""
            return True
        elif 0 < duration_seconds <= 60 and not video.get('live_status'):
            _LOGGER.debug(f"Video ID {video['id']} classified as Short.")
            return True  # It's a Short; no need for 'statistics'
        elif duration_seconds > 60:
            # It's a regular video
            if 'statistics' not in video:
                video['statistics'] = {'viewCount': '0', 'likeCount': '0', 'commentCount': '0'}
            _LOGGER.debug(f"Video ID {video['id']} classified as Regular.")
            return True
        else:
            _LOGGER.debug(f"Video ID {video['id']} does not meet Short or Regular criteria.")
            return False

    async def _async_update_data(self):
        stored_data = await self.store.async_load()
        if stored_data:
            self.data = {
                "data": stored_data.get("data", []),
                "shorts_data": stored_data.get("shorts_data", [])
            }
        else:
            self.data = {"data": [], "shorts_data": []}

        current_time = datetime.now(ZoneInfo('UTC'))
        is_initial_setup = not self.last_update_success

        if not is_initial_setup and self.last_webhook_time and (current_time - self.last_webhook_time) < timedelta(hours=6):
            _LOGGER.info("Skipping scheduled update due to recent webhook activity")
            await self.store.async_save(self.data)
            return self.data

        try:
            if self.youtube is None:
                _LOGGER.error("YouTube API instance is None in YouTubeDataUpdateCoordinator")
                await self.store.async_save(self.data)
                return {"data": [], "shorts_data": [], "info": "YouTube API instance is not initialized."}

            _LOGGER.debug("Fetching recent videos from YouTube API")
            videos = await self.youtube.get_recent_videos()

            if not videos['data'] and not videos['shorts_data']:
                message = "No new videos were fetched from YouTube API. Keeping existing data."
                _LOGGER.info(message)
                await self.store.async_save(self.data)
                return self.data

            _LOGGER.debug("Processing fetched videos")
            processed_data = await self._process_videos(videos['data'], "data")
            processed_shorts = await self._process_videos(videos['shorts_data'], "shorts_data")

            await self.delete_orphaned_images(processed_data, processed_shorts)

            if processed_data.get('data') or processed_shorts.get('shorts_data'):
                _LOGGER.info(f"Found {len(processed_data.get('data', []))} new regular videos and {len(processed_shorts.get('shorts_data', []))} new shorts")
            else:
                _LOGGER.info("No new videos found in this update")

            existing_videos = {v['id']: v for v in self.data.get('data', [])}
            existing_shorts = {v['id']: v for v in self.data.get('shorts_data', [])}

            for video in processed_data.get('data', []):
                existing_videos[video['id']] = video
            
            for video in processed_shorts.get('shorts_data', []):
                existing_shorts[video['id']] = video

            merged_data = {
                "data": list(existing_videos.values()),
                "shorts_data": list(existing_shorts.values())
            }

            max_regular_videos = self.config_entry.options.get(CONF_MAX_REGULAR_VIDEOS, DEFAULT_MAX_REGULAR_VIDEOS)
            max_short_videos = self.config_entry.options.get(CONF_MAX_SHORT_VIDEOS, DEFAULT_MAX_SHORT_VIDEOS)
            
            merged_data['data'] = sorted(merged_data['data'], key=lambda x: x.get('airdate', ''), reverse=True)[:max_regular_videos]
            merged_data['shorts_data'] = sorted(merged_data['shorts_data'], key=lambda x: x.get('airdate', ''), reverse=True)[:max_short_videos]

            self.data = merged_data
            await self.store.async_save(self.data)
            return self.data

        except QuotaExceededException as qee:
            _LOGGER.warning(f"API quota exceeded: {qee}")
            message = str(qee)
            persistent_notification.async_create(
                self.hass,
                message,
                title="YouTube API Quota Exceeded",
                notification_id="youtube_quota_exceeded"
            )
            await self.store.async_save(self.data)
            return self.data
        except Exception as err:
            _LOGGER.error(f"Error updating data from YouTube API: {err}", exc_info=True)
            await self.store.async_save(self.data)
            return self.data

    async def _process_videos(self, videos, video_type):
        if not videos:
            return {"data": [], "shorts_data": []}

        processed_videos = []
        shorts_videos = []

        for video in videos:
            if self._validate_video_data(video):
                duration_iso = video['contentDetails'].get('duration', 'PT0M0S')
                try:
                    duration_timedelta = isodate.parse_duration(duration_iso)
                    duration_seconds = int(duration_timedelta.total_seconds())
                    runtime = max(1, int(duration_seconds / 60))
                    video['runtime'] = str(runtime)
                except (ValueError, TypeError):
                    _LOGGER.debug(f"Video ID {video.get('id', 'Unknown')} has invalid duration format.")
                    continue

                # Process images
                video_id = video['id']
                thumbnails = video['snippet'].get('thumbnails', {})
                try:
                    video['fanart'] = await process_image_to_fanart(self.hass, video_id, thumbnails)
                except Exception as e:
                    _LOGGER.error(f"Error processing fanart for video {video_id}: {e}")
                    video['fanart'] = "/local/youtube_thumbnails/default_fanart.jpg"

                # Prioritize 'maxres' thumbnail, fallback to 'high'
                poster_url = thumbnails.get('maxres', {}).get('url') or thumbnails.get('high', {}).get('url')
                if poster_url:
                    try:
                        video['poster'] = await process_image_to_portrait(self.hass, poster_url, video_id)
                        _LOGGER.debug(f"Using {'maxres' if thumbnails.get('maxres', {}).get('url') else 'high'} thumbnail for video {video_id}")
                    except Exception as e:
                        _LOGGER.error(f"Error processing poster for video {video_id}: {e}")
                        video['poster'] = "/local/youtube_thumbnails/default_poster.jpg"
                else:
                    _LOGGER.warning(f"No high-resolution thumbnail available for video: {video_id}")
                    video['poster'] = "/local/youtube_thumbnails/default_poster.jpg"

                if 0 <= duration_seconds <= 60:
                    _LOGGER.debug(f"Video ID {video['id']} classified as Short.")
                    shorts_videos.append(video)
                elif duration_seconds > 60:
                    _LOGGER.debug(f"Video ID {video['id']} classified as Regular video.")
                    processed_videos.append(video)

        if video_type == "data":
            return {"data": processed_videos, "shorts_data": []}
        elif video_type == "shorts_data":
            return {"data": [], "shorts_data": shorts_videos}
        else:
            return {"data": processed_videos, "shorts_data": shorts_videos}

    async def delete_orphaned_images(self, processed_data, processed_shorts):
        save_dir = self.hass.config.path("www", "youtube_thumbnails")

        directory_exists = await self.hass.async_add_executor_job(os.path.exists, save_dir)
        if not directory_exists:
            _LOGGER.error(f"Directory does not exist: {save_dir}")
            return

        valid_files = set()

        # Include videos from processed data
        video_list = processed_data.get('data', []) + processed_shorts.get('shorts_data', [])

        # Include videos from persisted data
        persisted_data = await self.store.async_load()
        if persisted_data:
            video_list.extend(persisted_data.get('data', []))
            video_list.extend(persisted_data.get('shorts_data', []))

        for video in video_list:
            if isinstance(video, dict):
                video_id = video.get('id')
            elif isinstance(video, str):
                video_id = video
            else:
                _LOGGER.warning(f"Unexpected video format: {video}")
                continue

            if video_id:
                valid_files.add(f"{video_id}_portrait.jpg")
                valid_files.add(f"{video_id}_fanart.jpg")

        # Add all previously fetched video IDs
        for video_id in self.youtube.fetched_video_ids:
            valid_files.add(f"{video_id}_portrait.jpg")
            valid_files.add(f"{video_id}_fanart.jpg")

        _LOGGER.debug(f"Valid files to keep: {valid_files}")

        try:
            all_files = await aiofiles.os.listdir(save_dir)
            _LOGGER.debug(f"All files in '{save_dir}': {all_files}")
        except Exception as e:
            _LOGGER.error(f"Error listing files in {save_dir}: {e}")
            return

        to_delete = [file_name for file_name in all_files if file_name not in valid_files and not file_name.startswith('default_')]

        _LOGGER.debug(f"Orphaned files to delete: {to_delete}")

        if not to_delete:
            _LOGGER.info("No orphaned files to delete.")
            return

        for file_name in to_delete:
            file_path = os.path.join(save_dir, file_name)
            _LOGGER.debug(f"Attempting to delete orphaned file: {file_path}")
            try:
                await aiofiles.os.remove(file_path)
                _LOGGER.info(f"Deleted orphaned file: {file_path}")
            except Exception as e:
                _LOGGER.error(f"Failed to delete orphaned file {file_path}: {e}")

class YouTubeRecentlyAddedSensor(CoordinatorEntity, SensorEntity):
    def __init__(self, coordinator):
        super().__init__(coordinator)
        self._attr_unique_id = f"youtube_recently_added"
        self._attr_name = f"YouTube Recently Added"
        self._attr_icon = "mdi:youtube"
        _LOGGER.debug(f"Initializing YouTubeRecentlyAddedSensor with coordinator data: {coordinator.data}")

    async def async_added_to_hass(self):
        """Handle when entity is added to hass."""
        await super().async_added_to_hass()
        if self.coordinator.data:
            if isinstance(self.coordinator.data, dict) and "data" in self.coordinator.data:
                video_data = self.coordinator.data["data"]
                updated_video_data = []
                for video in video_data:
                    if isinstance(video, str):
                        video_details = await self.coordinator.youtube.get_video_by_id(video)
                        if video_details:
                            updated_video_data.append(video_details)
                    else:
                        updated_video_data.append(video)
                self.coordinator.data["data"] = updated_video_data
            _LOGGER.debug("Sensor added to hass with existing coordinator data.")
            self.async_write_ha_state()

    @property
    def state(self):
        if isinstance(self.coordinator.data, dict) and "data" in self.coordinator.data:
            video_data = self.coordinator.data["data"]
            _LOGGER.debug(f"Processing {len(video_data)} videos for state")
            
            processed_videos = []
            for video in video_data:
                if isinstance(video, dict) and "snippet" in video and "publishedAt" in video["snippet"]:
                    try:
                        processed_video = self._process_video_data(video)
                        if processed_video:
                            processed_videos.append(processed_video)
                    except Exception as e:
                        _LOGGER.error(f"Error processing video data: {e}", exc_info=True)

            sorted_videos = sorted(processed_videos, key=lambda x: x.get('airdate', ''), reverse=True)
            max_videos = self.coordinator.config_entry.options.get(CONF_MAX_REGULAR_VIDEOS, DEFAULT_MAX_REGULAR_VIDEOS)
            sorted_videos = sorted_videos[:max_videos]

            base_attributes = {
                "data": [
                    {
                        "title_default": "$title",
                        "line1_default": "$channel",
                        "line2_default": "$release",
                        "line3_default": "$runtime - $live_status - $genres",
                        "line4_default": "$views views - $likes likes",
                        "icon": "mdi:youtube"
                    }
                ],
                "friendly_name": "YouTube Recently Added",
                "current_quota": getattr(self.coordinator.youtube, 'current_quota', None)
            }

            self._prepared_items = []
            current_data = base_attributes["data"].copy()

            for item in sorted_videos:
                test_item = item.copy()
                if 'summary' in test_item:
                    paragraphs = test_item['summary'].split('\n\n')
                    test_item['summary'] = '\n\n'.join(paragraphs[:2]).strip()

                test_attributes = base_attributes.copy()
                test_attributes["data"] = current_data + [test_item]
                if len(json.dumps(test_attributes).encode('utf-8')) > 16000:
                    break

                current_data.append(test_item)
                self._prepared_items.append(test_item)

            _LOGGER.debug(f"YouTubeRecentlyAddedSensor state (limited): {len(self._prepared_items)}")
            return len(self._prepared_items)
        _LOGGER.debug("YouTubeRecentlyAddedSensor state: 0 (no data available)")
        return 0

    @property
    def extra_state_attributes(self):
        attributes = {
            "data": [
                {
                    "title_default": "$title",
                    "line1_default": "$channel",
                    "line2_default": "$release",
                    "line3_default": "$runtime - $live_status - $genres",
                    "line4_default": "$views views - $likes likes",
                    "icon": "mdi:youtube"
                }
            ]
        }

        if hasattr(self, '_prepared_items'):
            attributes["data"].extend(self._prepared_items)
        else:
            if isinstance(self.coordinator.data, dict) and "data" in self.coordinator.data:
                video_data = self.coordinator.data["data"]
                video_data_list = []
                for video in video_data:
                    if isinstance(video, dict) and "snippet" in video and "publishedAt" in video["snippet"]:
                        try:
                            processed_video = self._process_video_data(video)
                            if processed_video:
                                if 'live_status' not in processed_video:
                                    processed_video['live_status'] = video.get('live_status', '')
                                video_data_list.append(processed_video)
                        except Exception as e:
                            _LOGGER.error(f"Error processing video data: {e}", exc_info=True)
                            continue

                sorted_video_data = sorted(video_data_list, key=lambda x: x.get('airdate', ''), reverse=True)
                
                current_data = attributes["data"]
                
                for item in sorted_video_data[:self.state]:
                    test_item = item.copy()
                    if 'summary' in test_item:
                        paragraphs = test_item['summary'].split('\n\n')
                        test_item['summary'] = '\n\n'.join(paragraphs[:2]).strip()

                    test_attributes = attributes.copy()
                    test_attributes["data"] = current_data + [test_item]
                    if len(json.dumps(test_attributes).encode('utf-8')) > 16000:
                        _LOGGER.warning(f"YouTube Recently Added sensor items reduced to {len(current_data) - 1} due to 16KB attribute size limit.")
                        break

                    current_data.append(test_item)
                
                attributes["data"] = current_data

        attributes["friendly_name"] = "YouTube Recently Added"
        if self.coordinator.last_update_success:
            attributes["current_quota"] = getattr(self.coordinator.youtube, 'current_quota', None)

        return attributes

    def _process_video_data(self, video):
        video_id = video.get('id')
        published_at = video["snippet"].get("publishedAt")
        if not published_at:
            _LOGGER.warning(f"Video {video_id} is missing 'publishedAt' field.")
            return None

        try:
            published_date = parser.isoparse(published_at).replace(tzinfo=ZoneInfo("UTC"))
            local_tz = ZoneInfo(self.hass.config.time_zone)
            local_published_date = published_date.astimezone(local_tz)
            airdate = local_published_date.isoformat()
            aired = local_published_date.strftime("%Y-%m-%d")
            release = local_published_date.strftime("%A, %B %d, %Y %I:%M %p")
            
            # Check if this is a live stream video and its status
            live_status = video.get('live_status', '')
            is_live = bool(live_status)
            stream_ended = live_status == "📴 Stream ended"
            
            # Pass live status and stream ended status to _convert_duration
            runtime = self._convert_duration(video['contentDetails'].get('duration', 'PT0M0S'), is_live, stream_ended)

            statistics = video.get('statistics', {})
            view_count = statistics.get('viewCount')
            like_count = statistics.get('likeCount')

            views = f"{int(view_count):,}" if view_count is not None else "0"
            likes = f"{int(like_count):,}" if like_count is not None else "0"

            processed_data = {
                "id": video_id,
                "airdate": airdate,
                "aired": aired,
                "release": release,
                "title": video["snippet"].get("title", ""),
                "channel": video["snippet"].get("channelTitle", ""),
                "runtime": runtime,
                "genres": video.get("genres", ""),
                "views": views,
                "likes": likes,
                "summary": video["snippet"].get("description", ""),
                "poster": video.get("poster", ""),
                "fanart": video.get("fanart", ""),
                "deep_link": f"https://www.youtube.com/watch?v={video_id}"
            }
            
            # Add live_status if it exists
            if live_status:
                processed_data["live_status"] = live_status
                
            return processed_data
        except Exception as e:
            _LOGGER.error(f"Error processing video data for ID {video_id}: {e}", exc_info=True)
            return None

    def _convert_duration(self, duration, is_live=False, stream_ended=False):
        if is_live and not stream_ended:
            return ""
        try:
            duration_timedelta = isodate.parse_duration(duration)
            minutes = int(duration_timedelta.total_seconds() // 60)
            return str(max(1, minutes))
        except (isodate.ISO8601Error, ValueError, TypeError) as e:
            _LOGGER.error(f"Error converting duration: {e}")
            return ""

class YouTubeRecentlyAddedShortsSensor(YouTubeRecentlyAddedSensor):
    """Representation of a YouTube Recently Added Shorts Sensor."""

    def __init__(self, coordinator):
        super().__init__(coordinator)
        self._attr_unique_id = "youtube_recently_added_shorts"
        self._attr_name = "YouTube Recently Added Shorts"
        self._attr_icon = "mdi:youtube"
        _LOGGER.debug("Initializing YouTube Recently Added Shorts sensor")

    @property
    def state(self):
        _LOGGER.debug("Calculating state for YouTubeRecentlyAddedShortsSensor")
        if isinstance(self.coordinator.data, dict) and "shorts_data" in self.coordinator.data:
            shorts_data = self.coordinator.data["shorts_data"]
            _LOGGER.debug(f"Processing {len(shorts_data)} shorts for state")
            
            processed_shorts = []
            for video in shorts_data:
                if isinstance(video, dict) and "snippet" in video and "publishedAt" in video["snippet"]:
                    try:
                        processed_video = self._process_video_data(video)
                        if processed_video:
                            processed_shorts.append(processed_video)
                    except Exception as e:
                        _LOGGER.error(f"Error processing shorts data: {e}", exc_info=True)

            sorted_shorts = sorted(processed_shorts, key=lambda x: x.get('airdate', ''), reverse=True)
            max_videos = self.coordinator.config_entry.options.get(CONF_MAX_SHORT_VIDEOS, DEFAULT_MAX_SHORT_VIDEOS)
            sorted_shorts = sorted_shorts[:max_videos]

            base_attributes = {
                "data": [
                    {
                        "title_default": "$title",
                        "line1_default": "$channel",
                        "line2_default": "$release",
                        "line3_default": "$runtime - $live_status - $genres",
                        "line4_default": "$views views - $likes likes",
                        "icon": "mdi:youtube"
                    }
                ],
                "friendly_name": "YouTube Recently Added Shorts",
                "current_quota": getattr(self.coordinator.youtube, 'current_quota', None)
            }

            self._prepared_items = []
            current_data = base_attributes["data"].copy()

            for item in sorted_shorts:
                test_item = item.copy()
                if 'summary' in test_item:
                    paragraphs = test_item['summary'].split('\n\n')
                    test_item['summary'] = '\n\n'.join(paragraphs[:2]).strip()

                test_attributes = base_attributes.copy()
                test_attributes["data"] = current_data + [test_item]
                if len(json.dumps(test_attributes).encode('utf-8')) > 16000:
                    break

                current_data.append(test_item)
                self._prepared_items.append(test_item)

            _LOGGER.debug(f"YouTubeRecentlyAddedShortsSensor state (limited): {len(self._prepared_items)}")
            return len(self._prepared_items)

        _LOGGER.warning("No shorts data available. Returning 0.")
        return 0

    @property
    def extra_state_attributes(self):
        attributes = {
            "data": [
                {
                    "title_default": "$title",
                    "line1_default": "$channel",
                    "line2_default": "$release",
                    "line3_default": "$runtime - $live_status - $genres",
                    "line4_default": "$views views - $likes likes",
                    "icon": "mdi:youtube"
                }
            ]
        }

        if hasattr(self, '_prepared_items'):
            attributes["data"].extend(self._prepared_items)
        else:
            if isinstance(self.coordinator.data, dict) and "shorts_data" in self.coordinator.data:
                shorts_data = self.coordinator.data["shorts_data"]
                shorts_data_list = []
                for video in shorts_data:
                    if isinstance(video, dict) and "snippet" in video and "publishedAt" in video["snippet"]:
                        try:
                            processed_video = self._process_video_data(video)
                            if processed_video:
                                shorts_data_list.append(processed_video)
                        except Exception as e:
                            _LOGGER.error(f"Error processing shorts data: {e}", exc_info=True)
                            continue

                sorted_shorts_data = sorted(shorts_data_list, key=lambda x: x.get('airdate', ''), reverse=True)
                
                current_data = attributes["data"]
                
                for item in sorted_shorts_data[:self.state]:
                    test_item = item.copy()
                    if 'summary' in test_item:
                        paragraphs = test_item['summary'].split('\n\n')
                        test_item['summary'] = '\n\n'.join(paragraphs[:2]).strip()

                    test_attributes = attributes.copy()
                    test_attributes["data"] = current_data + [test_item]
                    if len(json.dumps(test_attributes).encode('utf-8')) > 16000:
                        _LOGGER.warning(f"YouTube Recently Added Shorts sensor items reduced to {len(current_data) - 1} due to 16KB attribute size limit.")
                        break

                    current_data.append(test_item)
                
                attributes["data"] = current_data

        attributes["friendly_name"] = "YouTube Recently Added Shorts"
        if self.coordinator.last_update_success:
            attributes["current_quota"] = getattr(self.coordinator.youtube, 'current_quota', None)

        return attributes


