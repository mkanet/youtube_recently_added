import logging
from homeassistant import config_entries
from homeassistant.core import callback
import voluptuous as vol
import aiohttp
from .const import DOMAIN, CONF_API_KEY, CONF_CLIENT_ID, CONF_CLIENT_SECRET, CONF_REFRESH_TOKEN, CONF_CHANNEL_IDS, CONF_MAX_REGULAR_VIDEOS, CONF_MAX_SHORT_VIDEOS, DEFAULT_MAX_REGULAR_VIDEOS, DEFAULT_MAX_SHORT_VIDEOS
from .youtube_api import YouTubeAPI

_LOGGER = logging.getLogger(__name__)

class YouTubeRecentlyAddedConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 1
    CONNECTION_CLASS = config_entries.CONN_CLASS_CLOUD_POLL

    def __init__(self):
        self.youtube_api = None
        self.auth_url = None

    async def async_step_user(self, user_input=None):
        errors = {}
        if user_input is not None:
            try:
                self.youtube_api = YouTubeAPI(
                    user_input[CONF_API_KEY],
                    user_input[CONF_CLIENT_ID],
                    user_input[CONF_CLIENT_SECRET],
                    None,  # refresh_token
                    None,  # channel_ids
                    self.hass
                )
                self.auth_url = await self.youtube_api.get_authorization_url()

                if not self.auth_url:
                    _LOGGER.error("Authorization URL is None. There was a problem generating it.")
                    errors["base"] = "auth_url_none"

                return await self.async_step_auth()
            except aiohttp.ClientError as err:
                _LOGGER.error("Network error during initial setup: %s", str(err))
                errors["base"] = "cannot_connect"
            except ValueError as e:
                _LOGGER.error("Invalid input error during initial setup: %s", str(e))
                errors["base"] = "invalid_auth"
            except Exception as e:
                _LOGGER.error("Unexpected error during initial setup: %s", str(e), exc_info=True)
                errors["base"] = "auth_error"

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema({
                vol.Required(CONF_API_KEY): str,
                vol.Required(CONF_CLIENT_ID): str,
                vol.Required(CONF_CLIENT_SECRET): str,
            }),
            errors=errors,
        )

    async def async_step_auth(self, user_input=None):
        if user_input is None:
            return self.async_show_form(
                step_id="auth",
                data_schema=vol.Schema({
                    vol.Required("code"): str,
                    vol.Required(CONF_MAX_REGULAR_VIDEOS, default=DEFAULT_MAX_REGULAR_VIDEOS): int,
                    vol.Required(CONF_MAX_SHORT_VIDEOS, default=DEFAULT_MAX_SHORT_VIDEOS): int,
                }),
                description_placeholders={"auth_url": self.auth_url},
            )

        try:
            refresh_token = await self.youtube_api.perform_oauth2_flow(user_input["code"])
            
            # Fetch the user's channels using the YouTube API
            channel_list = await self.youtube_api.fetch_user_channels(refresh_token)
            if not channel_list:
                raise ValueError("No channels fetched. Please check your YouTube subscriptions.")
            self.channel_ids = channel_list  # Store the fetched channel IDs as a list
            self.youtube_api.channel_ids = channel_list  # Also store in the API instance

            entry = self.async_create_entry(
                title="YouTube Recently Added",
                data={
                    CONF_API_KEY: self.youtube_api.api_key,
                    CONF_CLIENT_ID: self.youtube_api.client_id,
                    CONF_CLIENT_SECRET: self.youtube_api.client_secret,
                    CONF_REFRESH_TOKEN: refresh_token,
                    CONF_CHANNEL_IDS: self.channel_ids,
                    CONF_MAX_REGULAR_VIDEOS: user_input[CONF_MAX_REGULAR_VIDEOS],
                    CONF_MAX_SHORT_VIDEOS: user_input[CONF_MAX_SHORT_VIDEOS],
                },
            )
            self.youtube_api.config_entry = entry
            return entry
        except Exception as e:
            _LOGGER.error("Error during authentication in async_step_auth: %s", str(e))
            return self.async_show_form(
                step_id="auth",
                data_schema=vol.Schema({
                    vol.Required("code"): str,
                    vol.Required(CONF_MAX_REGULAR_VIDEOS, default=DEFAULT_MAX_REGULAR_VIDEOS): int,
                    vol.Required(CONF_MAX_SHORT_VIDEOS, default=DEFAULT_MAX_SHORT_VIDEOS): int,
                }),
                errors={"base": "auth_error"},
                description_placeholders={"auth_url": self.auth_url},
            )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry):
        return YouTubeRecentlyAddedOptionsFlowHandler(config_entry)

class YouTubeRecentlyAddedOptionsFlowHandler(config_entries.OptionsFlow):
    def __init__(self, config_entry):
        self.config_entry = config_entry
        self.youtube_api = None
        self.auth_url = None
        self.options = dict(config_entry.data)

    async def async_step_init(self, user_input=None):
        errors = {}
        if user_input is not None:
            self.options.update(user_input)
            return self.async_create_entry(title="", data=self.options)

        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema({
                vol.Required(CONF_MAX_REGULAR_VIDEOS, 
                             default=self.config_entry.options.get(CONF_MAX_REGULAR_VIDEOS, DEFAULT_MAX_REGULAR_VIDEOS)): int,
                vol.Required(CONF_MAX_SHORT_VIDEOS, 
                             default=self.config_entry.options.get(CONF_MAX_SHORT_VIDEOS, DEFAULT_MAX_SHORT_VIDEOS)): int,
            }),
            errors=errors,
        )

    async def async_step_auth(self, user_input=None):
        if user_input is None:
            return self.async_show_form(
                step_id="auth",
                data_schema=vol.Schema({
                    vol.Required("code"): str,
                }),
                description_placeholders={
                    "auth_url": self.auth_url,
                },
            )

        try:
            token = await self.youtube_api.perform_oauth2_flow(user_input["code"])
            self.options[CONF_REFRESH_TOKEN] = token

            # Fetch the user's channels using the YouTube API
            channel_list = await self.youtube_api.fetch_user_channels(token)
            if not channel_list:
                raise ValueError("No channels fetched. Please check your YouTube subscriptions.")
            
            self.options[CONF_CHANNEL_IDS] = channel_list  # Store as list
            
            # Update the entry with the new token and channels
            self.hass.config_entries.async_update_entry(
                self.config_entry,
                data=self.options,
            )
            
            return self.async_create_entry(title="", data=self.options)
        except Exception as error:
            _LOGGER.error("OAuth2 error: %s", str(error))
            return self.async_show_form(
                step_id="auth",
                data_schema=vol.Schema({
                    vol.Required("code"): str,
                }),
                errors={"base": "auth_error"},
                description_placeholders={
                    "auth_url": self.auth_url,
                },
            )
