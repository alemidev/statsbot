import logging

from pyrogram import filters

from bot import alemiBot

from .driver import DRIVER

logger = logging.getLogger(__name__)

@alemiBot.on_message(~filters.edited, group=999999) # happen last and always!
async def log_message_hook(client, message): # TODO handle edits!
	if hasattr(message, "service"):
		DRIVER.parse_system_event(message)
	else:
		fname = None
		if DRIVER.log_media:
			try:
				fname = await client.download_media(message, file_name="data/scraped_media/")
			except:
				logger.exception("Error while downloading media")
		if DRIVER.log_messages:
			DRIVER.parse_message_event(message, file_name=fname)

@alemiBot.on_message(filters.edited, group=999999) # happen last and always!
async def log_edit_hook(client, message): # TODO handle edits!
	if DRIVER.log_messages:
		DRIVER.parse_edit_event(message)

@alemiBot.on_deleted_messages(group=999999)
async def log_deleted_hook(client, message):
	if DRIVER.log_messages:
		DRIVER.parse_deletion_event(message)