"""This file contains all pyrogram event hooks. Events are parsed and inserted by the database driver"""
import logging

from pyrogram import filters

from bot import alemiBot

from plugins.statsbot.driver import DRIVER

logger = logging.getLogger(__name__)

@alemiBot.on_message(~filters.edited & ~filters.service, group=999999) # happen last and always!
@DRIVER.log_error_event
async def log_message_hook(client, message):
	"""Log all new non-service messages"""
	fname = None
	if DRIVER.log_media:
		fname = await client.download_media(message, file_name="data/scraped_media/")
	if DRIVER.log_messages:
		DRIVER.parse_message_event(message, file_name=fname)

@alemiBot.on_message(filters.edited, group=999999)
@DRIVER.log_error_event
async def log_edit_hook(_, message):
	"""Log all message edits"""
	if DRIVER.log_messages:
		DRIVER.parse_edit_event(message)

@alemiBot.on_deleted_messages(group=999999)
@DRIVER.log_error_event
async def log_deleted_hook(_, deletions):
	"""Log all message deletions"""
	if DRIVER.log_messages:
		DRIVER.parse_deletion_event(deletions)

@alemiBot.on_message(~filters.edited & filters.service, group=999999)
@DRIVER.log_error_event
async def log_service_message_hook(_, message):
	"""Log all service messages"""
	if DRIVER.log_service:
		DRIVER.parse_service_event(message)

@alemiBot.on_chat_member_updated(group=999999)
@DRIVER.log_error_event
async def log_chat_member_updates(_, update):
	"""Log chat member updates"""
	if DRIVER.log_service:
		DRIVER.log_raw_event(update) # TODO parse these

@alemiBot.on_user_status(group=999999)
@DRIVER.log_error_event
async def log_user_status(_, user):
	"""Log user status updates"""
	if DRIVER.log_service:
		DRIVER.parse_status_update_event(user)
