import functools

from datetime import datetime
from pymongo import MongoClient
from pyrogram.types import Message, ReplyKeyboardRemove, ReplyKeyboardMarkup, InlineKeyboardMarkup

from bot import alemiBot
from util.serialization import convert_to_dict

from .util.serializer import diff, extract_chat, extract_message, extract_user, extract_delete, extract_service_message

import logging

logger = logging.getLogger(__name__)

class DatabaseDriver:
	def __init__(self):
		kwargs = {}
		host = alemiBot.config.get("database", "host", fallback="localhost")
		port = int(alemiBot.config.get("database", "port", fallback=27017))
		username = alemiBot.config.get("database", "username", fallback=None)
		if username:
			kwargs["username"] = username
		password = alemiBot.config.get("database", "password", fallback=None)
		if password:
			kwargs["password"] = password
		self.log_messages = alemiBot.config.get("database", "log_messages", fallback=True)
		self.log_media = alemiBot.config.get("database", "log_media", fallback=False)

		self.messages = 0
		self.deletions = 0
		self.edits = 0
		self.users = 0
		self.chats = 0

		self.client = MongoClient(host, port, **kwargs)
		self.db = self.client[alemiBot.config.get("database", "dbname", fallback="alemibot")]

	def log_error_event(self, fun):
		@functools.wraps(fun)
		async def wrapper(client, message):
			try:
				await fun(client, message)
			except Exception as e:
				logger.exception("Serialization error")
				message.exception = e
				self.db.exceptions.insert_one(convert_to_dict(message))
		return wrapper

	def parse_message_event(self, message:Message, file_name=None):
		msg = extract_message(message)
		if file_name:
			msg["file"] = file_name
		self.db.messages.insert_one(msg)
		self.messages += 1

		if message.from_user:
			usr = extract_user(message)
			usr_id = usr["id"]
			prev = self.db.users.find_one({"id": usr_id})
			if prev:
				usr = diff(prev, usr)
			else:
				self.users += 1
			if usr: # don't insert if no diff!
				self.db.users.update_one({"id": usr_id}, {"$set": usr}, upsert=True)
		
		if message.sender_chat:
			chat = extract_chat(message)
			chat_id = chat["id"]
			prev = self.db.chats.find_one({"id": chat_id})
			if prev:
				chat = diff(prev, chat)
			else:
				self.chats += 1
			if chat: # don't insert if no diff!
				self.db.chats.update_one({"id": chat_id}, {"$set": chat}, upsert=True)

	def parse_service_event(self, message:Message):
		self.db.service.insert_one(extract_service_message(message))
		if message.chat:
			chat = extract_chat(message)
			chat_id = chat["id"]
			prev = self.db.chats.find_one({"id": chat_id})
			if prev:
				chat = diff(prev, chat)
			else:
				self.chats += 1
			if chat: # don't insert if no diff!
				self.db.chats.update_one({"id": chat_id}, {"$set": chat}, upsert=True)

	def parse_edit_event(self, message:Message):
		self.edits += 1
		doc = { "date": datetime.utcfromtimestamp(message.edit_date), "text": message.text}
		if message.reply_markup:
			if isinstance(message.reply_markup, ReplyKeyboardMarkup):
				doc["keyboard"] = message.reply_markup.keyboard
			elif isinstance(message.reply_markup, InlineKeyboardMarkup):
				doc["inline"] = message.reply_markup.inline_keyboard
			elif isinstance(message.reply_markup, ReplyKeyboardRemove):
				doc["keyboard"] = []
		self.db.messages.update_one(
			{"id": message.message_id, "chat": message.chat.id},
			{"$push": {"edits":	doc} }
		)

	def parse_deletion_event(self, message:Message):
		deletions = extract_delete(message)
		for deletion in deletions:
			self.db.deletions.insert_one(deletion)
			self.deletions += 1

			flt = {"id": deletion["id"]}
			if "chat" in deletion:
				flt["chat"] = deletion["chat"]
			self.db.messages.update_one(flt, {"$set":
					{"deleted": datetime.utcfromtimestamp(deletion["date"])}})

DRIVER = DatabaseDriver()