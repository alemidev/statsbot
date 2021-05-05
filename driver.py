import functools
import traceback

from datetime import datetime
from typing import Any

from pymongo import MongoClient
from pyrogram.types import Message, User

from bot import alemiBot
from util.serialization import convert_to_dict

from plugins.statsbot.util.serializer import (
	diff, extract_chat, extract_message, extract_user, extract_delete, 
	extract_service_message, extract_edit_message
)

import logging

logger = logging.getLogger(__name__)

class Counter:
	"""Auto-Increasing Counter. Has a dict of keys which start at 0. Every time an attr is accessed, a new function
	   increasing value of key with same name is returned (inserted equal to 0 if missing). To get values,
	   use hash [] access (__getitem__). Initialize this with a list of strings (not really necessary)
	   Increase (and add new value if missing) just with
	   		counter.something() """
	def __init__(self, keys:list):
		self.storage = { k : 0 for k in keys }

	def __getattr__(self, name:str) -> int:
		if name not in self.storage:
			self.storage[name] = 0
		def incr():
			self.storage[name] += 1
			return self.storage[name]
		return incr

	def __str__(self) -> str:
		return str(self.storage)

	def __contains__(self, name:str) -> bool:
		return name in self.storage

	def __getitem__(self, name:str) -> Any:
		if name not in self.storage:
			return 0
		return self.storage[name]
	
	def __setitem__(self, name:str, value:Any):
		self.storage[name] = value

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
		self.log_service = alemiBot.config.get("database", "log_service", fallback=True)
		self.log_media = alemiBot.config.get("database", "log_media", fallback=False)

		self.counter = Counter(["service", "messages", "deletions", "edits", "users", "chats"])

		self.client = MongoClient(host, port, **kwargs)
		self.db = self.client[alemiBot.config.get("database", "dbname", fallback="alemibot")]

	def log_error_event(self, fun):
		@functools.wraps(fun)
		async def wrapper(client, message):
			try:
				await fun(client, message)
			except Exception as ex:
				logger.exception("Serialization error")
				exc_data = {
					"type" : repr(ex),
					"text" : str(ex),
					"traceback" : traceback.format_exc(),
				}
				doc = convert_to_dict(message)
				doc["exception"] = exc_data
				self.db.exceptions.insert_one(doc)
		return wrapper

	def log_raw_event(self, event:Any):
		self.db.raw.insert_one(convert_to_dict(event))

	def parse_message_event(self, message:Message, file_name=None):
		msg = extract_message(message)
		if file_name:
			msg["file"] = file_name
		self.db.messages.insert_one(msg)
		self.counter.messages()

		if message.from_user:
			usr = extract_user(message)
			usr_id = usr["id"]
			prev = self.db.users.find_one({"id": usr_id})
			if prev:
				usr = diff(prev, usr)
			else:
				self.counter.users()
			if usr: # don't insert if no diff!
				self.db.users.update_one({"id": usr_id}, {"$set": usr}, upsert=True)

		if message.sender_chat:
			chat = extract_chat(message)
			chat_id = chat["id"]
			prev = self.db.chats.find_one({"id": chat_id})
			if prev:
				chat = diff(prev, chat)
			else:
				self.counter.chats()
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
				self.counter.chats()
			if chat: # don't insert if no diff!
				self.db.chats.update_one({"id": chat_id}, {"$set": chat}, upsert=True)

	def parse_edit_event(self, message:Message):
		self.counter.edits()
		doc = extract_edit_message(message)
		self.db.messages.update_one(
			{"id": message.message_id, "chat": message.chat.id},
			{"$push": {"edits":	doc} }
		)

	def parse_deletion_event(self, message:Message):
		deletions = extract_delete(message)
		for deletion in deletions:
			self.db.deletions.insert_one(deletion)
			self.counter.deletions()

			flt = {"id": deletion["id"]}
			if "chat" in deletion:
				flt["chat"] = deletion["chat"]
			self.db.messages.update_one(flt, {"$set": {"deleted": deletion["date"]}})

	def parse_status_update_event(self, user:User):
		if user.status == "offline": # just update last online date
			self.db.users.update_one({"id": user.id}, # there are a ton of these, can't diff user every time I get one
				{"$set": {"last_online_date": datetime.utcfromtimestamp(user.last_online_date)} })

DRIVER = DatabaseDriver()
