import functools
import traceback

from datetime import datetime
from typing import Any

from pymongo import MongoClient
from pyrogram.types import Message, User
from pymongo.errors import ServerSelectionTimeoutError, DuplicateKeyError

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
		kwargs["connectTimeoutMS"] = int(alemiBot.config.get("database", "timeout", fallback=3000))
		self.log_messages = alemiBot.config.get("database", "log_messages", fallback=True)
		self.log_service = alemiBot.config.get("database", "log_service", fallback=True)
		self.log_media = alemiBot.config.get("database", "log_media", fallback=False)

		self.counter = Counter(["service", "messages", "deletions", "edits", "users", "chats"])

		self.client = MongoClient(host, port, **kwargs)
		self.db = self.client[alemiBot.config.get("database", "dbname", fallback="alemibot")]

	def log_error_event(self, func):
		@functools.wraps(func)
		async def wrapper(client, message):
			try:
				await func(client, message)
			except ServerSelectionTimeoutError as ex:
				logger.error("Could not connect to MongoDB")
				logger.info(str(message))
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

	def insert_doc_duplicable(self, doc:dict, coll:str = "messages", ignore:bool = False):
		"""Will attempt to insert a document which may be duplicate

		If there is already a document with same index, increase that document `dup` \
		field and insert this with `dup` null.
		This is kind of an expensive operation: it tries to just do 1 insertion, but if \
		that is rejected because of a duplicate key, it will make a search, an update and \
		another insertion after that.
		This works only when duplicates are very few compared to insertions!
		"""
		try:
			self.db[coll].insert_one(doc)
		except DuplicateKeyError: # if there's already a message with this id and chat, add a dup field to previous one
			if ignore:
				return
			duplicates_count = 1
			for dup in self.db[coll].find({"id":doc["id"],"chat":doc["chat"]}):
				if "dup" in dup:
					duplicates_count = max(duplicates_count, dup["dup"])
			self.db[coll].update_one({"id":doc["id"],"chat":doc["chat"],"dup":None},
										{"$set": {"dup": duplicates_count+1}})
			self.db[coll].insert_one(doc) # most recent msg will always have `dup` null

	def log_raw_event(self, event:Any):
		self.db.raw.insert_one(convert_to_dict(event))

	def parse_message_event(self, message:Message, file_name=None, ignore_duplicates=False):
		msg = extract_message(message)
		if file_name:
			msg["file"] = file_name
		self.insert_doc_duplicable(msg, ignore=ignore_duplicates)
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

	def parse_service_event(self, message:Message, ignore_duplicates=False):
		msg = extract_service_message(message)
		self.insert_doc_duplicable(msg, coll="service", ignore=ignore_duplicates)
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

	def parse_edit_event(self, message:Message): # TODO replace `text` so that we always query most recent edit
		self.counter.edits()
		doc = extract_edit_message(message)
		self.db.messages.update_one(
			{"id": message.message_id, "chat": message.chat.id, "dup": None},
			{"$push": {"edits":	doc} }
		)

	def parse_deletion_event(self, message:Message):
		deletions = extract_delete(message)
		for deletion in deletions:
			self.db.deletions.insert_one(deletion)
			self.counter.deletions()

			flt = {"id": deletion["id"], "dup": None}
			if "chat" in deletion:
				flt["chat"] = deletion["chat"]
			self.db.messages.update_one(flt, {"$set": {"deleted": deletion["date"]}})

	def parse_status_update_event(self, user:User):
		if user.status == "offline": # just update last online date
			self.db.users.update_one({"id": user.id}, # there are a ton of these, can't diff user every time I get one
				{"$set": {"last_online_date": datetime.utcfromtimestamp(user.last_online_date)} })

DRIVER = DatabaseDriver()
