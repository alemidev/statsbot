import functools
import traceback

from datetime import datetime
from typing import Any

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ServerSelectionTimeoutError, DuplicateKeyError
from pymongo import ASCENDING, DESCENDING, MongoClient

from pyrogram.types import Message, User, ChatMemberUpdated
from pyrogram.errors import PeerIdInvalid

from bot import alemiBot
from util.serialization import convert_to_dict

from plugins.statsbot.util.serializer import (
	diff, extract_chat, extract_member_update, extract_message, extract_user, extract_delete, 
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

def has_index(indexes, index):
	for name in indexes:
		if indexes[name]["key"] == index:
			return True
	return False

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

		self.client = AsyncIOMotorClient(host, port, **kwargs)
		dbname = alemiBot.config.get("database", "dbname", fallback="alemibot")
		self.db = self.client[dbname]
		# create a (sync) MongoClient too for blocking operations
		self.sync_client = MongoClient(host, port, **kwargs)
		self.sync_db = self.sync_client[dbname]

		# Check (and create if missing) essential indexes
		logger.info("Checking index (may take a while first time...)")

		# Build dates indexes first
		if not has_index(self.sync_db.messages.index_information(), [("date",-1)]):
			self.sync_db.messages.create_index([("date",-1)], name="alemibot-chronological")
		if not has_index(self.sync_db.service.index_information(), [("date",-1)]):
			self.sync_db.service.create_index([("date",-1)], name="alemibot-chronological")
		if not has_index(self.sync_db.deletions.index_information(), [("date",-1)]):
			self.sync_db.deletions.create_index([("date",-1)], name="alemibot-chronological")
		if not has_index(self.sync_db.members.index_information(), [("date",-1)]):
			self.sync_db.members.create_index([("date",-1)], name="alemibot-chronological")
		# This is not unique but still speeds up a ton
		if not has_index(self.sync_db.members.index_information(), [("chat",1),("user",1),("date",1)]):
			self.sync_db.members.create_index([("chat",1),("user",1),("date",1)], name="alemibot-member-history")
		# Building these may fail, run datafix script with duplicates option
		try:
			if not has_index(self.sync_db.messages.index_information(), [("chat",1),("id",1),("date",-1)]):
				self.sync_db.messages.create_index([("chat",1),("id",1),("date",-1)],
												name="alemibot-unique-messages", unique=True)
			if not has_index(self.sync_db.service.index_information(), [("chat",1),("id",1),("date",-1)]):
				self.sync_db.service.create_index([("chat",1),("id",1),("date",-1)],
												name="alemibot-unique-service", unique=True)
			if not has_index(self.sync_db.deletions.index_information(), [("chat",1),("id",1),("date",-1)]):
				self.sync_db.deletions.create_index([("chat",1),("id",1),("date",-1)],
												name="alemibot-unique-deletions", unique=True)
		except:
			logger.exception("Error while building unique indexes. Check util/datafix.py if there are duplicates")
		# Then make user and chat indexes
		try:
			if not has_index(self.sync_db.users.index_information(), [("id",1)]):
				self.sync_db.users.create_index([("id",1)], name="alemibot-unique-users", unique=True)
			if not has_index(self.sync_db.chats.index_information(), [("id",1)]):
				self.sync_db.chats.create_index([("id",1)], name="alemibot-unique-chats", unique=True)
		except:
			logger.exception("Error while building users/chats indexes. Not having these indexes will affect performance!")

	def log_error_event(self, func):
		"""will log exceptions to db

		If an error happens while parsing and serializing an event, this decorator
		will catch it and log to a separate db the whole event with a stacktrace.
		"""
		@functools.wraps(func)
		async def wrapper(client, message):
			try:
				await func(client, message)
			except ServerSelectionTimeoutError as ex:
				logger.error("Could not connect to MongoDB")
				logger.info(str(message))
			except DuplicateKeyError as e:
				error_key = getattr(e, '_OperationFailure__details')["keyValue"]
				logger.error(f"Rejecting duplicate document | {error_key}")
			except Exception as ex:
				logger.exception("Serialization error")
				exc_data = {
					"type" : repr(ex),
					"text" : str(ex),
					"traceback" : traceback.format_exc(),
				}
				doc = convert_to_dict(message)
				doc["exception"] = exc_data
				await self.db.exceptions.insert_one(doc)
		return wrapper

	async def fetch_user(self, uid:int, client:'pyrogram.Client') -> dict:
		"""get a user from db or telegram

		Try to fetch an user from database and, if missing, fetch it from telegram and insert it.
		Needs a client instance to fetch from telegram if missing.
		"""
		usr = await self.db.users.find_one({"id":uid})
		if not usr:
			try:
				usr = extract_user(await client.get_users(uid))
			except PeerIdInvalid:
				return {"id":uid}
			await self.db.users.insert_one(usr)
		return usr

	async def log_raw_event(self, event:Any):
		await self.db.raw.insert_one(convert_to_dict(event))

	async def parse_message_event(self, message:Message, file_name=None):
		msg = extract_message(message)
		if file_name:
			msg["file"] = file_name
		await self.db.messages.insert_one(msg)
		self.counter.messages()

		if message.from_user:
			await self.db.chats.update_one({"id":message.chat.id}, {"$inc": {f"messages.{message.from_user.id}":1}})
			await self.db.users.update_one({"id":message.from_user.id}, {"$inc": {"messages":1}})

		# Log users writing in dms so we have stats!
		if message.chat.type == "private":
			usr = extract_user(message.from_user)
			usr_id = usr["id"]
			prev = await self.db.users.find_one({"id": usr_id})
			if prev:
				usr = diff(prev, usr)
			else:
				self.counter.users()
			if usr: # don't insert if no diff!
				await self.db.users.update_one({"id": usr_id}, {"$set": usr}, upsert=True)

	async def parse_service_event(self, message:Message, ignore_duplicates=False):
		msg = extract_service_message(message)
		await self.db.service.insert_one(msg)
		if message.chat:
			chat = extract_chat(message.chat)
			chat_id = chat["id"]
			prev = await self.db.chats.find_one({"id": chat_id})
			if prev:
				chat = diff(prev, chat)
			else:
				self.counter.chats()
			if chat: # don't insert if no diff!
				await self.db.chats.update_one({"id": chat_id}, {"$set": chat}, upsert=True)

	async def parse_member_event(self, update:ChatMemberUpdated):
		doc = extract_member_update(update)
		await self.db.members.insert_one(doc)
		self.counter.members()

		usr = extract_user((update.new_chat_member or update.old_chat_member).user)
		usr_id = usr["id"]
		prev = await self.db.users.find_one({"id": usr_id})
		if prev:
			usr = diff(prev, usr)
		else:
			self.counter.users()
		if usr: # don't insert if no diff!
			await self.db.users.update_one({"id": usr_id}, {"$set": usr}, upsert=True)

	async def parse_edit_event(self, message:Message): # TODO replace `text` so that we always query most recent edit
		self.counter.edits()
		doc = extract_edit_message(message)
		await self.db.messages.find_one_and_update(
			{"id": message.message_id, "chat": message.chat.id},
			{"$push": {"edits":	doc} }, sort=[("date",-1)]
		)

	async def parse_deletion_event(self, message:Message):
		deletions = extract_delete(message)
		for deletion in deletions:
			await self.db.deletions.insert_one(deletion)
			self.counter.deletions()

			flt = {"id": deletion["id"], "dup": None}
			if "chat" in deletion:
				flt["chat"] = deletion["chat"]
			await self.db.messages.update_one(flt, {"$set": {"deleted": deletion["date"]}})

	async def parse_status_update_event(self, user:User):
		if user.status == "offline": # just update last online date
			await self.db.users.update_one({"id": user.id}, # there are a ton of these, can't diff user every time I get one
				{"$set": {"last_online_date": datetime.utcfromtimestamp(user.last_online_date)} })

DRIVER = DatabaseDriver()
