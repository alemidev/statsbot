import asyncio
import functools
import traceback

from datetime import datetime
from typing import Any, List, Callable, Dict

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo.errors import ServerSelectionTimeoutError, DuplicateKeyError
from pymongo import ASCENDING, DESCENDING, MongoClient

from pyrogram import Client
from pyrogram.types import Message, User, ChatMemberUpdated
from pyrogram.errors import PeerIdInvalid, ChannelPrivate
from pyrogram.enums import ChatType

from alemibot import alemiBot
from alemibot.util.serialization import convert_to_dict

from .util.serializer import (
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
		self.start = datetime.now()
		self.storage = { k : 0 for k in keys }

	def __getattr__(self, name:str) -> Callable[[], int]:
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

def _log_error_event(func: Callable):
	"""will log exceptions to db

	If an error happens while parsing and serializing an event, this decorator
	will catch it and log to a separate db the whole event with a stacktrace.
	"""
	@functools.wraps(func)
	async def wrapper(self, event:Any, *args, **kwargs):
		try:
			await func(self, event, *args, **kwargs)
		except ServerSelectionTimeoutError as ex:
			logger.error("Could not connect to MongoDB")
			logger.info(str(event))
		except DuplicateKeyError as e:
			error_key = getattr(e, '_OperationFailure__details')["keyValue"]
			logger.warning(f"Rejecting duplicate document\n\t{error_key}\n\t{str(event)}")
		except Exception as ex:
			logger.exception("Serialization error")
			exc_data = {
				"type" : repr(ex),
				"text" : str(ex),
				"traceback" : traceback.format_exc(),
			}
			doc = convert_to_dict(event)
			doc["exception"] = exc_data
			await self.db.exceptions.insert_one(doc)
	return wrapper

async def insert_replace(db:AsyncIOMotorDatabase, collection:str, doc:dict) -> bool:
	"""Attempt to insert a document, replace any duplicate found"""
	try:
		await db[collection].insert_one(doc)
		return True
	except DuplicateKeyError as e:
		error_key = getattr(e, '_OperationFailure__details')["keyValue"]
		prev = await db[collection].find_one(error_key)
		doc["_id"] = prev["_id"]
		logger.warning("Replacing duplicate on %s | key %s", collection, str(error_key))
		await asyncio.gather(
			db.exceptions.insert_one({"_": "Replace", "prev": prev, "new": doc}),
			db[collection].replace_one(error_key, doc)
		)
	return False

def has_index(indexes, index):
	for name in indexes:
		if indexes[name]["key"] == index:
			return True
	return False

class DatabaseDriver:
	log_messages : bool
	log_service : bool
	log_media : bool

	counter : Counter
	client: AsyncIOMotorClient
	db : AsyncIOMotorDatabase

	def __init__(self):
		self.log_messages = False
		self.log_service = False
		self.log_media = False

		self.counter : Counter = Counter(["service", "messages", "deletions", "edits", "users", "chats"])

	async def configure(self, app:alemiBot):
		self.log_messages = app.config.getboolean("database", "log_messages", fallback=True)
		self.log_service = app.config.getboolean("database", "log_service", fallback=True)
		self.log_media = app.config.getboolean("database", "log_media", fallback=False)

		kwargs : Dict[str, Any] = {}
		host = app.config.get("database", "host", fallback="localhost")
		port = int(app.config.get("database", "port", fallback=27017))
		username = app.config.get("database", "username", fallback=None)
		dbname = app.config.get("database", "dbname", fallback="alemibot")
		if username:
			kwargs["username"] = username
		password = app.config.get("database", "password", fallback=None)
		if password:
			kwargs["password"] = password
		kwargs["connectTimeoutMS"] = app.config.getint("database", "timeout", fallback=3000)
		self.client = AsyncIOMotorClient(host, port, **kwargs)

		self.db = self.client[dbname]

		# Check (and create if missing) essential indexes
		logger.info("Checking index (may take a while first time...)")

		# Build dates indexes first
		if not has_index(await self.db.messages.index_information(), [("date",-1)]):
			await self.db.messages.create_index([("date",-1)], name="alemibot-chronological")
		if not has_index(await self.db.service.index_information(), [("date",-1)]):
			await self.db.service.create_index([("date",-1)], name="alemibot-chronological")
		if not has_index(await self.db.deletions.index_information(), [("date",-1)]):
			await self.db.deletions.create_index([("date",-1)], name="alemibot-chronological")
		if not has_index(await self.db.members.index_information(), [("date",-1)]):
			await self.db.members.create_index([("date",-1)], name="alemibot-chronological")
		# This is not unique but still speeds up a ton
		if not has_index(await self.db.members.index_information(), [("chat",1),("user",1),("date",1)]):
			await self.db.members.create_index([("chat",1),("user",1),("date",1)], name="alemibot-member-history")
		# This is very useful for counting messages for each member
		if not has_index(await self.db.messages.index_information(), [("user",1)]):
			await self.db.messages.create_index([("user",1)], name="alemibot-per-user")
		if not has_index(await self.db.service.index_information(), [("user",1)]):
			await self.db.service.create_index([("user",1)], name="alemibot-per-user")
		# Building these may fail, run datafix script with duplicates option
		try:
			if not has_index(await self.db.messages.index_information(), [("chat",1),("id",1),("date",-1)]):
				await self.db.messages.create_index([("chat",1),("id",1),("date",-1)],
												name="alemibot-unique-messages", unique=True)
			if not has_index(await self.db.service.index_information(), [("chat",1),("id",1),("date",-1)]):
				await self.db.service.create_index([("chat",1),("id",1),("date",-1)],
												name="alemibot-unique-service", unique=True)
			if not has_index(await self.db.deletions.index_information(), [("chat",1),("id",1),("date",-1)]):
				await self.db.deletions.create_index([("chat",1),("id",1),("date",-1)],
												name="alemibot-unique-deletions", unique=True)
		except:
			logger.exception("Error while building unique indexes. Check util/datafix.py if there are duplicates")
		# Then make user and chat indexes
		try:
			if not has_index(await self.db.users.index_information(), [("id",1)]):
				await self.db.users.create_index([("id",1)], name="alemibot-unique-users", unique=True)
			if not has_index(await self.db.chats.index_information(), [("id",1)]):
				await self.db.chats.create_index([("id",1)], name="alemibot-unique-chats", unique=True)
		except:
			logger.exception("Error while building users/chats indexes. Not having these indexes will affect performance!")


	async def fetch_user(self, uid:int, client:Client = None) -> dict:
		"""get a user from db or telegram

		Try to fetch an user from database and, if missing, fetch it from telegram and insert it.
		Needs a client instance to fetch from telegram missing users.
		"""
		usr = await self.db.users.find_one({"id":uid})
		if not usr:
			if not client:
				return {"id":uid}
			try:
				usr = extract_user(await client.get_users(uid))
				await self.db.users.insert_one(usr)
			except (PeerIdInvalid, ChannelPrivate) as e:
				logger.warning("Could not fetch user %d from Telegram : %s", uid, str(e))
				return {"id":uid}
			except (ServerSelectionTimeoutError, DuplicateKeyError) as e:
				logger.warning("Could not fetch user %d from db : %s", uid, str(e))
				pass # ignore, usr has been set anyway
		return usr

	async def fetch_chat(self, cid:int, client:Client = None) -> dict:
		"""get a chat from db or telegram

		Try to fetch a chat from database and, if missing, fetch it from telegram and insert it.
		Needs a client instance to fetch from telegram missing chats.
		"""
		chat = await self.db.chats.find_one({"id":cid})
		if not chat:
			if not client:
				return {"id":cid}
			try:
				chat = extract_chat(await client.get_chat(cid))
				await self.db.users.insert_one(chat)
			except (PeerIdInvalid, ChannelPrivate) as e:
				logger.warning("Could not fetch chat %d from db : %s", cid, str(e))
				return {"id":cid}
			except (ServerSelectionTimeoutError, DuplicateKeyError) as e:
				logger.warning("Could not fetch chat %d from db : %s", cid, str(e))
				pass # ignore, chat has been set anyway
		return chat

	@_log_error_event
	async def log_raw_event(self, event:Any):
		await self.db.raw.insert_one(convert_to_dict(event))

	@_log_error_event
	async def parse_message_event(self, message:Message, file_name=None):
		msg = extract_message(message)
		if file_name:
			msg["file"] = file_name

		if await insert_replace(self.db, 'messages', msg):
			self.counter.messages()

		await self.db.chats.update_one({"id":message.chat.id}, {"$inc": {"messages.total":1}})
		if message.from_user:
			await self.db.chats.update_one({"id":message.chat.id}, {"$inc": {f"messages.{message.from_user.id}":1}})
			await self.db.users.update_one({"id":message.from_user.id}, {"$inc": {"messages":1}})

		# Log users writing in dms so we have stats!
		if message.chat.type == ChatType.PRIVATE:
			usr = extract_user(message.from_user)
			usr_id = usr["id"]
			prev = await self.db.users.find_one({"id": usr_id})
			if prev:
				usr = diff(prev, usr)
			else:
				self.counter.users()
				usr["messages"] = 0
			if usr: # don't insert if no diff!
				await self.db.users.update_one({"id": usr_id}, {"$set": usr}, upsert=True)

	@_log_error_event
	async def parse_service_event(self, message:Message):
		msg = extract_service_message(message)
		await insert_replace(self.db, 'service', msg)
		if message.chat:
			chat = extract_chat(message.chat)
			chat_id = chat["id"]
			prev = await self.db.chats.find_one({"id": chat_id})
			if prev:
				chat = diff(prev, chat)
			else:
				self.counter.chats()
				chat["messages.total"] = 0 if message._client.me.is_bot or message.chat.type not in ("supergroup", "channel") \
						else await message._client.get_history_count(chat_id) # Accessing _client is a cheap fix
			if chat: # don't insert if no diff!
				await self.db.chats.update_one({"id": chat_id}, {"$set": chat}, upsert=True)

	@_log_error_event
	async def parse_member_event(self, update:ChatMemberUpdated):
		doc = extract_member_update(update)
		if await insert_replace(self.db, 'members', doc):
			self.counter.members()

		usr = extract_user((update.new_chat_member or update.old_chat_member).user)
		usr_id = usr["id"]
		prev = await self.db.users.find_one({"id": usr_id})
		if prev:
			usr = diff(prev, usr)
		else:
			self.counter.users()
			usr["messages"] = 0
		if usr: # don't insert if no diff!
			await self.db.users.update_one({"id": usr_id}, {"$set": usr}, upsert=True)

	@_log_error_event
	async def parse_edit_event(self, message:Message): # TODO replace `text` so that we always query most recent edit
		self.counter.edits()
		doc = extract_edit_message(message)
		await self.db.messages.find_one_and_update(
			{"id": message.id, "chat": message.chat.id},
			{"$push": {"edits":	doc} }, sort=[("date",-1)]
		)

	@_log_error_event
	async def parse_deletion_event(self, message:List[Message]):
		deletions = extract_delete(message)
		for deletion in deletions:
			if await insert_replace(self.db, 'deletions', deletion):
				self.counter.deletions()

			flt = {"id": deletion["id"]}
			if "chat" in deletion:
				flt["chat"] = deletion["chat"]
			else:
				flt["chat"] = { "$ge":0 }
			await self.db.messages.update_one(flt, {"$set": {"deleted": deletion["date"]}})

	@_log_error_event
	async def parse_status_update_event(self, user:User):
		if user.status == "offline": # just update last online date
			await self.db.users.update_one({"id": user.id}, # there are a ton of these, can't diff user every time I get one
				{"$set": {"last_online_date": datetime.utcfromtimestamp(user.last_online_date)} })

DRIVER = DatabaseDriver()

@alemiBot.on_ready() # TODO make sure nothing
async def register_db_connection(client:alemiBot):
	await DRIVER.configure(client)
