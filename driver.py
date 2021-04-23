from pymongo import MongoClient
from pyrogram.types import Message

from bot import alemiBot

from .util.serializer import diff, extract_chat, extract_message, extract_user, extract_delete

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

	def parse_message_event(self, message:Message, file_name=None):
		msg = extract_message(message)
		if file_name:
			msg["file"] = file_name
		self.db.messages.insert_one(msg)
		self.messages += 1

		usr = extract_user(message)
		usr_id = usr["id"]
		prev = self.db.users.find_one({"id": usr_id})
		if prev:
			usr = diff(prev, usr)
		else:
			self.users += 1
		if usr: # don't insert if no diff!
			self.db.users.update_one({"id": usr_id}, {"$set": usr}, upsert=True)

		chat = extract_chat(message)
		if chat:
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
		self.db.messages.update_one(
			{"id": message.message_id, "chat": message.chat.id},
			{"$set": {"edits": { message.date : message.text} } }
		)

	def parse_deletion_event(self, message:Message):
		deletion = extract_delete(message)
		self.db.deletions.insert_one(deletion)
		self.deletions += 1

		flt = {"id": deletion["id"]}
		if "chat" in deletion:
			flt["chat"] = deletion["chat"]
		self.db.messages.update_one(flt, {"$set": {"deleted": True}})

DRIVER = DatabaseDriver()