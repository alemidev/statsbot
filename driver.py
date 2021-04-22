from pymongo import MongoClient

from bot import alemiBot

class Database:
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

		self.client = MongoClient(host, port, **kwargs)
		self.db = self.client[alemiBot.config.get("database", "dbname", fallback="alemibot")]

DB = Database()