"""
This is a utility tool to fix stuff in your database. Move it to your bot root folder and run.
Right now this tool can:
* Convert all dates from int to datetime
"""
if __name__ == "__main__":
	import sys
	import asyncio
	from time import time
	from datetime import datetime

	from pymongo import ASCENDING
	from pymongo.errors import DuplicateKeyError

	from plugins.statsbot.driver import DRIVER

	COLLECTIONS = ["messages", "service", "deletions"]
	LAST = time()
	def progress(curr, total, warn=False, interval=1):
		global LAST
		if time() - LAST > interval:
			print(f"{'[!]' if warn else ''}\t{curr}/{total}           ", end="\r")
			LAST = time()


	if sys.argv[1] in ("date", "dates"):
		total = 0
		for coll in COLLECTIONS:
			total += DRIVER.sync_db[coll].count_documents({"date":{"$type":"int"}})

		curr = 0
		for coll in COLLECTIONS:
			for doc in DRIVER.sync_db[coll].find({"date":{"$type":"int"}}):
				curr += 1
				progress(curr, total)
				DRIVER.sync_db[coll].update_one(
					{"id":doc["id"],"chat":doc["chat"]},
					[
						{"date":{"$set":datetime.utcfromtimestamp(doc["date"])}},
					]
				)

	elif sys.argv[1] in ("duplicates", "duplicate", "dupe", "dupes", "dup"):
		total = 0
		for coll in COLLECTIONS:
			total += DRIVER.sync_db[coll].count_documents({})

		curr = 0
		for coll in COLLECTIONS:
			for doc in DRIVER.sync_db[coll].find({}):
				curr += 1
				progress(curr, total)
				dupes = DRIVER.sync_db[coll].count_documents({"chat":doc["chat"],"id":doc["id"],"date":doc["date"]})
				while dupes > 1:
					DRIVER.sync_db[coll].delete_one({"chat":doc["chat"],"id":doc["id"],"date":doc["date"]})
					dupes = DRIVER.sync_db[coll].count_documents({"chat":doc["chat"],"id":doc["id"],"date":doc["date"]})

	elif sys.argv[1] in ("count_all", "count_all_messages"):
		for doc in DRIVER.sync_db['users'].find({}):
			DRIVER.sync_db.update_one({"id":doc["id"]}, {"$set": {"messages":0}})
		for doc in DRIVER.sync_db['chats'].find({}):
			DRIVER.sync_db.update_one({"id":doc["id"]}, {"$set": {"messages":{}}})

		total = DRIVER.sync_db['messages'].count_documents({})

		curr = 0
		for doc in DRIVER.sync_db['messages'].find({}):
			curr += 1
			progress(curr, total)
			DRIVER.sync_db.chats.update_one({"id":doc["chat"]}, {"$inc": {f"messages.total":1}})
			if not str(doc["user"]).startswith("-"):
				DRIVER.sync_db.chats.update_one({"id":doc["chat"]}, {"$inc": {f"messages.{doc['user']}":1}})
				DRIVER.sync_db.users.update_one({"id":doc["user"]}, {"$inc": {"messages":1}})
	elif sys.argv[1] in ("joindates", "migrate_joins"):
		total  = DRIVER.sync_db.service.count_documents({"new_chat_members":{"$exists":1}})
		total += DRIVER.sync_db.service.count_documents({"left_chat_member":{"$exists":1}})

		curr = 0
		for doc in DRIVER.sync_db.service.find({"new_chat_members":{"$exists":1}}):
			curr += 1
			progress(curr, total)
			for m in doc["new_chat_members"]:
				DRIVER.sync_db.members.insert_one({"chat":doc["chat"], "date":doc["date"], "user":m, "performer":doc["user"], "joined":True})
		for doc in DRIVER.sync_db.service.find({"left_chat_member":{"$exists":1}}):
			curr += 1
			progress(curr, total)
			DRIVER.sync_db.members.insert_one({"chat":doc["chat"], "date":doc["date"], "user":doc["left_chat_member"], "performer":doc["user"], "left":True})
	elif sys.argv[1] in ("count_user", "count_user_messages"):
		total  = DRIVER.sync_db.users.count_documents({})

		curr = 0
		for doc in DRIVER.sync_db.users.find({}):
			curr += 1
			progress(curr, total)
			count = DRIVER.sync_db.messages.count_documents({"user":doc["id"]})
			DRIVER.sync_db.users.update_one({"id":doc["id"]}, {"$set":{"messages":count}})
	elif sys.argv[1] in ("count_chat_messages"):
		from pyrogram import Client
		with Client(sys.argv[2] if len(sys.argv) > 2 else "alemibot") as app:
			total  = DRIVER.sync_db.chats.count_documents({})
			me = app.get_me()
			curr = 0
			for doc in DRIVER.sync_db.chats.find({"type":"supergroup"}):
				curr += 1
				progress(curr, total)
				if me.is_bot:	
					if "messages" not in doc:
						continue
					count = sum(int(doc["messages"][val]) for val in doc["messages"])
				else:
					try:
						count = app.get_history_count(doc["id"])
					except Exception:
						continue
				if "messages" not in doc:
					DRIVER.sync_db.chats.update_one({"id":doc["id"]}, {"$set":{"messages":{}}}, upsert=True)
				DRIVER.sync_db.chats.update_one({"id":doc["id"]}, {"$set":{"messages.total":count}}, upsert=True)
	else:
		raise ValueError("No command given")
