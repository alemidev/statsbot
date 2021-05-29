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

	if sys.argv[1] in ("duplicates", "duplicate", "dupe", "dupes", "dup"):
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
