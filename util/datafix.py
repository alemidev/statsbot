"""
This is a utility tool to fix stuff in your database. Move it to your bot root folder and run.
Right now this tool can:
* Convert all dates from int to datetime
"""
if __name__ == "__main__":
	import sys
	from time import time
	from datetime import datetime

	from pymongo import ASCENDING
	from pymongo.errors import DuplicateKeyError

	from plugins.statsbot.driver import DRIVER

	LAST = time()
	def progress(curr, total, warn=False, interval=1):
		global LAST
		if time() - LAST > interval:
			print(f"{'[!]' if warn else ''}\t{curr}/{total}           ", end="\r")
			LAST = time()

	# if sys.argv[1] in ("date", "dates"):

	total = 0
	for coll in ["messages", "service", "deletions"]:
		total += DRIVER.db[coll].count_documents({"date":{"$type":"int"}})
		
	curr = 0
	for coll in ["messages", "service", "deletions"]:
		for doc in DRIVER.db[coll].find({"date":{"$type":"int"}}):
			curr += 1
			progress(curr, total)
			DRIVER.db[coll].update_one(
				{"id":doc["id"],"chat":doc["chat"]},
				[
					{"date":{"$set":datetime.utcfromtimestamp(doc["date"])}},
				]
			)
