"""
This is a migration script for the old database structure (aka none, just log everything).
To run this, move it to your bot root folder. Of course, have statsbot installed and set up.
Replace OLD_DB and OLD_COLLECTION with the db and collection names you had set up in your config.
The new db name will be read off config.ini. Consider moving your old collection to a separate db.
Switch to admin db and run this command
	db.runCommand({renameCollection:"alemibot.events",to:"old.events"})	
"""
if __name__ == "__main__":
	import traceback
	import asyncio
	from time import time

	from pymongo import ASCENDING
	from pymongo.errors import DuplicateKeyError

	from plugins.statsbot.driver import DRIVER

	OLD_DB = "old"	# REPLACEME
	OLD_COLLECTION = "events"	# REPLACEME

	class DictWrapper:
		def __init__(self, storage:dict):
			self.storage = storage 

		def __repr__(self):
			return repr(self.storage)

		def __str__(self):
			return str(self.storage)

		def __getattr__(self, name:str):
			if name in self.storage:
				if isinstance(self.storage[name], dict):
					return DictWrapper(self.storage[name])
				elif isinstance(self.storage[name], list):
					return [ (DictWrapper(e) if type(e) is dict else e) for e in self.storage[name]  ]
				return self.storage[name]
			return None

	async def parse_dict(doc:dict):
		if isinstance(doc, list):
			await DRIVER.parse_deletion_event([ DictWrapper(m) for m in doc ])
		elif doc["_"] == "Message":
			if "service" in doc: 
				await DRIVER.parse_service_event(DictWrapper(doc))
			elif "edit_date" in doc: 
				await DRIVER.parse_edit_event(DictWrapper(doc))
			else:
				await DRIVER.parse_message_event(DictWrapper(doc))
		elif doc["_"] == "Delete":
			await DRIVER.parse_deletion_event([DictWrapper(doc)])

	LAST = time()
	def progress(curr, total, warn=False, interval=1):
		global LAST
		if time() - LAST > interval:
			print(f"{'[!]' if warn else ''}\t{curr}/{total}           ", end="\r")
			LAST = time()

	async def main():
		total = await DRIVER.client[OLD_DB][OLD_COLLECTION].count_documents({})
		curr = 0

		async for doc in DRIVER.client[OLD_DB][OLD_COLLECTION].find({}):
			curr += 1
			try:
				parse_dict(doc)
				progress(curr, total)
			except DuplicateKeyError:
				progress(curr, total, warn=True)
			except:
				print()
				traceback.print_exc()
				break

	asyncio.run(main())
