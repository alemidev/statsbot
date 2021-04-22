from bot import alemiBot

from .driver import DB

from .util.serializer import extract_message, extract_user, extract_chat

# @alemiBot.on_message(group=999999) # happen last and always!
async def log_hook(client, message):
	msg = extract_message(message)
	chat = extract_chat(message)
	user = extract_user(message)
	old_user = DB.users.find_one({"id":user["id"]})
	if old_user:
		pass
		# check if different, update fields
	old_chat = DB.chats.find_one({"id":chat["id"]})
	if old_chat:
		pass
		# check if different, update fields
	DB.messages.insert_one(msg)


# @alemiBot.on_deleted_messages(group=999999)
async def log_deleted_hook(client, message):
	pass
	# TODO parse deletions