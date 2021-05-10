import random

from time import time
from datetime import datetime
from uuid import uuid4
                             
from pymongo import ASCENDING

from pyrogram.types import InputTextMessageContent, InlineQueryResultArticle, InlineKeyboardMarkup, InlineKeyboardButton

from bot import alemiBot

from util.command import filterCommand
from util.permission import is_allowed, check_superuser
from util.message import ProgressChatAction, edit_or_reply
from util.getters import get_username, get_channel, get_user
from util.decorators import report_error, set_offline, cancel_chat_action
from util.help import HelpCategory

from ..driver import DRIVER

import logging
logger = logging.getLogger(__name__)

HELP = HelpCategory("SCOREBOARDS")


@HELP.add(sudo=False)
@alemiBot.on_message(is_allowed & filterCommand(["stats", "stat"], list(alemiBot.prefixes)))
@report_error(logger)
@set_offline
@cancel_chat_action
async def stats_cmd(client, message):
	"""get your stats

	Will show your first sighting and count your sent messages, \
	chats you visited, chats you partecipated in, edits and media sent.
	"""
	user = get_user(message)
	if not user:
		return await edit_or_reply(message, "`[!] → ` You are no one")
	prog = ProgressChatAction(client, message.chat.id)
	await prog.tick()
	uid = user.id
	total_messages = DRIVER.db.messages.count_documents({"user":uid})
	await prog.tick()
	total_media = DRIVER.db.messages.count_documents({"user":uid,"media":{"$exists":1}})
	await prog.tick()
	total_edits = DRIVER.db.messages.count_documents({"user":uid,"edits":{"$exists":1}})
	await prog.tick()
	total_replies = DRIVER.db.messages.count_documents({"user":uid,"reply":{"$exists":1}})
	await prog.tick()
	visited_chats = len(DRIVER.db.service.distinct("chat", {"user":uid}))
	await prog.tick()
	partecipated_chats = len(DRIVER.db.messages.distinct("chat", {"user":uid}))
	await prog.tick()
	oldest = datetime.now()
	oldest_message = DRIVER.db.messages.find_one({"user":uid}, sort=[("date",ASCENDING)])
	if oldest_message:
		oldest = oldest_message["date"]
	await prog.tick()
	oldest_event = DRIVER.db.service.find_one({"user":uid}, sort=[("date",ASCENDING)])
	if oldest_event:
		oldest = min(oldest, oldest_event["date"])
	await prog.tick()
	welcome = random.choice(["Hi", "Hello", "Welcome", "Nice to see you", "What's up", "Good day"])
	await edit_or_reply(message, f"`→ ` {welcome} {get_username(user)}\n" +
								 f"` → ` You sent **{total_messages}** messages\n" +
								 f"`  → ` **{total_media}** were media\n" +
								 f"`  → ` **{total_replies}** were replies\n" +
								 f"`  → ` **{total_edits}** were edited\n" +
								 f"` → ` You visited **{max(visited_chats, partecipated_chats)}** chats\n" +
								 f"`  → ` and partecipated in **{partecipated_chats}**\n" +
								 f"` → ` First saw you `{oldest}`"
	)

@HELP.add(sudo=False)
@alemiBot.on_message(is_allowed & filterCommand(["topmsg", "topmsgs", "top_messages"], list(alemiBot.prefixes), options={
	"chat" : ["-g", "--group"],
	"results" : ["-r", "--results"],
}, flags=["-all"]))
@report_error(logger)
@set_offline
@cancel_chat_action
async def top_messages_cmd(client, message):
	"""list tracked messages for users

	Checks (tracked) number of messages sent by group members in this chat.
	Add flag `-all` to list all messages tracked of users in this chat, or `-g` and specify a group to count in (only for superusers).
	By default, will only list top 25 members, but number of results can be specified with `-r`.
	"""
	results = min(int(message.command["results"] or 25), 100)
	global_search = check_superuser(message) and message.command["-all"]
	target_chat = message.chat
	if check_superuser(message) and "chat" in message.command:
		tgt = int(message.command["chat"]) if message.command["chat"].isnumeric() \
			else message.command["chat"]
		target_chat = await client.get_chat(tgt)
	res = []
	prog = ProgressChatAction(client, message.chat.id)
	out = "`→ ` Messages sent globally\n" if global_search else f"`→ ` Messages sent in {get_username(target_chat)}\n"
	msg = await edit_or_reply(message, out)
	if len(message.command) > 0:
		for uname in message.command.arg:
			await prog.tick()
			user = await client.get_user(uname)
			flt = {"user": user.id}
			if not global_search:
				flt["chat"] = target_chat.id
			res.append((get_username(user), DRIVER.db.messages.count_documents(flt)))
	elif target_chat.type in ("bot", "private"):
		user = get_user(message)
		flt = {"user": user.id}
		if not global_search:
			flt["chat"] = target_chat.id
		res.append((get_username(user), DRIVER.db.messages.count_documents(flt)))
	else:
		async for member in target_chat.iter_members():
			await prog.tick()
			flt = {"user": member.user.id}
			if not global_search:
				flt["chat"] = target_chat.id
			res.append((get_username(member.user), DRIVER.db.messages.count_documents(flt)))
	res.sort(key=lambda x: x[1], reverse=True)
	stars = 3 if len(res) > 3 else 0
	count = 0
	out = ""
	for usr, msgs in res:
		out += f"` → ` **{usr}** [`{msgs}`] {'☆'*stars}\n"
		stars -= 1
		count += 1
		if count >= results:
			break
	await edit_or_reply(msg, out)

@HELP.add(cmd="[<users>]", sudo=False)
@alemiBot.on_message(is_allowed & filterCommand(["joindate", "joindates", "join_date"], list(alemiBot.prefixes), options={
	"chat" : ["-g", "--group"],
	"results" : ["-r", "--results"],
}))
@report_error(logger)
@set_offline
@cancel_chat_action
async def joindate_cmd(client, message):
	"""list date users joined group

	Checks join date for users in current chat (will count previous joins if available).
	A specific group can be specified with `-g` (only by superusers).
	By default, will only list oldest 25 members, but number of results can be specified with `-r`.
	"""
	results = min(int(message.command["results"] or 25), 100)
	target_chat = message.chat
	if check_superuser(message) and "chat" in message.command:
		tgt = int(message.command["chat"]) if message.command["chat"].isnumeric() \
			else message.command["chat"]
		target_chat = await client.get_chat(tgt)
	if target_chat.type in ("bot", "private"):
		return await edit_or_reply(message, "`[!] → ` Can't query join dates in private chat")
	res = []
	prog = ProgressChatAction(client, message.chat.id)
	out = f"`→ ` Join dates in {get_channel(target_chat)}\n"
	msg = await edit_or_reply(message, out)
	creator = None
	if len(message.command) > 0:
		for uname in message.command.arg:
			await prog.tick()
			member = await client.get_chat_member(target_chat.id, uname)
			res.append((get_username(member.user), datetime.utcfromtimestamp(member.joined_date)))
	else:
		creator = "~~UNKNOWN~~"
		async for member in target_chat.iter_members():
			await prog.tick()
			if member.status == "creator":
				creator = get_username(member.user)
			else: # Still query db, maybe user left and then joined again! Telegram only tells most recent join
				event = DRIVER.db.service.find_one({"new_chat_members":member.user.id,"chat":target_chat.id}, sort=[("date", ASCENDING)])
				if event:
					res.append((get_username(member.user), event['date']))
				else:
					res.append((get_username(member.user), datetime.utcfromtimestamp(member.joined_date)))
	res.sort(key=lambda x: x[1])
	stars = 3 if len(res) > 3 else 0
	count = 0
	out = ""
	if creator:
		out += f"`→ ` **{creator}** [`CREATED`]\n"
	for usr, date in res:
		out += f"` → ` **{usr}** [`{str(date)}`] {'☆'*stars}\n"
		stars -= 1
		count += 1
		if count >= results:
			break
	await edit_or_reply(msg, out)
