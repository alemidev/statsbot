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
from util.text import sep
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
	if len(message.command) > 0 and check_superuser(message):
		user = await client.get_users(int(message.command[0])
				if message.command[0].isnumeric() else message.command[0])
	if not user:
		return await edit_or_reply(message, "`[!] → ` You are no one")
	prog = ProgressChatAction(client, message.chat.id)
	await prog.tick()
	uid = user.id
	total_messages = sep(await DRIVER.db.messages.count_documents({"user":uid}))
	await prog.tick()
	total_media = sep(await DRIVER.db.messages.count_documents({"user":uid,"media":{"$exists":1}}))
	await prog.tick()
	total_edits = sep(await DRIVER.db.messages.count_documents({"user":uid,"edits":{"$exists":1}}))
	await prog.tick()
	total_replies = sep(await DRIVER.db.messages.count_documents({"user":uid,"reply":{"$exists":1}}))
	await prog.tick()
	visited_chats = len(await DRIVER.db.service.distinct("chat", {"user":uid}))
	await prog.tick()
	partecipated_chats = len(await DRIVER.db.messages.distinct("chat", {"user":uid}))
	await prog.tick()
	oldest = datetime.now()
	oldest_message = await DRIVER.db.messages.find_one({"user":uid}, sort=[("date",ASCENDING)])
	if oldest_message:
		oldest = oldest_message["date"]
	await prog.tick()
	oldest_event = await DRIVER.db.service.find_one({"user":uid}, sort=[("date",ASCENDING)])
	if oldest_event:
		oldest = min(oldest, oldest_event["date"])
	await prog.tick()
	welcome = random.choice(["Hi", "Hello", "Welcome", "Nice to see you", "What's up", "Good day"])
	await edit_or_reply(message, f"<code>→ </code> {welcome} <b>{get_username(user)}</b>\n" +
								 f"<code> → </code> You sent <b>{total_messages}</b> messages\n" +
								 f"<code>  → </code> <b>{total_media}</b> were media\n" +
								 f"<code>  → </code> <b>{total_replies}</b> were replies\n" +
								 f"<code>  → </code> <b>{total_edits}</b> were edited\n" +
								 f"<code> → </code> You visited <b>{sep(max(visited_chats, partecipated_chats))}</b> chats\n" +
								 f"<code>  → </code> and partecipated in <b>{sep(partecipated_chats)}</b>\n" +
								 f"<code> → </code> First saw you <code>{oldest}</code>", parse_mode="html"
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
	out = "<code>→ </code> Messages sent <b>globally</b>\n" if global_search else f"<code>→ </code> Messages sent in <b>{get_username(target_chat)}</b>\n"
	msg = await edit_or_reply(message, out, parse_mode="html")
	if len(message.command) > 0:
		for uname in message.command.arg:
			await prog.tick()
			user = await client.get_users(uname)
			flt = {"user": user.id}
			if not global_search:
				flt["chat"] = target_chat.id
			res.append((get_username(user), await DRIVER.db.messages.count_documents(flt)))
	elif target_chat.type in ("bot", "private"):
		user = get_user(message)
		flt = {"user": user.id}
		if not global_search:
			flt["chat"] = target_chat.id
		res.append((get_username(user), await DRIVER.db.messages.count_documents(flt)))
	else:
		async for member in target_chat.iter_members():
			await prog.tick()
			flt = {"user": member.user.id}
			if not global_search:
				flt["chat"] = target_chat.id
			res.append((get_username(member.user), await DRIVER.db.messages.count_documents(flt)))
	res.sort(key=lambda x: x[1], reverse=True)
	stars = 3 if len(res) > 3 else 0
	count = 0
	out = ""
	for usr, msgs in res:
		out += f"<code> → </code> <b>{usr}</b> [<b>{sep(msgs)}</b>] {'☆'*stars}\n"
		stars -= 1
		count += 1
		if count >= results:
			break
	await edit_or_reply(msg, out, parse_mode="html")

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
	A specific group can be specified with `DRIVER.-g` (only by superusers).
	By default, will only list oldest 25 members, but number of results can be specified with `-r`.
	"""
	results = min(int(message.command["results"] or 25), 100)
	target_chat = message.chat
	if check_superuser(message) and "chat" in message.command:
		tgt = int(message.command["chat"]) if message.command["chat"].isnumeric() \
			else message.command["chat"]
		target_chat = await client.get_chat(tgt)
	if target_chat.type in ("bot", "private"):
		return await edit_or_reply(message, "<code>[!] → </code> Can't query join dates in private chat")
	res = []
	prog = ProgressChatAction(client, message.chat.id)
	out = f"<code>→ </code> Join dates in <b>{get_channel(target_chat)}</b>\n"
	msg = await edit_or_reply(message, out, parse_mode="html")
	creator = None
	if len(message.command) > 0:
		for uname in message.command.arg:
			await prog.tick()
			member = await client.get_chat_member(target_chat.id, uname)
			res.append((get_username(member.user), datetime.utcfromtimestamp(member.joined_date)))
	else:
		creator = "<s>UNKNOWN</s>"
		async for member in target_chat.iter_members():
			await prog.tick()
			if member.status == "creator":
				creator = get_username(member.user)
			else: # Still query db, maybe user left and then joined again! Telegram only tells most recent join
				event = await DRIVER.db.service.find_one({"new_chat_members":member.user.id,"chat":target_chat.id}, sort=[("date", ASCENDING)])
				if event:
					res.append((get_username(member.user), event['date']))
				else:
					res.append((get_username(member.user), datetime.utcfromtimestamp(member.joined_date)))
	res.sort(key=lambda x: x[1])
	stars = 3 if len(res) > 3 else 0
	count = 0
	out = ""
	if creator:
		out += f"<code>→ </code> <b>{creator}</b> [<code>CREATED</code>]\n"
	for usr, date in res:
		out += f"<code> → </code> <b>{usr}</b> [<code>{str(date)}</code>] {'☆'*stars}\n"
		stars -= 1
		count += 1
		if count >= results:
			break
	await edit_or_reply(msg, out, parse_mode="html")
