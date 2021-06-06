import random

from time import time
from datetime import datetime
from uuid import uuid4
                             
from pymongo import ASCENDING

from pyrogram.types import InputTextMessageContent, InlineQueryResultArticle, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import UserNotParticipant

from bot import alemiBot

from util.command import filterCommand
from util.permission import is_allowed, check_superuser
from util.message import ProgressChatAction, edit_or_reply
from util.text import sep
from util.getters import get_username, get_channel, get_user
from util.decorators import report_error, set_offline, cancel_chat_action
from util.help import HelpCategory

from plugins.statsbot.driver import DRIVER
from plugins.statsbot.util.getters import get_doc_username

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
	if not get_user(message):
		return await edit_or_reply(message, "`[!] → ` You are no one")
	user = None
	if len(message.command) > 0 and check_superuser(message):
		target_user = await client.get_users(int(message.command[0])
				if message.command[0].isnumeric() else message.command[0])
		user = await DRIVER.fetch_user(target_user.id, client)
	else:
		user = await DRIVER.fetch_user(get_user(message).id, client)
	prog = ProgressChatAction(client, message.chat.id)
	uid = user["id"]
	total_messages = sep(int(user["messages"]))
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
	scoreboard_all_users = await DRIVER.db.users.find({}, {"_id":0,"id":1,"messages":1}).to_list(None)
	await prog.tick()
	scoreboard_all_users = sorted([ (doc["id"], doc["messages"]) for doc in scoreboard_all_users ], key=lambda x: -x[1])
	position = [x[0] for x in scoreboard_all_users].index(message.from_user.id)
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
	await edit_or_reply(message, f"<code>→ </code> {welcome} <b>{get_doc_username(user)}</b>\n" +
								 f"<code> → </code> You sent <b>{sep(total_messages)}</b> messages\n" +
								 f"<code>  → </code> Position <b>{sep(position+1)}</b> on global scoreboard\n" +
								 f"<code>  → </code> <b>{sep(total_media)}</b> media | <b>{sep(total_replies)}</b> replies | <b>{sep(total_edits)}</b> edits\n" +
								 f"<code> → </code> You visited <b>{sep(max(visited_chats, partecipated_chats))}</b> chats\n" +
								 f"<code>  → </code> and partecipated in <b>{sep(partecipated_chats)}</b>\n" +
								 f"<code> → </code> First saw you <code>{oldest}</code>", parse_mode="html"
	)

@HELP.add(sudo=False)
@alemiBot.on_message(is_allowed & filterCommand(["groupstats", "groupstat"], list(alemiBot.prefixes)))
@report_error(logger)
@set_offline
@cancel_chat_action
async def group_stats_cmd(client, message):
	"""get current group stats

	Will show group message count and global scoreboard position.
	Will show users and active users.
	Will count media messages, replies and edits.
	"""
	if not message.chat:
		return await edit_or_reply(message, "`[!] → ` No chat")
	group = message.chat
	if len(message.command) > 0 and check_superuser(message):
		chat = await client.get_chat(int(message.command[0])
				if message.command[0].isnumeric() else message.command[0])
	if group.type not in ("group", "supergroup"):
		return await edit_or_reply(message, "`[!] → ` Group stats available only in groups and supergroups")
	prog = ProgressChatAction(client, message.chat.id)
	await prog.tick()
	group_doc = await DRIVER.db.chats.find_one({"id":group.id}, {"_id":0, "id":1, "messages":1})
	total_messages = sum(group_doc["messages"][val] for val in group_doc["messages"]) if "messages" in group_doc else 0
	active_users = len(group_doc["messages"])
	await prog.tick()
	total_users = await client.get_chat_members_count(group.id)
	await prog.tick()
	total_media = sep(await DRIVER.db.messages.count_documents({"chat":group.id,"media":{"$exists":1}}))
	await prog.tick()
	total_edits = sep(await DRIVER.db.messages.count_documents({"chat":group.id,"edits":{"$exists":1}}))
	await prog.tick()
	total_replies = sep(await DRIVER.db.messages.count_documents({"chat":group.id,"reply":{"$exists":1}}))
	await prog.tick()
	scoreboard_all_chats = await DRIVER.db.chats.find({}, {"_id":0,"id":1,"messages":1}).to_list(None)
	await prog.tick()
	scoreboard_all_chats = sorted([ (doc["id"], sum(doc["messages"][val] for val in doc["messages"]) if "messages" in doc else 0) for doc in scoreboard_all_chats ], key=lambda x: -x[1])
	position = [x[0] for x in scoreboard_all_chats].index(group.id)
	oldest = datetime.now()
	oldest_message = await DRIVER.db.messages.find_one({"chat":group.id}, sort=[("date",ASCENDING)])
	if oldest_message:
		oldest = oldest_message["date"]
	await prog.tick()
	oldest_event = await DRIVER.db.service.find_one({"chat":group.id}, sort=[("date",ASCENDING)])
	if oldest_event:
		oldest = min(oldest, oldest_event["date"])
	await prog.tick()
	welcome = random.choice(["Greetings", "Hello", "Good day"])
	await edit_or_reply(message, f"<code>→ </code> {welcome} members of <b>{get_username(group)}</b>\n" +
								 f"<code> → </code> Your group counts <b>{sep(total_messages)}</b> messages\n" +
								 f"<code>  → </code> Position <b>{sep(position+1)}</b> on global scoreboard\n" +
								 f"<code>  → </code> <b>{sep(total_media)}</b> media | <b>{sep(total_replies)}</b> replies | <b>{sep(total_edits)}</b> edits\n" +
								 f"<code> → </code> Your chat has <b>{sep(total_users)}</b> users\n" +
								 f"<code>  → </code> of these, <b>{sep(active_users)}</b> sent at least 1 message\n" +
								 f"<code> → </code> Started tracking this chat on <code>{oldest}</code>", parse_mode="html"
	)

def user_index(scoreboard, uid):
	for index, tup in enumerate(scoreboard):
		if tup[0] == uid:
			return index
	return -1

@HELP.add(cmd="[<user>]", sudo=False)
@alemiBot.on_message(is_allowed & filterCommand(["topmsg", "topmsgs", "top_messages"], list(alemiBot.prefixes), options={
	"chat" : ["-g", "--group"],
	"results" : ["-r", "--results"],
	"offset" : ["-o", "--offset"],
}, flags=["-all"]))
@report_error(logger)
@set_offline
@cancel_chat_action
async def top_messages_cmd(client, message):
	"""list tracked messages for users

	Checks (tracked) number of messages sent by group members in this chat.
	Add flag `-all` to list all messages tracked of users in this chat, or `-g` and specify a group to count in (only for superusers).
	By default, will only list top 10 members, but number of results can be specified with `-r`.
	An username can be given to center scoreboard on that user.
	An offset can be manually specified too with `-o`.
	"""
	results = min(int(message.command["results"] or 10), 100)
	offset = int(message.command["offset"] or 0)
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
	await prog.tick()
	if global_search:
		async for u in DRIVER.db.users.find({"messages":{"$exists":1}}, {"_id":0,"id":1,"messages":1}):
			await prog.tick()
			res.append((u['id'], u['messages']))
	else:
		doc = await DRIVER.db.chats.find_one({"id":target_chat.id}, {"_id":0, "messages":1})
		res = [ (int(k), doc["messages"][k]) for k in doc["messages"].keys() ]
	if len(res) < 1:
		return await edit_or_reply(msg, "<code>[!] → </code> No results")
	res.sort(key=lambda x: -x[1])
	if len(message.command) > 0 and len(res) > results:
		target_user = await client.get_users(int(message.command[0]) if message.command[0].isnumeric() else message.command[0])
		offset += user_index(res, target_user.id) - (results // 2)
	stars = 3 if len(res) > 3 else 0
	count = 0
	out = ""
	for usr, msgs in res:
		await prog.tick()
		count += 1
		if count <= offset:
			continue
		if count > offset + results:
			break
		user_doc = await DRIVER.fetch_user(usr, client)
		out += f"<code> → </code> <b>{count}. {get_doc_username(user_doc)}</b> [<b>{sep(msgs)}</b>] {'☆'*(stars+1-count)}\n"
	await edit_or_reply(msg, out, parse_mode="html")

@HELP.add(cmd="[<user>]", sudo=False)
@alemiBot.on_message(is_allowed & filterCommand(["joindate", "joindates", "join_date"], list(alemiBot.prefixes), options={
	"chat" : ["-g", "--group"],
	"results" : ["-r", "--results"],
	"offset" : ["-o", "--offset"],
}, flags=["-query"]))
@report_error(logger)
@set_offline
@cancel_chat_action
async def joindate_cmd(client, message):
	"""list date users joined group

	Checks join date for users in current chat against database.
	Querying a very big group for users may take a very long time, so only join dates logged \
	to database are queried by default. Add flag `-query` to also search with \
	telegram queries (results will be stored so they can be reused).
	A specific group can be specified with `DRIVER.-g` (only by superusers).
	By default, will only list oldest 10 members, but number of results can be specified with `-r`.
	Specify an username to center leaderboard on that user.
	An offset can also be specified manually with `-o`.
	"""
	results = min(int(message.command["results"] or 10), 100)
	offset = int(message.command["offset"] or 0)
	also_query = bool(message.command["-query"])
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
	members = await DRIVER.db.chats.find_one({"id":target_chat.id},{"_id":0,"messages":1})
	members = [int(k) for k in members["messages"].keys()]
	for uid in members:
		await prog.tick()
		event = await DRIVER.db.members.find_one(
			{"chat":target_chat.id, "user":uid, "joined": {"$exists":1}},
			sort=[("date", ASCENDING)]
		)
		if not event: # search in service messages too
			event = await DRIVER.db.service.find_one(
				{"chat":target_chat.id, "user":uid, "new_chat_members":uid},
				sort=[("date", ASCENDING)]
			)
		if event:
			res.append((uid, event['date']))
		elif also_query:
			try:
				m = await client.get_chat_member(target_chat.id, uid)
			except UserNotParticipant: # user left, can't query for a date anymore
				continue
			await DRIVER.db.members.insert_one(
				{"chat":target_chat.id, "user":uid, "joined":True,
				"date":datetime.utcfromtimestamp(m.joined_date)})
			res.append((uid, datetime.utcfromtimestamp(m.joined_date)))
	if len(res) < 1:
		return await edit_or_reply(msg, "<code>[!] → </code> No results")
	res.sort(key=lambda x: x[1])
	if len(message.command) > 0 and len(res) > results:
		target_user = await client.get_users(int(message.command[0]) if message.command[0].isnumeric() else message.command[0])
		i = user_index(res, target_user.id)
		if i > 0:
			offset += i - (results // 2)
	stars = 3 if len(res) > 3 else 0
	count = 0
	out = ""
	for usr, date in res:
		await prog.tick()
		count += 1
		if count <= offset:
			continue
		if count > offset + results:
			break
		user_doc = await DRIVER.fetch_user(usr, client)
		out += f"<code> → </code> <b>{count}. {get_doc_username(user_doc)}</b> [<code>{str(date)}</code>] {'☆'*(stars+1-count)}\n"
	await edit_or_reply(msg, out, parse_mode="html")
