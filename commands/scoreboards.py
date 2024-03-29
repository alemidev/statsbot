import random

from time import time
from datetime import datetime
from uuid import uuid4
from typing import Dict, Any

from pymongo import ASCENDING

from pyrogram.types import InputTextMessageContent, InlineQueryResultArticle, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import UserNotParticipant, PeerIdInvalid
from pyrogram.enums import ParseMode, ChatType

from alemibot import alemiBot

from alemibot.util.command import _Message as Message

from alemibot.util import (
	filterCommand, is_allowed, sudo,  ProgressChatAction, edit_or_reply, sep,
	get_username, get_user, report_error, set_offline, cancel_chat_action, HelpCategory
)

from ..driver import DRIVER
from ..util.getters import get_doc_username

import logging
logger = logging.getLogger(__name__)

HELP = HelpCategory("SCOREBOARDS")

# @HELP.add()
# @alemiBot.on_message(is_allowed & filterCommand(["rep", "reputation"]))
# @report_error(logger)
# @set_offline
# async def reputation_cmd(client:alemiBot, message:Message):
# 	if not message.reply_to_message:
# 		return await edit_or_reply(message, "`[!] → ` Reply to someone")
# 	if len(message.command) <= 0:
# 		return await edit_or_reply(message, "`[!] → ` ")

def level_from_points(pts:int, amount:int=10, factor:int=2):
	lvl = 0
	while pts >= amount:
		pts -= amount
		amount *= factor
		lvl += 1
	return lvl

@HELP.add(sudo=False)
@alemiBot.on_message(is_allowed & filterCommand(["stats", "stat"]))
@report_error(logger)
@set_offline
@cancel_chat_action
async def stats_cmd(client:alemiBot, message:Message):
	"""get your stats

	Will show your first sighting and count your sent messages, \
	chats you visited, chats you partecipated in, edits and media sent.
	"""
	if not get_user(message):
		return await edit_or_reply(message, "`[!] → ` You are no one")
	if message.sender_chat: # If sent anonymous msg, do /groupstats by default
		return await group_stats_cmd(client, message)
	user = None
	if len(message.command) > 0 and sudo(client, message):
		target_user = await client.get_users(int(message.command[0])
				if message.command[0].isnumeric() else message.command[0])
		user = await DRIVER.fetch_user(target_user.id, client)
	else:
		user = await DRIVER.fetch_user(get_user(message).id, client)
	uid = user["id"]
	total_messages = int(user["messages"] if "messages" in user else 0)
	with ProgressChatAction(client, message.chat.id) as prog:
		total_media = await DRIVER.db.messages.count_documents({"user":uid,"media":{"$exists":1}})
		total_edits = await DRIVER.db.messages.count_documents({"user":uid,"edits":{"$exists":1}})
		total_replies = await DRIVER.db.messages.count_documents({"user":uid,"reply":{"$exists":1}})
		visited_chats = len(await DRIVER.db.service.distinct("chat", {"user":uid}))
		partecipated_chats = len(await DRIVER.db.messages.distinct("chat", {"user":uid}))
		scoreboard_all_users = await DRIVER.db.users.find({"flags.bot":False}, {"_id":0,"id":1,"messages":1}).to_list(None)
		scoreboard_all_users = sorted([ (doc["id"], doc["messages"]) for doc in scoreboard_all_users if "messages" in doc ], key=lambda x: -x[1])
		scoreboard_id_only = [x[0] for x in scoreboard_all_users]
		position = (scoreboard_id_only.index(user["id"]) + 1) if user["id"] in scoreboard_id_only else len(scoreboard_id_only)
		position = sep(position) + (f" {'☆'*(4-position)}" if position < 4 else "")
		# Calculate msgs/minute sent today
		today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) # Force start of day
		minutes_passed_today = (datetime.utcnow() - today).total_seconds() / 60
		msgs_today = await DRIVER.db.messages.count_documents({"user":uid,"date":{"$gte":today}})
		msgs_per_minute_today = msgs_today / minutes_passed_today
		# Calculate oldest message
		oldest = datetime.now()
		oldest_message = await DRIVER.db.messages.find_one({"user":uid}, sort=[("date",ASCENDING)])
		if oldest_message:
			oldest = oldest_message["date"]
		oldest_event = await DRIVER.db.service.find_one({"user":uid}, sort=[("date",ASCENDING)])
		if oldest_event:
			oldest = min(oldest, oldest_event["date"])
	points = (
		partecipated_chats * 25  +
		total_media        * 7   +
		visited_chats      * 5   +
		total_replies      * 3   +
		total_messages     * 2   +
		total_edits        * 1
	)
	welcome = random.choice(["Hi", "Hello", "Welcome", "Nice to see you", "What's up", "Good day"])
	await edit_or_reply(
		message,
		f"<code>→ </code> {welcome} <b>{get_doc_username(user)}</b> (lvl {level_from_points(points)})\n" +
		f"<code> → </code> You sent <b>{sep(total_messages)}</b> messages\n" +
		f"<code>  → </code> Position <b>{position}</b> on global scoreboard\n" +
		f"<code>  → </code> Average of <b>{msgs_per_minute_today:.2f}</b> messages per minute today\n" +
		f"<code>  → </code> <b>{sep(total_media)}</b> media | <b>{sep(total_replies)}</b> replies | <b>{sep(total_edits)}</b> edits\n" +
		f"<code> → </code> You visited <b>{sep(max(visited_chats, partecipated_chats))}</b> chats\n" +
		f"<code>  → </code> and partecipated in <b>{sep(partecipated_chats)}</b>\n" +
		f"<code> → </code> First saw you <code>{oldest}</code>",
		parse_mode=ParseMode.HTML, disable_web_page_preview=True
	)

@HELP.add(sudo=False)
@alemiBot.on_message(is_allowed & filterCommand(["groupstats", "groupstat"]))
@report_error(logger)
@set_offline
@cancel_chat_action
async def group_stats_cmd(client:alemiBot, message:Message):
	"""get current group stats

	Will show group message count and global scoreboard position.
	Will show users and active users.
	Will count media messages, replies and edits.
	"""
	if not message.chat:
		return await edit_or_reply(message, "`[!] → ` No chat")
	group = message.chat
	if len(message.command) > 0 and sudo(client, message):
		group = await client.get_chat(int(message.command[0])
				if message.command[0].isnumeric() else message.command[0])
	if group.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
		return await edit_or_reply(message, "`[!] → ` Group stats available only in groups and supergroups")
	with ProgressChatAction(client, message.chat.id) as prog:
		group_doc = await DRIVER.db.chats.find_one({"id":group.id, "messages":{"$exists":1}}, {"_id":0, "id":1, "messages":1})
		# total_messages = sum(group_doc["messages"][val] for val in group_doc["messages"]) if "messages" in group_doc else 0
		total_messages = group_doc["messages"]["total"]
		active_users = max(0, len(group_doc["messages"] if "messages" in group_doc else '') -1) # jank af don't judge me it works
		total_users = await client.get_chat_members_count(group.id)
		total_media = await DRIVER.db.messages.count_documents({"chat":group.id,"media":{"$exists":1}})
		total_edits = await DRIVER.db.messages.count_documents({"chat":group.id,"edits":{"$exists":1}})
		total_replies = await DRIVER.db.messages.count_documents({"chat":group.id,"reply":{"$exists":1}})
		scoreboard_all_chats = await DRIVER.db.chats.find({}, {"_id":0,"id":1,"messages":1}).to_list(None)
		# scoreboard_all_chats = sorted([ (doc["id"], sum(doc["messages"][val] for val in doc["messages"]) if "messages" in doc else 0) for doc in scoreboard_all_chats ], key=lambda x: -x[1])
		scoreboard_all_chats = sorted([ (doc["id"], doc["messages"]["total"]) for doc in scoreboard_all_chats if "messages" in doc and "total" in doc["messages"] ], key=lambda x: -x[1])
		scoreboard_id_only = [x[0] for x in scoreboard_all_chats]
		position = (scoreboard_id_only.index(group.id) + 1) if group.id in scoreboard_id_only else len(scoreboard_id_only)
		position = sep(position) + (f" {'☆'*(4-position)}" if position < 4 else "")
		# Calculate msgs/minute sent today
		today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) # Force start of day
		minutes_passed_today = (datetime.utcnow() - today).total_seconds() / 60
		msgs_today = await DRIVER.db.messages.count_documents({"chat":group.id,"date":{"$gte":today}})
		msgs_per_minute_today = msgs_today / minutes_passed_today
		# Calculate oldest message
		oldest = datetime.now()
		oldest_message = await DRIVER.db.messages.find_one({"chat":group.id}, sort=[("date",ASCENDING)])
		if oldest_message:
			oldest = oldest_message["date"]
		oldest_event = await DRIVER.db.service.find_one({"chat":group.id}, sort=[("date",ASCENDING)])
		if oldest_event:
			oldest = min(oldest, oldest_event["date"])
	welcome = random.choice(["Greetings", "Hello", "Good day"])
	await edit_or_reply(
		message,
		f"<code>→ </code> {welcome} members of <b>{get_username(group)}</b>\n" +
		f"<code> → </code> Your group counts <b>{sep(total_messages)}</b> messages\n" +
		f"<code>  → </code> Position <b>{position}</b> on global scoreboard\n" +
		f"<code>  → </code> Average of <b>{msgs_per_minute_today:.2f}</b> messages per minute today\n" +
		f"<code>  → </code> <b>{sep(total_media)}</b> media | <b>{sep(total_replies)}</b> replies | <b>{sep(total_edits)}</b> edits\n" +
		f"<code> → </code> Your group has <b>{sep(total_users)}</b> users\n" +
		f"<code>  → </code> of these, <b>{sep(active_users)}</b> sent at least 1 message\n" +
		f"<code> → </code> Started tracking this chat on <code>{oldest}</code>",
		parse_mode=ParseMode.HTML, disable_web_page_preview=True
	)


@HELP.add(cmd="[<chat>]")
@alemiBot.on_message(sudo & filterCommand(["topgroups", "topgroup", "top_groups", "top_group"], options={
	"results" : ["-r", "--results"],
	"offset" : ["-o", "--offset"],
}))
@report_error(logger)
@set_offline
@cancel_chat_action
async def top_groups_cmd(client:alemiBot, message:Message):
	"""list tracked messages for groups

	Checks (tracked) number of messages sent in each group.
	By default, will only list top 10 groups, but number of results can be specified with `-r`.
	An username/group id can be given to center scoreboard on that group.
	An offset can be manually specified too with `-o`.
	"""
	results = int(message.command["results"] or 10)
	offset = int(message.command["offset"] or 0)
	out = "<code>→ </code> Most active groups\n"
	msg = await edit_or_reply(message, out, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
	res = []
	out = ""
	with ProgressChatAction(client, message.chat.id) as prog:
		async for doc in DRIVER.db.chats.find({}, {"type":1, "messages.total":1, "title":1, "id":1, "username":1}):
			if doc["type"] not in ("group", "supergroup"):
				continue
			# res.append((doc, sum(doc["messages"][val] for val in doc["messages"]) if "messages" in doc else 0))
			if "messages" in doc and "total" in doc["messages"]:
				res.append((doc, doc["messages"]["total"]))
		res.sort(key=lambda x: -x[1])
		if len(message.command) > 0 and len(res) > results:
			target_group = await client.get_chat(int(message.command[0]) if message.command[0].isnumeric() else message.command[0])
			offset += [ doc[0]["id"] for doc in res ].index(target_group.id) - (results // 2)
		stars = 3 if len(res) > 3 else 0
		count = 0
		for doc, msgs in res:
			count += 1
			if count <= offset:
				continue
			if count > offset + results:
				break
			out += f"<code> → </code> <b>{count}. {get_doc_username(doc)}</b> [<b>{sep(msgs)}</b>] {'☆'*(stars+1-count)}\n"
	await edit_or_reply(msg, out, parse_mode=ParseMode.HTML, disable_web_page_preview=True)


def user_index(scoreboard, uid):
	for index, tup in enumerate(scoreboard):
		if tup[0] == uid:
			return index
	return -1

@HELP.add(cmd="[<user>]", sudo=False)
@alemiBot.on_message(is_allowed & filterCommand(["topmsg", "topmsgs", "top_messages"], options={
	"chat" : ["-g", "--group"],
	"results" : ["-r", "--results"],
	"offset" : ["-o", "--offset"],
}, flags=["-all", "-bots"]))
@report_error(logger)
@set_offline
@cancel_chat_action
async def top_messages_cmd(client:alemiBot, message:Message):
	"""list tracked messages for users

	Checks (tracked) number of messages sent by group members in this chat.
	Add flag `-all` to list all messages tracked of users in this chat, or `-g` and specify a group to count in (only for superusers).
	By default, will only list top 10 members, but number of results can be specified with `-r`.
	An username can be given to center scoreboard on that user.
	An offset can be manually specified too with `-o`.
	Add flag `-bots` to include bots in global scoreboard (always included in group scoreboards).
	"""
	results = min(int(message.command["results"] or 10), 100)
	offset = int(message.command["offset"] or 0)
	global_search = sudo(client, message) and message.command["-all"]
	target_chat = message.chat
	if sudo(client, message) and "chat" in message.command:
		tgt = int(message.command["chat"]) if message.command["chat"].isnumeric() \
			else message.command["chat"]
		target_chat = await client.get_chat(tgt)
	res = []
	out = "<code>→ </code> Messages sent <b>globally</b>\n" if global_search else f"<code>→ </code> Messages sent in <b>{get_username(target_chat)}</b>\n"
	msg = await edit_or_reply(message, out, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
	with ProgressChatAction(client, message.chat.id) as prog:
		if global_search:
			query : Dict[str, Any] = {"messages":{"$exists":1}}
			if not message.command["-bots"]:
				query["flags.bot"] = False
			async for u in DRIVER.db.users.find(query, {"_id":0,"id":1,"messages":1}):
				res.append((u['id'], u['messages']))
		else:
			doc = await DRIVER.db.chats.find_one({"id":target_chat.id}, {"_id":0, "messages":1})
			if not doc or not doc["messages"]:
				return await edit_or_reply(msg, "<code>[!] → </code> No data available")
			res = [ (int(k), doc["messages"][k]) for k in doc["messages"].keys() if k.isnumeric() ]
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
			count += 1
			if count <= offset:
				continue
			if count > offset + results:
				break
			user_doc = await DRIVER.fetch_user(usr, client)
			out += f"<code> → </code> <b>{count}. {get_doc_username(user_doc)}</b> [<b>{sep(msgs)}</b>] {'☆'*(stars+1-count)}\n"
	await edit_or_reply(msg, out, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

@HELP.add(cmd="[<user>]", sudo=False)
@alemiBot.on_message(is_allowed & filterCommand(["joindate", "joindates", "join_date"], options={
	"chat" : ["-g", "--group"],
	"results" : ["-r", "--results"],
	"offset" : ["-o", "--offset"],
}, flags=["-query"]))
@report_error(logger)
@set_offline
@cancel_chat_action
async def joindate_cmd(client:alemiBot, message:Message):
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
	if sudo(client, message) and "chat" in message.command:
		tgt = int(message.command["chat"]) if message.command["chat"].isnumeric() \
			else message.command["chat"]
		target_chat = await client.get_chat(tgt)
	if target_chat.type in ("bot", "private"):
		return await edit_or_reply(message, "<code>[!] → </code> Can't query join dates in private chat")
	res = []
	out = f"<code>→ </code> Join dates in <b>{get_username(target_chat)}</b>\n"
	msg = await edit_or_reply(message, out, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
	with ProgressChatAction(client, message.chat.id) as prog:
		members = await DRIVER.db.chats.find_one({"id":target_chat.id},{"_id":0,"messages":1})
		if members is None:
			return await edit_or_reply(msg, "<code>[!] → </code> This group has not been indexed yet")
		members = [ int(k) for k in members["messages"].keys() if k.isnumeric() ] if "messages" in members else []
		for uid in members:
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
				except (UserNotParticipant, PeerIdInvalid): # user left, can't query for a date anymore
					continue
				joined_date = m.joined_date
				if joined_date is None: # Chat creator, get 1st event in target chat
					if client.me.is_bot:
						joined_date = 0 # cheap fix for bots
					else:
						first_message = await client.get_history(target_chat.id, limit=1, reverse=True)
						joined_date = first_message[0].date
				await DRIVER.db.members.insert_one(
					{"chat":target_chat.id, "user":uid, "joined":True,
					"date":datetime.utcfromtimestamp(joined_date)})
				res.append((uid, datetime.utcfromtimestamp(joined_date)))
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
			count += 1
			if count <= offset:
				continue
			if count > offset + results:
				break
			user_doc = await DRIVER.fetch_user(usr, client)
			when = "CREATOR" if date < datetime(1970, 1, 1, 1, 0, 0) else str(date) # cheap fix for bots
			out += f"<code> → </code> <b>{count}. {get_doc_username(user_doc)}</b> [<code>{when}</code>] {'☆'*(stars+1-count)}\n"
	await edit_or_reply(msg, out, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
