import io
import os
import re
import json
import html
import asyncio

from datetime import datetime
from pymongo import ASCENDING, DESCENDING, collection
from pyrogram.errors import PeerIdInvalid

from bot import alemiBot

from util.command import filterCommand
from util.permission import is_allowed, is_superuser, check_superuser
from util.getters import get_username
from util.message import ProgressChatAction, edit_or_reply, is_me
from util.text import tokenize_json, order_suffix, sep
from util.decorators import report_error, set_offline, cancel_chat_action
from util.help import HelpCategory

from plugins.statsbot.driver import DRIVER
from plugins.statsbot.util.getters import get_doc_username

import logging
logger = logging.getLogger(__name__)

HELP = HelpCategory("BIGBROTHER")

# I'm very lazy and made this
async def _run_count(collection, full_count=False):
	if full_count:
		return await collection.count_documents({})
	else:
		return await collection.estimated_document_count()

@HELP.add()
@alemiBot.on_message(is_superuser & filterCommand(["dbstats", "dbstat"], list(alemiBot.prefixes), flags=["-count"]))
@report_error(logger)
@set_offline
@cancel_chat_action
async def dbstats_cmd(client, message):
	"""get database stats

	List collections, entries, new entries in this session and disk usage.
	Results are calculated from metadata for faster queries.
	Add flag `-count` to actually count entries (very slow with 1M+ entries!).
	"""
	full_count = bool(message.command["-count"])
	with ProgressChatAction(client, message.chat.id, random=True) as prog:
		oldest_msg = await DRIVER.db.messages.find_one({"date":{"$ne":None}}, sort=[("date", ASCENDING)])
		msg_count = sep(await _run_count(DRIVER.db.messages, full_count))
		user_count = sep(await _run_count(DRIVER.db.users, full_count))
		chat_count = sep(await _run_count(DRIVER.db.chats, full_count))
		deletions_count = sep(await _run_count(DRIVER.db.deletions, full_count))
		service_count = sep(await _run_count(DRIVER.db.service, full_count))
		members_count = sep(await _run_count(DRIVER.db.members, full_count))
		msg_size = order_suffix((await DRIVER.db.command("collstats", "messages"))['totalSize'])
		user_size = order_suffix((await DRIVER.db.command("collstats", "users"))['totalSize'])
		chat_size = order_suffix((await DRIVER.db.command("collstats", "chats"))['totalSize'])
		deletions_size = order_suffix((await DRIVER.db.command("collstats", "deletions"))['totalSize'])
		service_size = order_suffix((await DRIVER.db.command("collstats", "service"))['totalSize'])
		members_size = order_suffix((await DRIVER.db.command("collstats", "members"))['totalSize'])
		db_size = order_suffix((await DRIVER.db.command("dbstats"))["totalSize"])
		medianumber = sep(len(os.listdir("plugins/statsbot/data")))
		now = datetime.now()
		msgs_per_s = DRIVER.counter['messages'] / (now - client.start_time).total_seconds()
		service_per_h = DRIVER.counter['service'] / ((now - client.start_time).total_seconds() / 3600)
		deletions_per_s = DRIVER.counter['deletions'] / (now - client.start_time).total_seconds()
		members_per_h = DRIVER.counter['members'] / ((now - client.start_time).total_seconds() / 3600)
		users_per_h = DRIVER.counter['users'] / ((now - client.start_time).total_seconds() / 3600)
		chats_per_h = DRIVER.counter['chats'] / ((now - client.start_time).total_seconds() / 3600)
		proc = await asyncio.create_subprocess_exec( # This is not cross platform!
			"du", "-b", "plugins/statsbot/data/",
			stdout=asyncio.subprocess.PIPE,
			stderr=asyncio.subprocess.STDOUT)
		stdout, _stderr = await proc.communicate()
		mediasize = order_suffix(float(stdout.decode('utf-8').split("\t")[0]))

		uptime = str(datetime.now() - client.start_time)
		await edit_or_reply(message, f"<code>→ </code> <b>online for</b> <code>{uptime}</code>" +
						f"\n<code>→ </code> <b>first event</b> <code>{oldest_msg['date']}</code>" +
						f"\n<code> → </code> <b>{msg_count}</b> msgs logged (+{sep(DRIVER.counter['messages'])} new | <i>{msgs_per_s:.2f}/s</i> | <b>{msg_size}</b>)" +
						f"\n<code> → </code> <b>{service_count}</b> events tracked (+{sep(DRIVER.counter['service'])} new | <i>{service_per_h:.2f}/h</i> | <b>{service_size}</b>)" +
						f"\n<code> → </code> <b>{deletions_count}</b> deletions saved (+{sep(DRIVER.counter['deletions'])} new | <i>{deletions_per_s:.2f}/s</i> | <b>{deletions_size}</b>)" +
						f"\n<code> → </code> <b>{members_count}</b> members updated (+{sep(DRIVER.counter['members'])} new | <i>{members_per_h:.2f}/h</i> | <b>{members_size}</b>)" +
						f"\n<code> → </code> <b>{user_count}</b> users met (+{sep(DRIVER.counter['users'])} new | <i>{users_per_h:.2f}/h</i> | <b>{user_size}</b>)" +
						f"\n<code> → </code> <b>{chat_count}</b> chats visited (+{sep(DRIVER.counter['chats'])} new | <i>{chats_per_h:.2f}/h</i> | <b>{chat_size}</b>)" +
						f"\n<code> → </code> DB total size <b>{db_size}</b>" +
						f"\n<code> → </code> <b>{medianumber}</b> documents archived (size <b>{mediasize}</b>)",
						parse_mode="html", disable_web_page_preview=True
		)

BACKFILL_STOP = False

@report_error(logger)
@set_offline
async def back_fill_messages(client, message, target_group, limit, offset, interval, silent=False):
	global BACKFILL_STOP
	count = 0
	if not silent:
		await edit_or_reply(message, f"<code> → </code> [ <b>0 / {sep(limit)}</b> ]", parse_mode="html")
	async for msg in client.iter_history(target_group.id, limit=limit, offset=offset):
		if BACKFILL_STOP:
			BACKFILL_STOP = False
			return await edit_or_reply(message, f"<code>[!] → </code> Stopped at [ <b>{sep(count)} / {sep(limit)}</b> ]", parse_mode="html")
		if msg.service:
			await DRIVER.parse_service_event(msg)
		else:
			await DRIVER.parse_message_event(msg)
		count += 1
		if not silent and count % interval == 0:
			await edit_or_reply(message, f"<code> → </code> [ <b>{sep(count)} / {sep(limit)}</b> ]", parse_mode="html")
	await edit_or_reply(message, f"<code> → </code> Done [ <b>{sep(count)} / {sep(limit)}</b> ]", parse_mode="html")


@HELP.add(cmd="<amount>")
@alemiBot.on_message(is_superuser & filterCommand(["backfill"], list(alemiBot.prefixes), options={
	"group" : ["-g", "--group"],
	"interval" : ["-i", "--interval"],
	"offset" : ["-o", "--offset"],
}, flags=["-silent", "-stop"]))
@report_error(logger)
@set_offline
async def back_fill_cmd(client, message):
	"""iter previous history to fill database

	Call telegram iter_history to put in db messages sent before joining.
	Specify a group to backfill with `-g`.
	Specify an offset to start backfilling from on with `-o`.
	Specify an interval to update on with `-i`.
	Add flag `-silent` to not show progress.
	Use flag `-stop` to interrupt an ongoing backfill.
	"""
	global BACKFILL_STOP
	if message.command["-stop"]:
		BACKFILL_STOP = True
		return await edit_or_reply(message, "` → ` Stopping")
	if len(message.command) < 1:
		return await edit_or_reply(message, "`[!] → ` No input")
	limit = int(message.command[0])
	offset = int(message.command["offset"] or 0)
	target_group = message.chat
	interval = int(message.command["interval"] or 500)
	silent = bool(message.command["-silent"])
	msg = message
	if "group" in message.command:
		target_group = await client.get_chat(int(message.command["group"])
			if message.command["group"].isnumeric() else message.command["group"])
	if not silent and not is_me(message):
		msg = await edit_or_reply(message, f"<code>$</code>backfill {message.command.text}", parse_mode="html") # ugly but will do ehhh
	asyncio.create_task(
		back_fill_messages(
			client, msg, target_group, limit, offset,
			interval, silent=silent
		)
	)

# Handy ugly util to get chat off database
async def safe_get_chat(client, chat):
	try:
		doc = await DRIVER.db.chats.find_one({"id":chat})
		if not doc:
			return f"<s>{chat}</s>"
		return get_doc_username(doc)
	except Exception as e:
		logger.exception("Failed to search channel '%s'", str(chat))
		return f"<s>{chat}</s>"

@HELP.add(cmd="<regex>")
@alemiBot.on_message(is_superuser & filterCommand(["source"], list(alemiBot.prefixes), options={
	"min" : ["-m", "--min"],
}))
@report_error(logger)
@set_offline
@cancel_chat_action
async def source_cmd(client, message):
	"""find chats where certain regex is used

	Will search all chats which contain at least 10 occurrence of given regex, and show \
	message count matching given regex.
	The minimum occurrances can be changed with `-m` option.
	"""
	if len(message.command) < 1:
		return await edit_or_reply(message, "<code>[!] → </code> No input", parse_mode="html")
	minmsgs = int(message.command["min"] or 10)
	msg = await edit_or_reply(message, f"<code>→ </code> Chats mentioning <code>{message.command[0]}</code> (<i>>= {minmsgs} times</i>)", parse_mode="html", disable_web_page_preview=True)
	results = []
	with ProgressChatAction(client, message.chat.id, action="playing") as prog:
		for chat in await DRIVER.db.messages.distinct("chat", {"text": {"$regex": message.command[0]}}):
			count = await DRIVER.db.messages.count_documents({"chat":chat,"text":{"$regex":message.command[0]}})
			if count >= minmsgs:
				results.append((await safe_get_chat(client, chat), count))
	if len(results) < 1:
		return await edit_or_reply(msg, "<code>[!] → </code> No results", parse_mode="html")
	results.sort(key= lambda x: x[1], reverse=True)
	out = ""
	for res in results:
		out += f"<code> → </code> [<b>{sep(res[1])}</b>] {res[0]}\n"
	await edit_or_reply(msg, out, parse_mode="html", disable_web_page_preview=True)


@HELP.add(cmd="<{query}>")
@alemiBot.on_message(is_superuser & filterCommand(["query", "q", "log"], list(alemiBot.prefixes), options={
	"limit" : ["-l", "-limit"],
	"filter" : ["-f", "-filter"],
	"collection" : ["-coll", "-collection"],
	"database" : ["-db", "-database"]
}, flags=["-cmd", "-count", "-id", "-asc"]))
@report_error(logger)
@set_offline
@cancel_chat_action
async def query_cmd(client, message):
	"""interact with db

	Make queries to the underlying database (MongoDB) to request documents.
	You can just get the number or matches with `-count` flag.
	The query and the filter must be a valid JSON dictionaries without spaces; if you need spaces, wrap them in `'`.
	You can specify a limit for results (`-l`), if not given, will default to 10.
	Add `-id` flag to include `_id` field.
	If multiple userbots are logging in the same database (but in different collections), you can specify in which \
	collection to query with `-coll`. You can also specify which database to use with `-db` option, but the user \
	which the bot is using to login will need permissions to read.
	Query will sort by date, descending. Add flag `-asc` to sort ascending.
	"""
	if len(message.command) < 1:
		return await edit_or_reply(message, "`[!] → ` No input")
	buf = []
	cursor = None
	lim = int(message.command["limit"] or 10)

	database = DRIVER.db
	if "database" in message.command:
		database = DRIVER.client[message.command["database"]]
	collection = database.messages
	if "collection" in message.command:
		collection = database[message.command["collection"]]

	q = json.loads(message.command[0])
	flt = json.loads(message.command["filter"] or '{}')
	prog = ProgressChatAction(client, message.chat.id)
	if not message.command["-id"]:
		flt["_id"] = False

	if message.command["-cmd"]:
		buf = [ await database.command(*message.command.args) ]
	elif message.command["-count"]:
		buf = [ await collection.count_documents(q) ]
	else:
		cursor = collection.find(q, flt).sort("date", ASCENDING if message.command["-asc"] else DESCENDING)
		async for doc in cursor.limit(lim):
			await prog.tick()
			buf.append(doc)

	raw = json.dumps(buf, indent=2, default=str, ensure_ascii=False)
	if len(message.text.markdown) + len(tokenize_json(raw)) > 4090:
		f = io.BytesIO(raw.encode("utf-8"))
		f.name = "query.json"
		await client.send_document(message.chat.id, f, reply_to_message_id=message.message_id,
								caption=f"` → Query result `", progress=prog.tick)
	else:
		await edit_or_reply(message, "` → `" + tokenize_json(raw))

@HELP.add(cmd="<user>")
@alemiBot.on_message(is_superuser & filterCommand(["groups"], list(alemiBot.prefixes)))
@report_error(logger)
@cancel_chat_action
@set_offline
async def groups_cmd(client, message):
	"""get all groups a user has been sighted in

	Will scan database for member updates, service messages and messages sent by specified member.
	Any group which received a message or a member update, will be listed.
	For each group listed, regular messages sent in there from user will be counted.
	"""
	if len(message.command) < 1:
		return await edit_or_reply(message, "`[!] → ` No input")
	uid = message.command[0]
	user = await client.get_users(int(uid) if uid.isnumeric() else uid)
	msg = await edit_or_reply(message, "<code>→ </code> Checking sightings\n", parse_mode="html", disable_web_page_preview="True")
	with ProgressChatAction(client, message.chat.id, "find_location") as prog:
		member_groups, service_groups, message_groups = await asyncio.gather(
			DRIVER.db.members.distinct("chat", {"user": user.id}),
			DRIVER.db.service.distinct("chat", {"user": user.id}),
			DRIVER.db.messages.distinct("chat", {"user": user.id})
		)
		member_groups = set(member_groups)
		service_groups = set(service_groups)
		message_groups = set(message_groups)

		group_ids = member_groups.union(service_groups, message_groups)
		groups = [ doc async for doc in DRIVER.db.chats.find({"id": {"$in": list(group_ids)}}) ]
		unkns = group_ids - set(doc["id"] for doc in groups)

		output = f"<code> → </code> of {get_username(user)}"
		for group in groups:
			count = await DRIVER.db.messages.count_documents({"chat": group.id, "user": user.id})
			output += f"\n<code>  → </code> {get_doc_username(group)} [<b>{count}</b>]"

		for unk in unkns:
			count = await DRIVER.db.messages.count_documents({"chat": unk, "user": user.id})
			try:
				where = get_username(await client.get_chat(unk))
			except PeerIdInvalid:
				where = f"<s>{unk}</s>"
			output += f"\n<code>  → </code> {where} [<b>{count}</b>]"
	await edit_or_reply(msg, output, parse_mode="html", disable_web_page_preview=True)


@HELP.add(cmd="[<id>]", sudo=False)
@alemiBot.on_message(is_allowed & filterCommand(["history", "hist"], list(alemiBot.prefixes), options={
	"group" : ["-g"]
}, flags=["-t", "-a"]))
@report_error(logger)
@set_offline
async def hist_cmd(client, message):
	"""get edit history of a message

	Request edit history of a message. You can specify a message id or reply to a message.
	By giving the `-t` flag, edit timestamps will be shown.
	Superuser can check history of messages in different groups by giving the group id (or name) in the `-g` option.
	"""
	m_id = None
	c_id = message.chat.id
	show_time = message.command["-t"]
	show_author = message.command["-a"]
	if message.reply_to_message is not None:
		m_id = message.reply_to_message.message_id
	elif len(message.command) > 0:
		m_id = int(message.command[0])
	if m_id is None:
		return
	if "group" in message.command and check_superuser(message):
		if message.command["group"].isnumeric():
			c_id = int(message.command["group"])
		else:
			c_id = (await client.get_chat(message.command["group"])).id
	LINE = "` → ` {date} {author} {text}\n"
	doc = await DRIVER.db.messages.find_one({"id": m_id, "chat": c_id}, sort=[("date", DESCENDING)])
	if doc:
		author = get_username(await client.get_users(doc['user']))
		out = LINE.format(
			date=f"[--{doc['date']}--]" if show_time else "",
			author=f"**{author}** >" if show_author else "",
			text=html.escape(doc["text"] if "text" in doc else ""),
		)
		if "edits" in doc:
			for edit in doc["edits"]:
				out += LINE.format(
					date=f"[--{edit['date']}--]" if show_time else "",
					text=edit["text"],
					author="",
				)
		await edit_or_reply(message, out)
	else:
		await edit_or_reply(message, "`[!] → ` Nothing found")

@HELP.add(cmd="[<n>]", sudo=False)
@alemiBot.on_message(is_allowed & filterCommand(["peek", "deld", "deleted", "removed"], list(alemiBot.prefixes), options={
	"group" : ["-g", "-group"],
	"offset" : ["-o", "-offset"],
}, flags=["-time", "-id", "-all", "-down", "-bots"]))
@report_error(logger)
@set_offline
async def deleted_cmd(client, message): # This is a mess omg
	"""get deleted messages

	Request last deleted messages in this chat.
	Add `-time` to show timestamps.
	Add `-id` to show message ids.
	A number of messages to peek can be specified.
	Bots don't receive deletion events, so you must reply to a message to peek messages sent just before (or after with `-down`).
	If any media was attached to the message and downloaded, a local path will be provided.
	An offset can be specified with `-o` : if given, the most `<offset>` recent messages will be skipped and older messages will be peeked.
	Superuser can peek globally (`-all`) or in a specific group (`-g <id>`).
	Keep in mind that Telegram doesn't send easy to use deletion data, so the bot needs to lookup ids and messages in the database when he receives a deletion.
	Messages from bots will be filtered out, add `-bots` flag to keep them.
	Telegram doesn't even always include the chat id, so false positives may happen.
	For specific searches, use the query (`.q`) command.
	"""
	show_time = message.command["-time"]
	show_id = message.command["-id"]
	msg_after = message.command["-down"]
	all_groups = message.command["-all"]
	target_group = message.chat
	offset = int(message.command["offset"] or 0)
	limit = int(message.command[0] or 1)
	if client.me.is_bot and not message.reply_to_message:
		return await edit_or_reply(message, "`[!] → ` You need to reply to a message")
	if check_superuser(message):
		if all_groups:
			target_group = None
		elif "group" in message.command:
			if message.command["group"].isnumeric():
				target_group = await client.get_chat(int(message.command["group"]))
			else:
				target_group = await client.get_chat(message.command["group"])
	if client.me.is_bot:
		limit = min(limit, 5)
	count = 0
	flt = {}
	if client.me.is_bot: # bots don't receive delete events so peek must work slightly differently
		msg = await DRIVER.db.messages.find_one({"id":message.reply_to_message.message_id, "chat":target_group.id}, sort=[("date",DESCENDING)])
		if not msg:
			return await edit_or_reply(message, "`[!] → ` No record of requested message")
		if msg_after:
			flt["date"] = {"$gt":msg["date"]}
		else:
			flt["date"] = {"$lt":msg["date"]}
	else:
		flt["deleted"] = {"$exists":1}
	if target_group:
		flt["chat"] = target_group.id

	pre_text = f"<code>→ </code> Peeking <b>{limit}</b> message{'s' if limit > 1 else ''} " + \
			("down " if msg_after else "") + \
			(f"in <b>{get_username(target_group)}</b> " if "group" in message.command else '') + \
			(f"from <a href=\"{message.reply_to_message.link}\">here</a> " if client.me.is_bot else "") + "\n"
	msg = await edit_or_reply(message, pre_text, parse_mode="html", disable_web_page_preview=True)
	LINE = "<code> → </code> {time}{m_id}<b>{user}</b> {where} {media} <code>|</code> {text}\n"
	cursor = DRIVER.db.messages.find(flt).sort("date", ASCENDING if msg_after else DESCENDING)
	chat_cache = {}
	out = ""
	with ProgressChatAction(client, message.chat.id) as prog:
		async for doc in cursor:
			if offset > 0:
				offset -=1
				continue
			author = f"<s>{doc['user']}</s>"
			try: # TODO completely rely on database!
				usr = await (client.get_chat(doc["user"]) if doc["user"] < 0 else client.get_users(doc["user"]))
				if not message.command["-bots"] and doc["user"] > 0 and usr.is_bot:
					continue
				author = get_username(usr)
			except PeerIdInvalid:
				pass # ignore, sometimes we can't lookup users
			if doc["chat"] not in chat_cache: # cache since this causes floodwaits!
				chat_cache[doc["chat"]] = await client.get_chat(doc["chat"])
			out += LINE.format(
				time=f"[<code>{doc['date']}</code>] " if show_time else "",
				m_id=f"[<code>{doc['id']}</code>] " if show_id else "",
				user=author,
				where=f"(<i>{get_username(chat_cache[doc['chat']])}</i>)" if all_groups else "",
				media=f"[<code>{doc['media']}</code>]" if "media" in doc else "",
				text=html.escape(doc["text"] if "text" in doc else ""),
			)
			count += 1
			if count >= limit:
				break
	if not out:
		out += "<code>[!] → </code> Nothing to display"
	await edit_or_reply(msg, out, parse_mode="html", disable_web_page_preview=True)
