import io
import os
import json
import asyncio

from datetime import datetime
from pymongo import ASCENDING, DESCENDING

from bot import alemiBot

from util.command import filterCommand
from util.permission import is_allowed, is_superuser, check_superuser
from util.getters import get_username, get_channel
from util.message import ProgressChatAction, edit_or_reply, is_me
from util.text import tokenize_json, order_suffix, sep
from util.decorators import report_error, set_offline
from util.help import HelpCategory

from plugins.statsbot.driver import DRIVER

import logging
logger = logging.getLogger(__name__)

HELP = HelpCategory("BIGBROTHER")

BACKFILL_STOP = False

@report_error(logger)
@set_offline
async def back_fill_messages(client, message, target_group, limit, offset, interval, silent=False):
	global BACKFILL_STOP
	count = 0
	if not silent:
		await edit_or_reply(message, f"` → ` [ **0 / {sep(limit)}** ]")
	async for msg in client.iter_history(target_group.id, limit=limit, offset=offset):
		if BACKFILL_STOP:
			BACKFILL_STOP = False
			return await edit_or_reply(message, f"`[!] → ` Stopped at [ **{sep(count)} / {sep(limit)}** ]")
		if msg.service:
			DRIVER.parse_service_event(msg, ignore_duplicates=True)
		else:
			DRIVER.parse_message_event(msg, ignore_duplicates=True)
		count += 1
		if not silent and count % interval == 0:
			await edit_or_reply(message, f"` → ` [ **{sep(count)} / {sep(limit)}** ]")
	await edit_or_reply(message, "` → ` Done [ **{sep(count)} / {sep(limit)}** ]")


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
		msg = await edit_or_reply(message, f"$ backfill {message.command.text}") # ugly but will do ehhh
	asyncio.create_task(
		back_fill_messages(
			client, msg, target_group, limit, offset,
			interval, silent=silent
		)
	)

@HELP.add()
@alemiBot.on_message(is_superuser & filterCommand(["dbstats", "dbstat"], list(alemiBot.prefixes)))
@report_error(logger)
@set_offline
async def dbstats_cmd(client, message):
	"""get database stats

	List collections, entries, new entries in this session and disk usage.
	"""
	prog = ProgressChatAction(client, message.chat.id)
	await prog.tick()
	oldest_msg = DRIVER.db.messages.find_one({"date":{"$ne":None}}, sort=[("date", ASCENDING)])
	await prog.tick()
	msg_count = sep(DRIVER.db.messages.count({}))
	await prog.tick()
	user_count = sep(DRIVER.db.users.count({}))
	await prog.tick()
	chat_count = sep(DRIVER.db.chats.count({}))
	await prog.tick()
	deletions_count = sep(DRIVER.db.deletions.count({}))
	await prog.tick()
	service_count = sep(DRIVER.db.service.count({}))
	await prog.tick()
	msg_size = order_suffix(DRIVER.db.command("collstats", "messages")['totalSize'])
	await prog.tick()
	user_size = order_suffix(DRIVER.db.command("collstats", "users")['totalSize'])
	await prog.tick()
	chat_size = order_suffix(DRIVER.db.command("collstats", "chats")['totalSize'])
	await prog.tick()
	deletions_size = order_suffix(DRIVER.db.command("collstats", "deletions")['totalSize'])
	await prog.tick()
	service_size = order_suffix(DRIVER.db.command("collstats", "service")['totalSize'])
	await prog.tick()
	db_size = order_suffix(DRIVER.db.command("dbstats")["totalSize"])
	await prog.tick()
	medianumber = sep(len(os.listdir("data/scraped_media")))
	proc = await asyncio.create_subprocess_exec( # This is not cross platform!
		"du", "-b", "data/scraped_media",
		stdout=asyncio.subprocess.PIPE,
		stderr=asyncio.subprocess.STDOUT)
	stdout, _stderr = await proc.communicate()
	mediasize = order_suffix(float(stdout.decode('utf-8').split("\t")[0]))

	uptime = str(datetime.now() - client.start_time)
	await edit_or_reply(message, f"`→ ` **online for** `{uptime}`" +
					f"\n`→ ` **first event** `{oldest_msg['date']}`" +
					f"\n` → ` **{msg_count}** msgs logged (+{sep(DRIVER.counter['messages'])} new | **{msg_size}**)" +
					f"\n` → ` **{service_count}** events tracked (+{sep(DRIVER.counter['service'])} new | **{service_size}**)" +
					f"\n` → ` **{deletions_count}** deletions saved (+{sep(DRIVER.counter['deletions'])} new | **{deletions_size}**)" +
					f"\n` → ` **{user_count}** users met (+{sep(DRIVER.counter['users'])} new | **{user_size}**)" +
					f"\n` → ` **{chat_count}** chats visited (+{sep(DRIVER.counter['chats'])} new | **{chat_size}**)" +
					f"\n` → ` DB total size **{db_size}**" +
					f"\n` → ` **{medianumber}** documents archived (size **{mediasize}**)")

@HELP.add(cmd="<{query}>")
@alemiBot.on_message(is_superuser & filterCommand(["query", "q", "log"], list(alemiBot.prefixes), options={
	"limit" : ["-l", "-limit"],
	"filter" : ["-f", "-filter"],
	"collection" : ["-coll", "-collection"],
	"database" : ["-db", "-database"]
}, flags=["-cmd", "-count", "-id"]))
@report_error(logger)
@set_offline
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

	if message.command["-cmd"]:
		cursor = [ database.command(*message.command.args) ] # ewww but small patch
	else:
		q = json.loads(message.command[0])
		flt = json.loads(message.command["filter"] or '{}')
		if not message.command["-id"]:
			flt["_id"] = False
		cursor = collection.find(q, flt).sort("date", -1)


	if message.command["-count"]:
		buf = [ cursor.count() ]
	else:
		prog = ProgressChatAction(client, message.chat.id)
		for doc in cursor.limit(lim):
			await prog.tick()
			buf.append(doc)

	raw = json.dumps(buf, indent=2, default=str, ensure_ascii=False)
	if len(message.text.markdown) + len(tokenize_json(raw)) > 4090:
		f = io.BytesIO(raw.encode("utf-8"))
		f.name = "query.json"
		await client.send_document(message.chat.id, f, reply_to_message_id=message.message_id,
								caption=f"` → Query result `")
	else:
		await edit_or_reply(message, "` → `" + tokenize_json(raw))

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
	doc = DRIVER.db.messages.find_one({"id": m_id, "chat": c_id}, sort=[("date", DESCENDING)])
	if doc:
		out = LINE.format(
			date=f"[--{doc['date']}--]" if show_time else "",
			author=f"**{get_username(await client.get_users(doc['user']))}** >" if show_author else "",
			text=doc["text"],
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
}, flags=["-t", "-all", "-down"]))
@report_error(logger)
@set_offline
async def deleted_cmd(client, message): # This is a mess omg
	"""get deleted messages

	Request last deleted messages in this chat.
	Use `-t` to add timestamps.
	A number of messages to peek can be specified.
	Bots don't receive deletion events, so you must reply to a message to peek messages sent just before (or after with `-down`).
	If any media was attached to the message and downloaded, a local path will be provided.
	An offset can be specified with `-o` : if given, the most `<offset>` recent messages will be skipped and older messages will be peeked.
	Superuser can peek globally (`-all`) or in a specific group (`-g <id>`).
	Keep in mind that Telegram doesn't send easy to use deletion data, so the bot needs to lookup ids and messages in the database when he receives a deletion.
	Telegram doesn't even always include the chat id, so false positives may happen.
	For specific searches, use the query (`.q`) command.
	"""
	show_time = message.command["-t"]
	msg_after = message.command["-down"]
	all_groups = message.command["-all"]
	target_group = message.chat
	offset = int(message.command["offset"] or 0)
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
	limit = 1
	if len(message.command) > 1:
		limit = int(message.command[0])
		if client.me.is_bot:
			limit = min(limit, 5)
	count = 0
	if client.me.is_bot: # bots don't receive delete events so peek must work slightly differently
		msg = DRIVER.db.messages.find_one({"id":message.reply_to_message.message_id, "chat":target_group.id})
		if not msg:
			return await edit_or_reply(message, "`[!] → ` No record of requested message")
		if msg_after:
			flt = {"date": {"$gt":msg["date"]}}
		else:
			flt = {"date": {"$lt":msg["date"]}}
	else:
		flt = {"deleted": True}
	if target_group:
		flt["chat"] = target_group.id

	prog = ProgressChatAction(client, message.chat.id)
	out = f"`→ ` Peeking `{limit}` message{'s' if limit > 1 else ''} " + \
			("down " if msg_after else "") + \
			(f"in **{get_channel(target_group)}** " if "group" in message.command else '') + \
			(f"from [here]({message.reply_to_message.link}) " if client.me.is_bot else "") + "\n"
	LINE = "{time}[`{m_id}`] **{user}** {where} → {text} {media}\n"
	cursor = DRIVER.db.messages.find(flt).sort("date", ASCENDING if msg_after else DESCENDING)
	for doc in cursor:
		await prog.tick()
		if offset > 0:
			offset -=1
			continue
		author = f"~~{doc['user']}~~"
		if str(doc["user"]).startswith("-100"):
			usr = await client.get_chat(doc["user"])
			if usr and usr.username:
				author = usr.username
		else:
			usr = await client.get_users(doc["user"])
			if usr:
				author = get_username(usr) # if mention=True sometimes it fails?
		group = await client.get_chat(doc["chat"])
		out += LINE.format(
			time=str(doc["date"]) + " " if show_time else "",
			m_id=doc["id"],
			user=author,
			where=f"(__{get_channel(group)}__)" if all_groups else "",
			text=doc["text"] if "text" in doc else "",
			media=f"<~~{doc['media']}~~>" if "media" in doc else "",
		)
		count += 1
		if count >= limit:
			break
	if count == 0:
		out += "`[!] → ` Nothing to display"
	await edit_or_reply(message, out)
