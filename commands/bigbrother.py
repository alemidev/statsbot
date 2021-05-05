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
from util.message import ProgressChatAction, edit_or_reply
from util.text import tokenize_json, order_suffix
from util.decorators import report_error, set_offline
from util.help import HelpCategory

from plugins.statsbot.driver import DRIVER

import logging
logger = logging.getLogger(__name__)

HELP = HelpCategory("BIGBROTHER")

HELP.add_help(["dbstats", "dbstat"], "get database stats",
				"list collections entries and disk usage." ,public=False)
@alemiBot.on_message(is_superuser & filterCommand(["dbstats", "dbstat"], list(alemiBot.prefixes)))
@report_error(logger)
@set_offline
async def dbstats_cmd(client, message):
	logger.info("Getting stats")
	prog = ProgressChatAction(client, message.chat.id)
	await prog.tick()
	msg_count = DRIVER.db.messages.count({})
	await prog.tick()
	user_count = DRIVER.db.users.count({})
	chat_count = DRIVER.db.chats.count({})
	await prog.tick()
	deletions_count = DRIVER.db.deletions.count({})
	service_count = DRIVER.db.service.count({})
	await prog.tick()
	msg_size = DRIVER.db.command("collstats", "messages")['totalSize']
	user_size = DRIVER.db.command("collstats", "users")['totalSize']
	chat_size = DRIVER.db.command("collstats", "chats")['totalSize']
	deletions_size = DRIVER.db.command("collstats", "deletions")['totalSize']
	service_size = DRIVER.db.command("collstats", "service")['totalSize']
	db_size = DRIVER.db.command("dbstats")["totalSize"]
	await prog.tick()
	medianumber = len(os.listdir("data/scraped_media"))
	proc = await asyncio.create_subprocess_exec( # This is not cross platform!
		"du", "-b", "data/scraped_media",
		stdout=asyncio.subprocess.PIPE,
		stderr=asyncio.subprocess.STDOUT)
	stdout, _stderr = await proc.communicate()
	mediasize = float(stdout.decode('utf-8').split("\t")[0])

	uptime = str(datetime.now() - client.start_time)
	await edit_or_reply(message, f"`→ ` **online for** `{uptime}`" +
					f"\n` → ` **{msg_count}** msgs logged (+{DRIVER.counter['messages']} new | **{order_suffix(msg_size)}**)" +
					f"\n` → ` **{service_count}** events tracked (+{DRIVER.counter['service']} new | **{order_suffix(service_size)}**)" +
					f"\n` → ` **{deletions_count}** deletions saved (+{DRIVER.counter['deletions']} new | **{order_suffix(deletions_size)}**)" +
					f"\n` → ` **{user_count}** users met (+{DRIVER.counter['users']} new | **{order_suffix(user_size)}**)" +
					f"\n` → ` **{chat_count}** chats visited (+{DRIVER.counter['chats']} new | **{order_suffix(chat_size)}**)" +
					f"\n` → ` DB total size **{order_suffix(db_size)}**" +
					f"\n` → ` **{medianumber}** documents archived (size **{order_suffix(mediasize)}**)")

HELP.add_help(["query", "q", "log"], "interact with db",
				"make queries to the underlying database (MongoDB) to request documents. You can just get the number or matches with `-count` flag. " +
				"The query and the filter must be a valid JSON dictionaries without spaces; if you need spaces, wrap them in `'`. You can specify a " +
				"limit for results (`-l`), if not given, will default to 10. Add `-id` flag to include `_id` field. If multiple userbots are logging in the same " +
				"database (but in different collections), you can specify in which collection to query with `-coll`. You can also " +
				"specify which database to use with `-db` option, but the user which the bot is using to login will need permissions to read.",
				args="[-coll <name>] [-db <name>] [-l <n>] [-f <{filter}>] [-count] <{query}>")
@alemiBot.on_message(is_superuser & filterCommand(["query", "q", "log"], list(alemiBot.prefixes), options={
	"limit" : ["-l", "-limit"],
	"filter" : ["-f", "-filter"],
	"collection" : ["-coll", "-collection"],
	"database" : ["-db", "-database"]
}, flags=["-cmd", "-count", "-id"]))
@report_error(logger)
@set_offline
async def query_cmd(client, message):
	if "arg" not in message.command:
		return await edit_or_reply(message, "`[!] → ` No input")
	args = message.command
	buf = []
	cursor = None
	lim = 10
	if "limit" in args:
		lim = int(args["limit"])
	logger.info(f"Querying db : {args['arg']}")
	
	database = DRIVER.db
	if "database" in args:
		database = DRIVER.client[args["database"]]
	collection = database.messages
	if "collection" in args:
		collection = database[args["collection"]]

	if "-cmd" in args["flags"]:
		cursor = [ database.command(*args["cmd"]) ] # ewww but small patch
	else:
		q = json.loads(args["cmd"][0])
		flt = {}
		if "filter" in args:
			flt = json.loads(args["filter"])
		if "-id" not in message.command["flags"]:
			flt["_id"] = False
		cursor = collection.find(q, flt).sort("date", -1)


	if "-count" in args["flags"]:
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

HELP.add_help(["hist", "history"], "get edit history of a message",
				"request edit history of a message. You can specify a message id or reply to a message. " +
				"By giving the `-t` flag, edit timestamps will be shown. You can check history of messages in " +
				"different groups by giving the group id (or name) in the `-g` option.",
				public=True, args="[-t] [-a] [-g <g>] [<id>]")
@alemiBot.on_message(is_allowed & filterCommand(["history", "hist"], list(alemiBot.prefixes), options={
	"group" : ["-g"]
}, flags=["-t", "-a"]))
@report_error(logger)
@set_offline
async def hist_cmd(client, message):
	args = message.command
	m_id = None
	c_id = message.chat.id
	show_time = "-t" in args["flags"]
	show_author = "-a" in args["flags"]
	if message.reply_to_message is not None:
		m_id = message.reply_to_message.message_id
	elif "cmd" in args:
		m_id = int(args["cmd"][0])
	if m_id is None:
		return
	if "group" in args and check_superuser(message):
		if args["group"].isnumeric():
			c_id = int(args["group"])
		else:
			c_id = (await client.get_chat(args["group"])).id
	LINE = "` → ` {date} {author} {text}\n"
	logger.info("Querying db for message history")
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

HELP.add_help(["peek", "deld", "deleted", "removed"], "get deleted messages",
				"request last deleted messages in this channel. Use `-t` to add timestamps. A number of messages to peek can be specified. " +
				"Bots don't receive deletion events, so you must reply to a message to peek messages sent just before (or after with `-down`). " +
				"If only one message is being peeked, any media attached will be included, otherwise the filename will be appended to message text. " +
				"Owner can peek globally (`-all`) or in a specific group (`-g <id>`). Keep in mind that Telegram doesn't send easy to use " +
				"deletion data, so the bot needs to lookup ids and messages in the database when he receives a deletion. Telegram doesn't even always include " +
				"the chat id, so false positives may happen. For specific searches, use the query (`.q`) command. An offset can be " +
				"specified with `-o` : if given, the most `<offset>` recent messages will be skipped and older messages will be peeked.",
				public=True, args="[-t] [-g [id] | -all] [-o <n>] [<num>] [-down]")
@alemiBot.on_message(is_allowed & filterCommand(["peek", "deld", "deleted", "removed"], list(alemiBot.prefixes), options={
	"group" : ["-g", "-group"],
	"offset" : ["-o", "-offset"],
}, flags=["-t", "-all", "-down"]))
@report_error(logger)
@set_offline
async def deleted_cmd(client, message): # This is a mess omg
	args = message.command
	show_time = "-t" in args["flags"]
	msg_after = "-down" in args["flags"]
	target_group = message.chat
	all_groups = "-all" in args["flags"]
	offset = int(args["offset"]) if "offset" in args else 0
	if client.me.is_bot and not message.reply_to_message:
		return await edit_or_reply(message, "`[!] → ` You need to reply to a message")
	if check_superuser(message):
		if all_groups:
			target_group = None
		elif "group" in args:
			if args["group"].isnumeric():
				target_group = await client.get_chat(int(args["group"]))
			else:
				target_group = await client.get_chat(args["group"])
	limit = 1
	if "arg" in args:
		limit = int(args["arg"])
		if client.me.is_bot:
			limit = min(limit, 5)
	logger.info(f"Peeking {limit} messages")
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
			(f"in **{get_channel(target_group)}** " if "group" in args else '') + \
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