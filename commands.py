import asyncio
import time
import json
import logging
import io
import os

from datetime import datetime

from pymongo import DESCENDING

from bot import alemiBot

from util import batchify
from util.command import filterCommand
from util.text import tokenize_json, order_suffix
from util.permission import is_allowed, is_superuser
from util.getters import get_text, get_text_dict, get_username, get_username_dict, get_channel, get_channel_dict
from util.message import edit_or_reply, is_me, parse_sys_dict
from util.decorators import report_error, set_offline
from util.help import HelpCategory

from .driver import DRIVER

import logging
logger = logging.getLogger(__name__)

HELP = HelpCategory("STATISTICS")

HELP.add_help(["stats", "stat"], "get stats",
				"Get uptime, disk usage for media and for db, number of tracked events.", public=True)
@alemiBot.on_message(is_allowed & filterCommand(["stats", "stat"], list(alemiBot.prefixes)))
@report_error(logger)
@set_offline
async def stats_cmd(client, message):
	logger.info("Getting stats")
	original_text = message.text.markdown
	msg = await edit_or_reply(message, "` → ` Fetching stats...")
	msg_count = DRIVER.db.messages.count({})
	user_count = DRIVER.db.users.count({})
	chat_count = DRIVER.db.chats.count({})
	msg_size = DRIVER.db.command("collstats", "messages")['totalSize']
	user_size = DRIVER.db.command("collstats", "users")['totalSize']
	chat_size = DRIVER.db.command("collstats", "chats")['totalSize']
	db_size = DRIVER.db.command("dbstats")["totalSize"]
	medianumber = len(os.listdir("data/scraped_media"))
	proc = await asyncio.create_subprocess_exec( # This is not cross platform!
		"du", "-b", "data/scraped_media",
		stdout=asyncio.subprocess.PIPE,
		stderr=asyncio.subprocess.STDOUT)
	stdout, _stderr = await proc.communicate()
	mediasize = float(stdout.decode('utf-8').split("\t")[0])

	uptime = str(datetime.now() - client.start_time)
	await msg.edit(original_text + f"\n`→ online for {uptime} `" +
					f"\n` → ` **{DRIVER.deletions}** deletions `|` **{DRIVER.edits}** edits" +
					f"\n` → ` **{DRIVER.messages}** messages logged (**{msg_count}** total `|` **{order_suffix(msg_size)}**)" +
					f"\n` → ` **{DRIVER.users}** users seen (**{user_count}** total `|` **{order_suffix(user_size)}**)" +
					f"\n` → ` **{DRIVER.chats}** chats tracked (**{chat_count}** total `|` **{order_suffix(chat_size)}**)" +
					f"\n` → ` DB total size **{order_suffix(db_size)}**" +
					f"\n` → ` **{medianumber}** documents archived (size **{order_suffix(mediasize)}**)")
	await client.set_offline()

HELP.add_help(["query", "q", "log"], "interact with db",
				"make queries to the underlying database (MongoDB) to request documents. You can just get the number or matches with `-count` flag. " +
				"The query and the filter must be a valid JSON dictionaries without spaces; if you need spaces, wrap them in `'`. You can specify a " +
				"limit for results (`-l`), if not given, will default to 10. If multiple userbots are logging in the same " +
				"database (but in different collections), you can specify in which collection to query with `-coll`. You can also " +
				"specify which database to use with `-db` option, but the user which the bot is using to login will need permissions to read.",
				args="[-coll <name>] [-db <name>] [-l <n>] [-f <{filter}>] [-count] <{query}>")
@alemiBot.on_message(is_superuser & filterCommand(["query", "q", "log"], list(alemiBot.prefixes), options={
	"limit" : ["-l", "-limit"],
	"filter" : ["-f", "-filter"],
	"collection" : ["-coll", "-collection"],
	"database" : ["-db", "-database"]
}, flags=["-cmd", "-count"]))
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
	elif "filter" in args:
		q = json.loads(args["cmd"][0])
		filt = json.loads(args["filter"])
		cursor = collection.find(q, filt).sort("date", -1)
	else:
		cursor = collection.find(json.loads(args["cmd"][0])).sort("date", -1)
	
	if "-count" in args["flags"]:
		buf = [ cursor.count() ]
	else:
		for doc in cursor.limit(lim):
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
				public=True, args="[-t] [-g <g>] [<id>]")
@alemiBot.on_message(is_allowed & filterCommand(["history", "hist"], list(alemiBot.prefixes), options={
	"group" : ["-g"]
}, flags=["-t"]))
@report_error(logger)
@set_offline
async def hist_cmd(client, message):
	args = message.command
	m_id = None
	c_id = message.chat.id
	show_time = "-t" in args["flags"]
	if message.reply_to_message is not None:
		m_id = message.reply_to_message.message_id
	elif "cmd" in args:
		m_id = int(args["cmd"][0])
	if m_id is None:
		return
	if "group" in args:
		if args["group"].isnumeric():
			c_id = int(args["group"])
		else:
			c_id = (await client.get_chat(args["group"])).id
	format_text = lambda doc: f"\n` → ` [--{doc['date']}--] {doc['text']}" \
					if show_time else \
				  lambda doc: f"\n` → ` {edit['text']}"
	logger.info("Querying db for message history")
	doc = DRIVER.db.messages.find_one({"id": m_id, "chat": c_id},sort=[("date", DESCENDING)])
	if doc:
		out = f"`→ ` **{get_username(message.from_user)}** {doc['text']}"
		for edit in doc["edits"]:
			out += format_text(doc)
		await edit_or_reply(message, out)
	else:
		await edit_or_reply(message, "`[!] → ` Nothing found")

HELP.add_help(["peek", "deld", "deleted", "removed"], "get deleted messages",
				"request last deleted messages in this channel. Use `-t` to add timestamps. A number of messages to peek can be specified. " +
				"If only one message is being peeked, any media attached will be included, otherwise the filename will be appended to message text. " +
				"Owner can peek globally (`-all`) or in a specific group (`-g <id>`). Keep in mind that Telegram doesn't send easy to use " +
				"deletion data, so the bot needs to lookup ids and messages in the database when he receives a deletion. Telegram doesn't even always include " +
				"the chat id, so false positives may happen. For specific searches, use the query (`.q`) command. An offset can be " +
				"specified with `-o` : if given, the most `<offset>` recent messages will be skipped and older messages will be peeked.",
				public=True, args="[-t] [-g [id] | -all] [-o <n>] [<num>]")
@alemiBot.on_message(is_allowed & filterCommand(["peek", "deld", "deleted", "removed"], list(alemiBot.prefixes), options={
	"group" : ["-g", "-group"],
	"offset" : ["-o", "-offset"],
}, flags=["-t", "-all", "-sys"]))
@report_error(logger)
@set_offline
async def deleted_cmd(client, message): # This is a mess omg
	args = message.command
	show_time = "-t" in args["flags"]
	target_group = message.chat
	offset = int(args["offset"]) if "offset" in args else 0
	if is_me(message):
		if "-all" in args["flags"]:
			target_group = None
		elif "group" in args:
			if args["group"].isnumeric():
				target_group = await client.get_chat(int(args["group"]))
			else:
				target_group = await client.get_chat(args["group"])
	limit = 1
	if "arg" in args:
		limit = int(args["arg"])
	logger.info(f"Peeking {limit} messages")
	count = 0
	flt = {"deleted": True}
	if target_group:
		flt["chat"] = target_group.id
	out = f"`→ Peeking {limit} message{'s' if limit > 1 else ''} " + \
			('in ' + get_channel(target_group) if target_group is not None else '') + "`\n\n"
	response = await edit_or_reply(message, out)
	LINE = "{time}`[{m_id}]` **{user}** {where} → {text} {media}\n"
	cursor = DRIVER.db.messages.find(flt)
	for doc in cursor:
		if offset > 0:
			offset -=1
			continue
		out += LINE.format(
			time=str(doc["date"]) + " " if show_time else "",
			m_id=doc["id"],
			user=get_username(await client.get_users(doc["user"])),
			where=get_channel(target_group) if target_group is None else "",
			text=doc["text"],
			media=doc["media"],
		)
		count += 1
		if count >= limit:
			break
	if count == 0:
		out += "`[!] → ` Nothing to display"
	await response.edit(out)


