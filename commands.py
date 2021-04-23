import asyncio
import time
import json
import logging
import io
import os

from datetime import datetime

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
	elif "arg" in args:
		m_id = int(args["arg"])
	if m_id is None:
		return
	if "group" in args:
		if args["group"].isnumeric():
			c_id = int(args["group"])
		else:
			c_id = (await client.get_chat(args["group"])).id
	await client.send_chat_action(message.chat.id, "upload_document")
	cursor = DRIVER.db.messages.find( {"_": "Message", "message_id": m_id, "chat.id": c_id},
			{"text": 1, "date": 1, "edit_date": 1} ).sort("date", -1)
	logger.info("Querying db for message history")
	out = ""
	for doc in cursor:
		if show_time:
			if "edit_date" not in doc or doc['edit_date'] is None:
				out += f"[{str(doc['date'])}] "
			else:
				out += f"[{str(doc['edit_date'])}] "
		if "text" in doc:
			out += f"` → ` {doc['text']['markdown']}\n"
		else:
			out += f"` → ` N/A\n"
	await edit_or_reply(message, out)
	await client.send_chat_action(message.chat.id, "cancel")


async def lookup_deleted_messages(client, message, COLLECTION, target_group, limit, show_time=False, include_system=False, offset=0):
	response = await edit_or_reply(message, f"` → Peeking {limit} message{'s' if limit > 1 else ''} " +
											('in ' + get_channel(target_group) if target_group is not None else '') + "`")
	chat_id = target_group.id if target_group is not None else None
	out = "\n\n"
	count = 0
	LINE = "{time}`[{m_id}]` **{user}** {where} → {system}{text} {media}\n"
	try:
		logger.debug("Querying db for deletions")
		keep_active = time.time()
		await client.send_chat_action(message.chat.id, "playing")
		cursor = COLLECTION.find({ "_": "Delete" }).sort("date", -1)
		for deletion in cursor: # TODO make this part not a fucking mess!
			if chat_id is not None and "chat" in deletion \
			and deletion["chat"]["id"] != chat_id:
				continue # don't make a 2nd query, should speed up a ton
			candidates = COLLECTION.find({"_": "Message", "message_id": deletion["message_id"]}).sort("date", -1)
			logger.debug("Querying db for possible deleted msg")
			if time.time() - keep_active > 5:
				await client.send_chat_action(message.chat.id, "playing")
				keep_active = time.time()
			for doc in candidates: # dank 'for': i only need one
				if chat_id is not None and ("chat" not in doc or doc["chat"]["id"] != chat_id):
					continue
				if not include_system and "service" in doc and doc["service"]:
					break # we don't care about service messages!
				if not include_system and "from_user" in doc and doc["from_user"]["is_bot"]:
					break # we don't care about bot messages!
				if offset > 0: # We found a message but we don't want it because an offset was set
					offset -=1 #   skip adding this to output
					break
				if limit == 1 and "attached_file" in doc: # Doing this here forces me to do ugly stuff below, eww!
					await client.send_document(message.chat.id, "data/scraped_media/"+doc["attached_file"], reply_to_message_id=message.message_id,
										caption="**" + (get_username_dict(doc['from_user']) if "from_user" in doc else "UNKNOWN") + "** `→" +
												(get_channel_dict(doc['chat']) + ' → ' if chat_id is None else '') +
												f"` {get_text_dict(doc)['raw']}")
				else:
					out += LINE.format(time=(str(doc["date"]) + " ") if show_time else "",
									m_id=doc["message_id"], user=(get_username_dict(doc["from_user"]) if "from_user" in doc else "UNKNOWN"),
									where='' if chat_id is not None else ("| --" + get_channel_dict(doc["chat"]) + '-- '),
									system=("--" + parse_sys_dict(doc) + "-- " if "service" in doc and doc["service"] else ""),
									text=get_text_dict(doc)['raw'], media=('' if "attached_file" not in doc else ('(`' + doc["attached_file"] + '`)')))
				count += 1
				break
			if count >= limit:
				break
		await client.send_chat_action(message.chat.id, "upload_document")
		if count > 0:
			if len(out) > 4096:
				for m in batchify(out, 4090):
					await response.reply(m)
			elif out.strip() != "": # This is bad!
				await response.edit(response.text + out)
		else:
			await response.edit(response.text + "**N/A**")
	except Exception as e:
		logger.exception("Issue while peeking into database")
		await response.edit(response.text + "\n`[!] → ` " + str(e))
	await client.send_chat_action(message.chat.id, "cancel")
	await client.set_offline() 

HELP.add_help(["peek", "deld", "deleted", "removed"], "get deleted messages",
				"request last deleted messages in this channel. Use `-t` to add timestamps. A number of messages to peek can be specified. " +
				"If only one message is being peeked, any media attached will be included, otherwise the filename will be appended to message text. " +
				"Service messages and bot messages will be excluded from peek by default, add `-sys` flag to include them. "
				"Owner can peek globally (`-all`) or in a specific group (`-g <id>`). Keep in mind that Telegram doesn't send easy to use " +
				"deletion data, so the bot needs to lookup ids and messages in the database, making cross searches. Big peeks, or peeks of very old deletions " +
				"will take some time to complete. For specific searches, use the query (`.q`) command. An offset can be specified with `-o` : if given, " +
				"the most `<offset>` recent messages will be skipped and older messages will be peeked. If multiple userbots are logging into the same database " +
				"(but different collections), you can specify on which collection to peek with `-coll`.",
				public=True, args="[-t] [-g [id] | -all] [-sys] [-o <n>] [<num>]")
@alemiBot.on_message(is_allowed & filterCommand(["peek", "deld", "deleted", "removed"], list(alemiBot.prefixes), options={
	"group" : ["-g", "-group"],
	"offset" : ["-o", "-offset"],
	"collection" : ["-coll", "-collection"]
}, flags=["-t", "-all", "-sys"]))
@report_error(logger)
@set_offline
async def deleted_cmd(client, message): # This is a mess omg
	args = message.command
	show_time = "-t" in args["flags"]
	target_group = message.chat
	include_system = "-sys" in args["flags"]
	offset = int(args["offset"]) if "offset" in args else 0
	coll = DRIVER.db.messages
	if is_me(message):
		if "-all" in args["flags"]:
			target_group = None
		elif "group" in args:
			if args["group"].isnumeric():
				target_group = await client.get_chat(int(args["group"]))
			else:
				target_group = await client.get_chat(args["group"])
		if "collection" in args:
			coll = DRIVER.db[args["collection"]]
	limit = 1
	if "arg" in args:
		limit = int(args["arg"])
	logger.info(f"Peeking {limit} messages")
	asyncio.get_event_loop().create_task( # launch the task async because it may take some time
		lookup_deleted_messages(
			client, message, coll,
			target_group, limit, show_time, include_system, offset
		)
	)

