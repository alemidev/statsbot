import asyncio
import json
import io
import os

from datetime import datetime

from pymongo import DESCENDING

from bot import alemiBot

from util.command import filterCommand
from util.text import tokenize_json, order_suffix
from util.permission import is_allowed, is_superuser
from util.message import edit_or_reply
from util.decorators import report_error, set_offline
from util.help import HelpCategory

from ..driver import DRIVER

import logging
logger = logging.getLogger(__name__)

HELP = HelpCategory("DATABASE")

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
	deletions_count = DRIVER.db.deletions.count({})
	service_count = DRIVER.db.service.count({})
	msg_size = DRIVER.db.command("collstats", "messages")['totalSize']
	user_size = DRIVER.db.command("collstats", "users")['totalSize']
	chat_size = DRIVER.db.command("collstats", "chats")['totalSize']
	deletions_size = DRIVER.db.command("collstats", "deletions")['totalSize']
	service_size = DRIVER.db.command("collstats", "service")['totalSize']
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
					f"\n` → ` **{DRIVER.counter['messages']}** messages logged (+{DRIVER.counter['edits']} edits)" +
					f"\n`  → ` **{msg_count}** total `|` size **{order_suffix(msg_size)}**" +
					f"\n` → ` **{DRIVER.counter['service']}** events tracked" +
					f"\n`  → ` **{service_count}** total `|` size **{order_suffix(service_size)}**" +
					f"\n` → ` **{DRIVER.counter['deletions']}** deletions saved" +
					f"\n`  → ` **{deletions_count}** total `|` size **{order_suffix(deletions_size)}**" +
					f"\n` → ` **{DRIVER.counter['users']}** users seen" +
					f"\n`  → ` **{user_count}** total `|` size **{order_suffix(user_size)}**" +
					f"\n` → ` **{DRIVER.counter['chats']}** chats visited" +
					f"\n`  → ` **{chat_count}** total `|` size **{order_suffix(chat_size)}**" +
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