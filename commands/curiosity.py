import re
import json

from collections import Counter

from pyrogram import filters
from pymongo import DESCENDING

from bot import alemiBot

from util.permission import is_allowed, is_superuser
from util.message import edit_or_reply
from util.getters import get_text, get_username, get_channel
from util.command import filterCommand
from util.decorators import report_error, set_offline
from util.help import HelpCategory

from ..driver import DRIVER

import logging
logger = logging.getLogger(__name__)

HELP = HelpCategory("CURIOSITY")

# This doesn't really fit here, will be moved into statistics plugin once I'm done with a proper statistics plugin
HELP.add_help(["freq", "frequent"], "find frequent words in messages",
				"find most used words in last messages. If no number is given, will search only " +
				"last 100 messages. By default, 10 most frequent words are shown, but number of results " +
				"can be changed with `-r`. By default, only words of `len > 3` will be considered. " +
				"A minimum word len can be specified with `-min`. Will search in current group or any specified with `-g`. " +
				"A single user can be specified with `-u` : only messages from that user will count if provided. Change " +
				"update interval with `-i`. Extra parameters for the db query can be given with `-q`.",
				args="[-r <n>] [-min <n>] [-g <group>] [-u <user>] [n] [-i <n>]", public=True)
@alemiBot.on_message(is_allowed & filterCommand(["freq", "frequent"], list(alemiBot.prefixes), options={
	"results" : ["-r", "-res"],
	"minlen" : ["-min"],
	"group" : ["-g", "-group"],
	"user" : ["-u", "-user"],
	"interval" : ["-i", "--interval"],
	"query" : ["-q", "--query"],
}, flags=["-all"]))
@report_error(logger)
@set_offline
async def frequency_cmd(client, message):
	results = int(message.command["results"]) if "results" in message.command else 10
	number = int(message.command["cmd"][0]) if "cmd" in message.command else 100
	min_len = int(message.command["minlen"]) if "minlen" in message.command else 3
	update_interval = int(message.command["interval"]) if "interval" in message.command else 10000
	query = {"text":{"$exists":1}}
	if "query" in message.command:
		query = {**query, **json.loads(message.command["query"])}
	group = None
	if "-all" not in message.command["flags"]:
		if "group" in message.command:
			val = message.command["group"]
			group = await client.get_chat(int(val) if val.isnumeric() else val)
			query["chat"] = group.id
		else:
			query["chat"] = message.chat.id
	user = None
	if "user" in message.command:
		val = message.command["user"]
		user = await client.get_users(int(val) if val.isnumeric() else val)
		query["user"] = user.id
	logger.info(f"Counting {results} most frequent words in last {number} messages")
	response = await edit_or_reply(message, f"` → ` Counting word occurrences...")
	words = []
	curr = 0
	for doc in DRIVER.db.messages.find(query).sort("date", DESCENDING).limit(number):
		if doc["text"]:
			words += [ w for w in re.sub(r"[^0-9a-zA-Z\s\n]+", "", doc["text"].lower()).split() if len(w) > min_len ]
			curr += 1
			if curr % update_interval == 0:
				await client.send_chat_action(message.chat.id, "playing")
				await response.edit(f"` → [{curr}/{number}] ` Counting word occurrences...")
	count = Counter(words).most_common()
	from_who = f"(from **{get_username(user)}**)" if user else ""
	where = "**everywhere**"
	if group:
		if group.invite_link:
			where = f"**[{get_channel(group)}]({group.invite_link})**"
		else:
			where = f"**{get_channel(group)}**"
	output = f"`→ ` {where} {from_who}\n` → ` **{results}** most frequent words __(len > {min_len})__ in last **{curr}** messages:\n"
	for i in range(results):
		output += f"`{i+1:02d}]{'-'*(results-i-1)}>` `{count[i][0]}` `({count[i][1]})`\n"
	await response.edit(output, parse_mode="markdown")
