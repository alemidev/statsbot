import re
import json

from collections import Counter

from pyrogram import filters
from pymongo import DESCENDING

from bot import alemiBot

from util.permission import is_allowed, is_superuser, check_superuser
from util.message import edit_or_reply
from util.getters import get_text, get_username, get_channel
from util.command import filterCommand
from util.decorators import report_error, set_offline
from util.help import HelpCategory

from ..driver import DRIVER

import logging
logger = logging.getLogger(__name__)

HELP = HelpCategory("CURIOSITY")

HELP.add_help(["freq", "frequent"], "find frequent words in messages",
				"find most used words in last messages. If no number is given, will search only " +
				"last 100 messages. By default, 10 most frequent words are shown, but number of results " +
				"can be changed with `-r`. By default, only words of `len > 3` will be considered. " +
				"A minimum word len can be specified with `-min`. Will search in current group by default. Perform " +
				"a db-wide search with flag `-all`, search in a specific group with `-g` (only for superuser). " +
				"A single user can be specified with `-u` : only messages from that user will count if provided. " +
				"Extra parameters for the db query can be given with `-q`. (only for superuser)",
				args="[-r <n>] [-min <n>] [-all | -g <group>] [-u <user>] [-q <{q}>] [n]", public=True)
@alemiBot.on_message(is_allowed & filterCommand(["freq", "frequent"], list(alemiBot.prefixes), options={
	"results" : ["-r", "-res"],
	"minlen" : ["-min"],
	"group" : ["-g", "-group"],
	"user" : ["-u", "-user"],
	"query" : ["-q", "--query"],
}, flags=["-all"]))
@report_error(logger)
@set_offline
async def frequency_cmd(client, message):
	results = int(message.command["results"]) if "results" in message.command else 10
	number = int(message.command["cmd"][0]) if "cmd" in message.command else 10000
	min_len = int(message.command["minlen"]) if "minlen" in message.command else 3
	query = {"text":{"$exists":1}}
	extra_query = False
	if check_superuser(message) and "query" in message.command:
		extra_query = True
		query = {**query, **json.loads(message.command["query"])}
	group = message.chat
	if "-all" in message.command["flags"]:
		group = None
	elif check_superuser(message) and "group" in message.command:
		tgt = int(message.command["group"])	if message.command["group"].isnumeric() \
			else message.command["group"] 
		group = await client.get_chat(tgt)

	if group:
		query["chat"] = group.id
	user = None
	if "user" in message.command:
		val = message.command["user"]
		user = await client.get_users(int(val) if val.isnumeric() else val)
		query["user"] = user.id
	logger.info(f"Counting {results} most frequent words in last {number} messages")
	response = await edit_or_reply(message, f"` → ` Querying...")
	words = []
	curr = 0
	for doc in DRIVER.db.messages.find(query).sort("date", DESCENDING).limit(number):
		if doc["text"]:
			words += [ w for w in re.sub(r"[^0-9a-zA-Z\s\n]+", "", doc["text"].lower()).split() if len(w) > min_len ]
			curr += 1
	count = Counter(words).most_common()
	# BELOW HERE IS VERY SPAGHETTI, TODO make understandable
	from_who = f"(from **{get_username(user)}**)" if user else ""
	extra = f"`{query}`" if extra_query else ""
	where = "**everywhere**"
	if group:
		where = f"**[{get_channel(group)}]({group.invite_link})**" if group.invite_link else f"**{get_channel(group)}**"
	output = f"`→ ` {where} {from_who} {extra}\n` → ` **{results}** most frequent words __(len > {min_len})__ in last **{curr}** messages:\n"
	for i, word in enumerate(count):
		output += f"`{i:02d}]{'-'*(results-i)}>` `{word[0]}` `({word[1]})`\n"
		if i >= results:
			break
	await edit_or_reply(response, output, parse_mode="markdown")