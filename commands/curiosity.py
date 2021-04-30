import re
import json

from time import time
from collections import Counter

from pyrogram import filters
from pymongo import DESCENDING

from bot import alemiBot

from util.permission import is_allowed, is_superuser, check_superuser
from util.message import ProgressChatAction, edit_or_reply
from util.getters import get_text, get_username, get_channel
from util.command import filterCommand
from util.decorators import report_error, set_offline
from util.help import HelpCategory

from ..driver import DRIVER

import logging
logger = logging.getLogger(__name__)

HELP = HelpCategory("CURIOSITY")

HELP.add_help(["freq", "frequent"], "find frequent words in messages",
				"find most used words. By default, 10 most frequent words are shown, but number of results " +
				"can be changed with `-r`. By default, only words of `len > 3` will be considered. " +
				"Minimum word len can be specified with `-min`. Perform a global search with flag `-all` or " +
				"search in a specific group with `-g` (only for superuser). Provide an username/user_id as argument " +
				"to count only messages from that user (or reply to a message)." +
				"Extra parameters for the db query can be given with `-q`. (only for superuser)",
				args="[-r <n>] [-min <n>] [-l <n>] [-all | -g <group>] [<user>] [-q <{q}>]", public=True)
@alemiBot.on_message(is_allowed & filterCommand(["freq", "frequent"], list(alemiBot.prefixes), options={
	"limit" : ["-l", "--limit"],
	"results" : ["-r", "--results"],
	"minlen" : ["-min"],
	"group" : ["-g", "--group"],
	"query" : ["-q", "--query"],
}, flags=["-all"]))
@report_error(logger)
@set_offline
async def frequency_cmd(client, message):
	results = min(int(message.command["results"]), 100) if "results" in message.command else 10
	limit = int(message.command["limit"]) if "limit" in message.command else 0
	min_len = int(message.command["minlen"]) if "minlen" in message.command else 3
	# Build query
	query = {"text":{"$exists":1}} # only msgs with text
	extra_query = False # Extra query
	if check_superuser(message) and "query" in message.command:
		extra_query = True
		query = {**query, **json.loads(message.command["query"])}
	# Add group filter to query
	group = message.chat
	if check_superuser(message):
		if "-all" in message.command["flags"]:
			group = None
		elif "group" in message.command:
			tgt = int(message.command["group"])	if message.command["group"].isnumeric() \
				else message.command["group"] 
			group = await client.get_chat(tgt)
	if group:
		query["chat"] = group.id
	# Add user filter to query
	user = None
	if "cmd" in message.command:
		val = message.command["cmd"][0]
		user = await client.get_users(int(val) if val.isnumeric() else val)
		query["user"] = user.id
	logger.info("Counting %d most frequent words", results)
	prog = ProgressChatAction(client, message.chat.id)
	words = []             		
	curr = 0               		
	cursor = DRIVER.db.messages.find(query).sort("date", DESCENDING)
	if limit > 0:
		cursor.limit(limit)
	for doc in cursor:
		await prog.tick()
		if doc["text"]:
			words += [ w for w in re.sub(r"[^0-9a-zA-Z\s\n\-\_\@]+", "", doc["text"].lower()).split() if len(w) > min_len ]
			curr += 1
	count = Counter(words).most_common()
	# Build output message
	stars = 5 if len(count) > 5 else 0
	from_who = f"(from **{get_username(user)}**)" if user else ""
	extra = f" | + `{query}`" if extra_query else ""
	where = "--everywhere--"
	if group:
		where = f"--[{get_channel(group)}]({group.invite_link})--" if group.invite_link else f"--{get_channel(group)}--"
	output = f"`→ ` {where} {from_who} {extra}\n`→ ` **{results}** most frequent words __(len > {min_len})__ in last **{curr}** messages:\n"
	for i, word in enumerate(count):
		output += f"` → ` [`{word[1]}`] **{word[0]}** {'☆'*stars}\n"
		stars -=1
		if i >= results - 1:
			break
	await edit_or_reply(message, output)