import re
import json

from time import time
from collections import Counter

from pyrogram import filters
from pymongo import DESCENDING

from bot import alemiBot

from util.permission import is_allowed, is_superuser, check_superuser
from util.message import ProgressChatAction, edit_or_reply
from util.text import sep
from util.getters import get_text, get_username, get_channel
from util.command import filterCommand
from util.decorators import report_error, set_offline, cancel_chat_action
from util.help import HelpCategory

from plugins.statsbot.driver import DRIVER

import logging
logger = logging.getLogger(__name__)

HELP = HelpCategory("CURIOSITY")

@HELP.add(cmd="[<user>]", sudo=False)
@alemiBot.on_message(is_allowed & filterCommand(["freq", "frequent"], list(alemiBot.prefixes), options={
	"limit" : ["-l", "--limit"],
	"results" : ["-r", "--results"],
	"minlen" : ["-min"],
	"group" : ["-g", "--group"],
	"query" : ["-q", "--query"],
}, flags=["-all"]))
@report_error(logger)
@set_offline
@cancel_chat_action
async def frequency_cmd(client, message):
	"""find frequent words in messages

	By default, 10 most frequent words are shown, but number of results can be changed with `-r`.
	By default, only words of `len > 3` will be considered. Minimum word len can be specified with `-min`.
	Perform a global search with flag `-all` or search in a specific group with `-g` (only for superuser).
	Provide an username/user_id as argument to count only messages from that user (or reply to a message).
	Extra parameters for the db query can be given with `-q`. (only for superuser)
	"""
	results = min(int(message.command["results"] or 10), 100)
	limit = int(message.command["limit"] or 0)
	min_len = int(message.command["minlen"] or 3)
	# Build query
	query = {"text":{"$exists":1}} # only msgs with text
	extra_query = False # Extra query
	if check_superuser(message) and "query" in message.command:
		extra_query = True
		query = {**query, **json.loads(message.command["query"])}
	# Add group filter to query
	group = message.chat
	if check_superuser(message):
		if message.command["-all"]:
			group = None
		elif "group" in message.command:
			tgt = int(message.command["group"])	if message.command["group"].isnumeric() \
				else message.command["group"] 
			group = await client.get_chat(tgt)
	if group:
		query["chat"] = group.id
	# Add user filter to query
	user = None
	if len(message.command) > 0:
		val = message.command[0]
		user = await client.get_users(int(val) if val.isnumeric() else val)
		query["user"] = user.id

	# Build output message
	from_who = f"(from **{get_username(user)}**)" if user else ""
	extra = f" | + `{query}`" if extra_query else ""
	where = "--everywhere--"
	if group:
		where = f"--[{get_channel(group)}]({group.invite_link})--" if group.invite_link else f"--{get_channel(group)}--"
	output = f"`→ ` {where} {from_who} {extra}\n`→ ` **{results}** most frequent words __(len > {min_len})__ in last **{limit}** messages:\n"
	msg = await edit_or_reply(message, output) # placeholder msg so we don't ping if usernames show up
	# Iterate db
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
	stars = 5 if len(count) > 5 else 0
	for i, word in enumerate(count):
		output += f"` → ` [**{sep(word[1])}**] `{word[0]}` {'☆'*stars}\n"
		stars -=1
		if i >= results - 1:
			break
	await edit_or_reply(msg, output)

@HELP.add(cmd="[<number>]", sudo=False)
@alemiBot.on_message(is_allowed & filterCommand(["active"], list(alemiBot.prefixes), options={
	"group" : ["-g", "--group"],
}))
@report_error(logger)
@set_offline
@cancel_chat_action
async def active_cmd(client, message):
	"""find members active in last messages

	Will iterate previous messages (default 100) to find members who sent at least 1 message.
	Specify another group with `-g` (only for superuser).
	"""
	number = int(message.command[0] or 100)
	target_group = message.chat
	if check_superuser(message) and "group" in message.command:
		arg = message.command["group"]
		target_group = await client.get_chat(int(arg) if arg.isnumeric() else arg)
	output = f"`→ ` Active members in last {number} messages:\n"
	if target_group.id != message.chat.id:
		output = f"`→ ` **{get_username(target_group)}**\n" + output
	msg = await edit_or_reply(message, output) # Send a placeholder first to not mention everyone
	query = {"chat":target_group.id}
	prog = ProgressChatAction(client, message.chat.id)
	users = [] # using a set() would save me a "in" check but sets don't have order. I want most recently active members on top
	for doc in DRIVER.db.messages.find(query).sort("date", DESCENDING).limit(number):
		await prog.tick()
		if doc["user"] and doc["user"] not in users:
			users.append(doc["user"])
	users = await client.get_users(users)
	# Build output message
	output = ""
	for usr in users:
		output += f"` → ` **{get_username(usr)}**\n"
	await edit_or_reply(msg, output)
