import re
import json

from time import time
from collections import Counter

from pyrogram import filters
from pyrogram.enums import ParseMode
from pymongo import DESCENDING

from alemibot import alemiBot

from alemibot.util.command import _Message as Message
from alemibot.util import (
	is_allowed, sudo, ProgressChatAction, edit_or_reply, sep, get_text,
	get_username, filterCommand, report_error, set_offline, cancel_chat_action, HelpCategory
)

from ..driver import DRIVER

import logging
logger = logging.getLogger(__name__)

HELP = HelpCategory("CURIOSITY")

@HELP.add(cmd="[<user>]", sudo=False)
@alemiBot.on_message(is_allowed & filterCommand(["freq", "frequent"], options={
	"limit" : ["-l", "--limit"],
	"results" : ["-r", "--results"],
	"minlen" : ["-min"],
	"group" : ["-g", "--group"],
	"query" : ["-q", "--query"],
}, flags=["-all", "-alnum"]))
@report_error(logger)
@set_offline
@cancel_chat_action
async def frequency_cmd(client:alemiBot, message:Message):
	"""find frequent words in messages

	By default, 10 most frequent words are shown, but number of results can be changed with `-r`.
	By default, only words of `len > 3` will be considered. Minimum word len can be specified with `-min`.
	Perform a global search with flag `-all` or search in a specific group with `-g` (only for superuser).
	Provide an username/user_id as argument to count only messages from that user (or reply to a message).
	Extra parameters for the db query can be given with `-q`. (only for superuser)
	Add flag `-alnum` to remove all non-alphanumeric characters.
	"""
	results = min(int(message.command["results"] or 10), 100)
	limit = int(message.command["limit"] or 0)
	min_len = int(message.command["minlen"] or 3)
	replace_unicode = bool(message.command["-alnum"])
	# Build query
	query = {"text":{"$exists":1}} # only msgs with text
	extra_query = False # Extra query
	if sudo(client, message) and "query" in message.command:
		extra_query = True
		query = {**query, **json.loads(message.command["query"])}
	# Add group filter to query
	group = message.chat
	if sudo(client, message):
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
	from_who = f"(from <b>{get_username(user)}</b>)" if user else ""
	extra = f" | + <code>{query}</code>" if extra_query else ""
	where = "<b>everywhere</b>"
	if group:
		where = f"<b>{get_username(group)}</b>"
	output = f"<code>→ </code> {where} {from_who} {extra}\n<code>→ </code> <b>{results}</b> most frequent words (<i>len > {min_len}</i>):\n"
	msg = await edit_or_reply(message, output, parse_mode=ParseMode.HTML, disable_web_page_preview=True) # placeholder msg so we don't ping if usernames show up
	# Iterate db
	def process(text):
		if replace_unicode:
			text = re.sub(r"[^0-9a-zA-Z\s\n\-\_\@]+", "", text)
		return text.lower().split()
	words = []             		
	curr = 0               		
	with ProgressChatAction(client, message.chat.id) as prog:
		cursor = DRIVER.db.messages.find(query).sort("date", DESCENDING)
		if limit > 0:
			cursor.limit(limit)
		async for doc in cursor:
			if doc["text"]:
				words += [ w for w in process(doc["text"]) if len(w) > min_len ]
				curr += 1
		count = Counter(words).most_common()
		stars = 5 if len(count) > 5 else 0
		output = f"<code>→ </code> last <b>{sep(curr)}<b> messages\n"
		for i, word in enumerate(count):
			output += f"<code> → </code> [<b>{sep(word[1])}</b>] <code>{word[0]}</code> {'☆'*stars}\n"
			stars -=1
			if i >= results - 1:
				break
	await edit_or_reply(msg, output, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

@HELP.add(cmd="[<number>]", sudo=False)
@alemiBot.on_message(is_allowed & filterCommand(["active"], options={
	"group" : ["-g", "--group"],
}))
@report_error(logger)
@set_offline
@cancel_chat_action
async def active_cmd(client:alemiBot, message:Message):
	"""find members active in last messages

	Will iterate previous messages (default 100) to find members who sent at least 1 message.
	Specify another group with `-g` (only for superuser).
	"""
	number = int(message.command[0] or 100)
	target_group = message.chat
	if sudo(client, message) and "group" in message.command:
		arg = message.command["group"]
		target_group = await client.get_chat(int(arg) if arg.isnumeric() else arg)
	output = f"<code>→ </code> Active members in last <b>{sep(number)}</b> messages:\n"
	if target_group.id != message.chat.id:
		output = f"<code>→ </code> <b>{get_username(target_group)}</b>\n" + output
	msg = await edit_or_reply(message, output, parse_mode=ParseMode.HTML, disable_web_page_preview=True) # Send a placeholder first to not mention everyone
	users = [] # using a set() would save me a "in" check but sets don't have order. I want most recently active members on top
	query = {"chat":target_group.id}
	with ProgressChatAction(client, message.chat.id) as prog:
		async for doc in DRIVER.db.messages.find(query).sort("date", DESCENDING).limit(number):
			if doc["user"] and doc["user"] not in users:
				users.append(doc["user"])
		users = await client.get_users(users)
		# Build output message
		output = ""
		for usr in users:
			output += f"<code> → </code> <b>{get_username(usr)}</b>\n"
	await edit_or_reply(msg, output, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
