from pymongo import DESCENDING

from bot import alemiBot

from util.command import filterCommand
from util.permission import is_allowed
from util.getters import get_username, get_channel
from util.message import edit_or_reply, is_me
from util.decorators import report_error, set_offline
from util.help import HelpCategory

from ..driver import DRIVER

import logging
logger = logging.getLogger(__name__)

HELP = HelpCategory("BIG-BROTHER")

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
	if "group" in args:
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
			author=f"**{get_username(await client.get_user(doc['user']))}** >" if show_author else "",
			text=doc["text"],
		) 
		if "edits" in doc:
			for edit in doc["edits"]:
				out += LINE.format(
					date=f"[--{edit['date']}--]" if show_time else "",
					text=edit["text"],
				)
		await edit_or_reply(message, out)
	else:
		await edit_or_reply(message, "`[!] → ` Nothing found")

HELP.add_help(["peek", "deld", "deleted", "removed"], "get deleted messages",
				"request last deleted messages in this channel. Use `-t` to add timestamps. A number of messages to peek can be specified. " +
				"Bots don't receive deletion events, so you must reply to a message to peek messages sent just before. " +
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
	if client.me.is_bot and not message.reply_to_message:
		return await edit_or_reply(message, "`[!] → ` You need to reply to a message")
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
		if client.me.is_bot:
			limit = min(limit, 5)
	logger.info(f"Peeking {limit} messages")
	count = 0
	if client.me.is_bot: # bots don't receive delete events so peek must work slightly differently
		date_limit = DRIVER.db.messages.find_one({"id":message.reply_to_message.message_id, "chat":target_group.id})['date']
		flt = {"date": {"$lt":date_limit}}
	else:
		flt = {"deleted": True}
	if target_group:
		flt["chat"] = target_group.id
	out = f"`→ Peeking {limit} message{'s' if limit > 1 else ''} " + \
			('in ' + get_channel(target_group) if target_group is not None else '') + "`\n\n"
	response = await edit_or_reply(message, out)
	LINE = "{time}`[{m_id}]` **{user}** {where} → {text} {media}\n"
	cursor = DRIVER.db.messages.find(flt).sort("date", DESCENDING)
	for doc in cursor:
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
			where=get_channel(group) if target_group is None and group else "",
			text=doc["text"] if "text" in doc else "",
			media=f"<~~{doc['media']}~~>" if "media" in doc else "",
		)
		count += 1
		if count >= limit:
			break
	if count == 0:
		out += "`[!] → ` Nothing to display"
	await response.edit(out)