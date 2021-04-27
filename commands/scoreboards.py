from time import time
from datetime import datetime

from pymongo import ASCENDING

from bot import alemiBot

from util.command import filterCommand
from util.permission import is_allowed, check_superuser
from util.message import edit_or_reply
from util.getters import get_username, get_channel
from util.decorators import report_error, set_offline
from util.help import HelpCategory

from ..driver import DRIVER

import logging
logger = logging.getLogger(__name__)

HELP = HelpCategory("SCOREBOARDS")

HELP.add_help(["topmsg", "topmsgs", "top_messages"], "list tracked messages for users",
				"checks (tracked) number of messages sent by group members in this chat. " +
				"Add flag `-all` to list all messages tracked of users in this chat, or `-g` and " +
				"specify a group to count in (only for superusers). By default, will only list top 25 members, but " +
				"number of results can be specified with `-r`", args="[-all | -g <group>] [-r <n>]", public=True)
@alemiBot.on_message(is_allowed & filterCommand(["topmsg", "topmsgs", "top_messages"], list(alemiBot.prefixes), options={
	"chat" : ["-g", "--group"],
	"results" : ["-r", "--results"],
}, flags=["-all"]))
@report_error(logger)
@set_offline
async def top_messages_cmd(client, message):
	global_search = "-all" in message.command["flags"]
	results = int(message.command["results"]) if "results" in message.command else 25
	target_chat = message.chat
	if check_superuser(message) and "chat" in message.command:
		tgt = int(message.command["chat"]) if message.command["chat"].isnumeric() \
			else message.command["chat"]
		target_chat = await client.get_chat(tgt)
	res = []
	msg = await edit_or_reply(message, "`→ ` Querying...")
	await client.send_chat_action(message.chat.id, "upload_document")
	now = time()
	async for member in target_chat.iter_members():
		if time() - now > 5:
			await client.send_chat_action(message.chat.id, "upload_document")
			now = time()
		flt = {"user": member.user.id}
		if not global_search:
			flt["chat"] = target_chat.id
		res.append((get_username(member.user), DRIVER.db.messages.count_documents(flt)))
	res.sort(key=lambda x: x[1], reverse=True)
	stars = 3
	count = 0
	out = ""
	if message.outgoing:
		out = message.text + "\n"
	out += "`→ ` Messages sent globally\n" if global_search else f"`→ ` Messages sent in {get_channel(target_chat)}\n"
	for usr, msgs in res:
		out += f"` → ` **{usr}** [`{msgs}`] {'☆'*stars}\n"
		stars -= 1
		count += 1
		if count >= results:
			break
	await msg.edit(out)

HELP.add_help(["joindate", "joindates", "join_date"], "list date users joined group",
				"checks join date for users in current chat (tracks previous joins!). A specific group can be " +
				"specified with `-g` (only by superusers). By default, will only list oldest 25 members, but " +
				"number of results can be specified with `-r`. A list of users can be provided and only their joindates " +
				"will be checked.", args="[-g <group>] [-r <n>] [<users>]", public=True)
@alemiBot.on_message(is_allowed & filterCommand(["joindate", "joindates", "join_date"], list(alemiBot.prefixes), options={
	"chat" : ["-g", "--group"],
	"results" : ["-r", "--results"],
}))
@report_error(logger)
@set_offline
async def joindate_cmd(client, message):
	results = int(message.command["results"]) if "results" in message.command else 25
	target_chat = message.chat
	if check_superuser(message) and "chat" in message.command:
		tgt = int(message.command["chat"]) if message.command["chat"].isnumeric() \
			else message.command["chat"]
		target_chat = await client.get_chat(tgt)
	res = []
	creator = "~~UNKNOWN~~"
	msg = await edit_or_reply(message, "`→ ` Querying...")
	await client.send_chat_action(message.chat.id, "upload_document")
	now = time()
	iterator = message.command["cmd"] if "cmd" in message.command else target_chat.iter_members()
	async for member in iterator:
		if time() - now > 5:
			await client.send_chat_action(message.chat.id, "upload_document")
			now = time()
		if member.status == "creator":
			creator = get_username(member.user)
		else: # Still query db, maybe user left and then joined again! Telegram only tells most recent join
			event = DRIVER.db.service.find_one({"new_chat_members":member.user.id,"chat":target_chat.id}, sort=[("date", ASCENDING)])
			if event:
				res.append((get_username(member.user), event['date']))
			else:
				res.append((get_username(member.user), datetime.utcfromtimestamp(member.joined_date) if
							type(member.joined_date) is int else member.joined_date))
	res.sort(key=lambda x: x[1])
	stars = 3 if len(res) > 3 else 0
	count = 0
	out = ""
	if message.outgoing:
		out = message.text + "\n"
	out += f"`→ ` Join dates in {get_channel(target_chat)}\n"
	out += f"`→ ` **{creator}** [`CREATOR`]\n"
	for usr, date in res:
		if count >= results:
			break
		out += f"` → ` **{usr}** [`{str(date)}`] {'☆'*stars}\n"
		stars -= 1
		count += 1
	await msg.edit(out)