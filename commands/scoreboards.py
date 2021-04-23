from time import time

from bot import alemiBot

from util.command import filterCommand
from util.permission import is_allowed
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
				"Add flag `-all` to list all messages tracked of users in this chat",
				public=True)
@alemiBot.on_message(is_allowed & filterCommand(["topmsg", "topmsgs", "top_messages"], list(alemiBot.prefixes), flags=["-all"]))
@report_error(logger)
@set_offline
async def query_cmd(client, message):
	global_search = "all" in message.command["flags"]
	chat_id = message.command["chat"] if "chat" in message.command else message.chat.id
	chat_title = get_channel(message.chat)
	if type(chat_id) is not int:
		if chat_id.isnumeric():
			chat_id = int(chat_id)
		else:
			chat = await client.get_chat(chat_id)
			chat_title = get_channel(chat)
			chat_id = chat.id
	res = []
	await client.send_chat_action(message.chat.id, "upload_document")
	now = time()
	async for member in message.chat.iter_members():
		if time() - now > 5:
			await client.send_chat_action(message.chat.id, "upload_document")
			now = time()
		flt = {"user": member.user.id}
		if not global_search:
			flt["chat"] = chat_id
		res.append(get_username(member.user), DRIVER.db.messages.count_documents(flt))
	res.sort(key=lambda x: x[1], reverse=True)
	stars = 3
	out = "`→ ` Messages sent\n" if global_search else f"`→ ` Messages sent in {chat_title}\n"
	for usr, msgs in res:
		out += f"` → ` {'☆'*stars} **{usr}** [`{msgs}`]\n"
		stars -= 1
	await edit_or_reply(message, out)
