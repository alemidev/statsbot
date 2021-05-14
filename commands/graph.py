import io
from datetime import datetime, timedelta

import numpy as np
import matplotlib.pyplot as plt

from bot import alemiBot

from util.permission import is_allowed, check_superuser
from util.message import ProgressChatAction, edit_or_reply
from util.getters import get_username
from util.command import filterCommand
from util.decorators import report_error, set_offline, cancel_chat_action
from util.help import HelpCategory

from plugins.statsbot.driver import DRIVER

import logging
logger = logging.getLogger(__name__)

HELP = HelpCategory("GRAPHS")

@HELP.add(cmd="[<len>]", sudo=False)
@alemiBot.on_message(is_allowed & filterCommand(["activity"], list(alemiBot.prefixes), options={
	"group" : ["-g", "--group"],
}, flags=["-all"]))
@report_error(logger)
@set_offline
@cancel_chat_action
async def graph_cmd(client, message):
	"""show messages per day in last days

	(WIP!) show messages sent per day in last X days (default 30, cap 90) in current group \
	(superuser can specify group or search globally).
	Plot will show most recent values to the right (so 0 is the oldest day).
	"""
	prog = ProgressChatAction(client, message.chat.id, action="playing")
	length = int(message.command[0] or 30)
	target_group = message.chat
	if check_superuser(message):
		if "group" in message.command:
			arg = message.command["group"]
			target_group = await client.get_chat(int(arg) if arg.isnumeric() else arg)
		elif message.command["-all"]:
			target_group = None
	else: # cap length if cmd not run by superuser
		length = min(length, 90)

	now = datetime.now()
	query = {"date":{"$gte": now - timedelta(length)}}
	if target_group:
		query["chat"] = target_group.id

	vals = np.zeros(length)
	for msg in DRIVER.db.messages.find(query):
		delta = now - msg["date"]
		vals[length - delta.days - 1] += 1
		await prog.tick()

	buf = io.BytesIO()

	fig = plt.figure()
	plt.plot(vals)
	fig.savefig(buf)

	buf.seek(0)
	buf.name = "plot.png"

	prog = ProgressChatAction(client, message.chat.id, action="upload_document")
	loc = "sent --globally--" if not target_group else f"in --{get_username(target_group)}--" if target_group.id != message.chat.id else ""
	await client.send_photo(message.chat.id, buf, reply_to_message_id=message.message_id,
									caption=f"` → ` Messages per day {loc} - last **{length}** days", progress=prog.tick)

