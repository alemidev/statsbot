import io
from datetime import datetime, timedelta

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from bot import alemiBot

from util.permission import is_allowed, check_superuser
from util.message import ProgressChatAction, edit_or_reply
from util.getters import get_username
from util.time import parse_timedelta
from util.command import filterCommand
from util.decorators import report_error, set_offline, cancel_chat_action
from util.help import HelpCategory

from plugins.statsbot.driver import DRIVER

import logging
logger = logging.getLogger(__name__)

HELP = HelpCategory("GRAPHS")

@HELP.add(cmd="[<len>]", sudo=False)
@alemiBot.on_message(is_allowed & filterCommand(["density", "activity"], list(alemiBot.prefixes), options={
	"group" : ["-g", "--group"],
	"user" : ["-u", "--user"],
	"dpi" : ["--dpi"],
	"timezone" : ["-tz", "--timezone"],
}, flags=["-all", "--sunday"]))
@report_error(logger)
@set_offline
@cancel_chat_action
async def graph_cmd(client, message):
	"""show messages per day in last days

	Show messages sent per day in last X days (default 15, cap 90) in current group \
	(superuser can specify group or search globally).
	Get graph of messages from a single user with `-u`.
	Specify plot dpi with `--dpi` (default is 300).
	Add flag `--sunday` to put markers on sundays.
	Dates are UTC, so days may get split weirdly for you. You can compensate by specifying a timezone (`-tz +6`, `-tz -4`).
	Plot will show most recent values to the right. X axis labels format will depend on amount of values plotted.
	"""
	prog = ProgressChatAction(client, message.chat.id, action="playing")
	length = int(message.command[0] or 15)
	dpi = int(message.command["dpi"] or 300)
	time_offset = parse_timedelta(message.command["timezone"] or "X")
	target_group = message.chat
	target_user = None
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
	if "user" in message.command:
		u_input = message.command["user"]
		target_user = await client.get_users(int(u_input) if u_input.isnumeric() else u_input)
		query["user"] = target_user.id

	vals = np.zeros(length)
	for msg in DRIVER.db.messages.find(query):
		delta = (now - msg["date"]) - time_offset
		vals[delta.days] += 1
		await prog.tick()

	buf = io.BytesIO()
	dates = [ now - timedelta(i) for i in range(length) ]

	fig, ax = plt.subplots()

	ax.plot(dates, vals)
	# Major ticks every 7 days.
	ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=(6) if message.command["--sunday"] else (0)))
	# Minor ticks every month.
	ax.xaxis.set_minor_locator(mdates.DayLocator())
	# Set formatter for dates on X axis depending on length
	if length <= 7:
		ax.xaxis.set_minor_formatter(mdates.DateFormatter('%a'))
	elif length <= 20:
		ax.xaxis.set_major_formatter(mdates.DateFormatter('%a %-d'))
	elif length <= 90:
		ax.xaxis.set_major_formatter(mdates.DateFormatter('%-d %h'))
	else:
		ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
	# Turn on grid
	ax.grid(True)

	fig.autofmt_xdate()

	fig.savefig(buf, dpi=dpi)

	buf.seek(0)
	buf.name = "plot.png"

	prog = ProgressChatAction(client, message.chat.id, action="upload_document")
	loc = "sent --globally--" if not target_group else f"in --{get_username(target_group)}--" if target_group.id != message.chat.id else ""
	frm = f"from **{get_username(target_user)}**" if target_user else ""
	await client.send_photo(message.chat.id, buf, reply_to_message_id=message.message_id,
									caption=f"`→ ` Messages per day {frm} {loc}\n` → ` Last **{length}** days", progress=prog.tick)
