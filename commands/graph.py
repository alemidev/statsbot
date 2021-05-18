import io
from datetime import datetime, timedelta

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from bot import alemiBot

from util.permission import is_allowed, check_superuser
from util.message import ProgressChatAction, edit_or_reply
from util.text import sep
from util.getters import get_user, get_username
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
	"keyword" : ["-k", "--keyword"],
	"dpi" : ["--dpi"],
	"timezone" : ["-tz", "--timezone"],
}, flags=["-all", "--sunday"]))
@report_error(logger)
@set_offline
@cancel_chat_action
async def density_cmd(client, message):
	"""show messages per day in last days

	Show messages sent per day in last X days (default 20, cap 90) in current group \
	(superuser can specify group or search globally).
	Get graph of messages from a single user with `-u`.
	Get graph of messages containing a specific keyword with `-k`.
	Specify plot dpi with `--dpi` (default is 300).
	Add flag `--sunday` to put markers on sundays.
	Dates are UTC, so days may get split weirdly for you. You can compensate by specifying a timezone (`-tz +6h`, `-tz -4h`).
	Plot will show most recent values to the right. X axis labels format will depend on amount of values plotted.
	If called in a private chat with bot, will show messages from user from any chat.
	"""
	prog = ProgressChatAction(client, message.chat.id, action="playing")
	length = int(message.command[0] or 20)
	dpi = int(message.command["dpi"] or 300)
	time_offset = parse_timedelta(message.command["timezone"] or "X")
	target_group = message.chat
	target_user = None
	if client.me.is_bot and message.chat.type == "private":
		target_group = None
		target_user = message.from_user
	if "user" in message.command:
		u_input = message.command["user"]
		target_user = await client.get_users(int(u_input) if u_input.isnumeric() else u_input)
	if check_superuser(message):
		if "group" in message.command:
			arg = message.command["group"]
			target_group = await client.get_chat(int(arg) if arg.isnumeric() else arg)
		elif message.command["-all"]:
			target_group = None
	else: # cap length if cmd not run by superuser
		length = min(length, 90)

	now = datetime.now().date()
	query = {"date":{"$gt": datetime.now() - timedelta(length) + time_offset}} # so it's at 00:00
	if target_group:
		query["chat"] = target_group.id
	if target_user:
		query["user"] = target_user.id
	if "keyword" in message.command:
		query["text"] = {"$regex":f"{message.command['keyword']}"}

	vals = np.zeros(length, dtype=np.int32)
	await prog.tick()
	for msg in DRIVER.db.messages.find(query):
		await prog.tick()
		delta = (now - (msg["date"] + time_offset).date())
		if delta.days >= length: # discard extra near limits
			continue
		vals[delta.days] += 1

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
		ax.xaxis.set_major_formatter(mdates.DateFormatter('%a %-d'))
	elif length <= 20:
		ax.xaxis.set_major_formatter(mdates.DateFormatter('%a %-d'))
	elif length <= 90:
		ax.xaxis.set_major_formatter(mdates.DateFormatter('%-d %h'))
	else:
		ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

	plot_title = "Msgs per day" + \
		(f" ({get_username(target_group)})" if target_group else '') + \
		(f" [from {get_username(target_user)}]" if target_user else '') + \
		(f" containing `{message.command['keyword']}`" if message.command["keyword"] else '') + \
		f" | last {length} days"
	ax.set_title(plot_title)
	# Turn on grid
	ax.grid(True)

	fig.autofmt_xdate()

	fig.savefig(buf, dpi=dpi)

	buf.seek(0)
	buf.name = "plot.png"

	prog = ProgressChatAction(client, message.chat.id, action="upload_document")
	caption = f"`→ ` Messages per day | last **{sep(length)}** days" + \
		("\n` → ` sent --globally--" if not target_group else f"\n` → ` in --{get_username(target_group)}--" if target_group.id != message.chat.id else "") + \
		(f"\n` → ` from **{get_username(target_user)}**" if target_user else '') + \
		(f"\n` → ` containing `{message.command['keyword']}`" if message.command['keyword'] else '')
	await client.send_photo(message.chat.id, buf, reply_to_message_id=message.message_id, caption=caption, progress=prog.tick)

@HELP.add(sudo=False)
@alemiBot.on_message(is_allowed & filterCommand(["heat", "heatmap"], list(alemiBot.prefixes), options={
	"group" : ["-g", "--group"],
	"user" : ["-u", "--user"],
	"keyword" : ["-k", "--keyword"],
	"dpi" : ["--dpi"],
	"offset" : ["-o", "--offset"],
	"timezone" : ["-tz", "--timezone"],
}, flags=["-all", "--sunday"]))
@report_error(logger)
@set_offline
@cancel_chat_action
async def heatmap_cmd(client, message):
	"""show heatmap of messages in week

	Show messages sent per day of the week (in last 7 weeks) in current group \
	(superuser can specify group or search globally).
	Get values of messages from a single user with `-u`.
	Get values of messages containing a specific word with `-k`.
	Specify plot dpi with `--dpi` (default is 300).
	Add flag `--sunday` to put markers on sundays.
	Dates are UTC, so days may get split weirdly for you. You can compensate by specifying a timezone (`-tz +6h`, `-tz -4h`).
	Get data of previous weeks by adding a week offset (`-o`).
	Command will not show incomplete week data: first row is last complete week logged.
	Number of weeks to display is locked for style reasons.
	If called in a private chat with bot, will show messages from user from any chat.
	"""
	prog = ProgressChatAction(client, message.chat.id, action="playing")
	dpi = int(message.command["dpi"] or 300)
	week_offset = int(message.command["offset"] or 0)
	time_offset = parse_timedelta(message.command["timezone"] or "X")
	target_group = message.chat
	target_user = None
	if client.me.is_bot and message.chat.type == "private":
		target_group = None
		target_user = message.from_user
	if check_superuser(message):
		if "group" in message.command:
			arg = message.command["group"]
			target_group = await client.get_chat(int(arg) if arg.isnumeric() else arg)
		elif message.command["-all"]:
			target_group = None
	if "user" in message.command:
		u_input = message.command["user"]
		target_user = await client.get_users(int(u_input) if u_input.isnumeric() else u_input)

	# Find last Sunday (last full week basically, so we don't plot a half week data)
	last_week_offset = (week_offset * 7) + (datetime.now().weekday() + (0 if message.command["--sunday"] else 1)) % 7
	now = (datetime.now() - timedelta(last_week_offset) + time_offset).date() # awful var name! TODO rename
	# Build query
	query = {"date":{"$gte": datetime(day=now.day, month=now.month, year=now.year) - timedelta(49 + last_week_offset) + time_offset, 
					"$lte": datetime(day=now.day, month=now.month, year=now.year) + time_offset}} # make new datetimes so it's at 00:00
	if target_group:
		query["chat"] = target_group.id
	if target_user:
		query["user"] = target_user.id
	if "keyword" in message.command:
		query["text"] = {"$regex":message.command['keyword']}

	# Create numpy holder
	vals = np.zeros((7,7), dtype=np.int32)
	await prog.tick()
	for msg in DRIVER.db.messages.find(query):
		await prog.tick()
		date_corrected = (msg["date"] + time_offset).date()
		delta = now - date_corrected # Find timedelta from msg to last_sunday
		if delta.days // 7 >= 7: # discard extra near limits
			continue
		vals[delta.days // 7][date_corrected.weekday()] += 1 # Access week (//7) and weekday

	buf = io.BytesIO()
	dates = [ ( now - timedelta((i*7)+6), now - timedelta(i*7) ) for i in range(7) ] # tuple with week bounds for labels

	week_numbers = []
	for d in dates:
		if d[0].month == d[1].month:
			week_numbers.append(d[0].strftime('%-d') + ' - ' + d[1].strftime('%-d %b'))
		else:
			week_numbers.append(d[0].strftime('%-d %b') + ' - ' + d[1].strftime('%-d %b'))
	week_days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
	if message.command["--sunday"]:
		week_days = week_days[6:] + week_days[:6]

	
	fig, ax = plt.subplots()
	im = ax.imshow(vals)
	
	# Add ticks to heatmap
	ax.set_xticks(np.arange(7))
	ax.set_yticks(np.arange(7))
	ax.set_xticklabels(week_days)
	ax.set_yticklabels(week_numbers)
	
	# Rotate the tick labels and set their alignment.
	plt.setp(ax.get_xticklabels(), rotation=45, ha="right",
	         rotation_mode="anchor")
	
	# Loop over data dimensions and create text annotations.
	for i in range(len(week_numbers)):
	    for j in range(len(week_days)):
	        text = ax.text(j, i, vals[i, j],
	                       ha="center", va="center", color="w")
	
	plot_title = "Msgs per weekday" + \
		(f" ({get_username(target_group)})" if target_group else '') + \
		(f" [from {get_username(target_user)}]" if target_user else '') + \
		(f" containing `{message.command['keyword']}`" if message.command["keyword"] else '')
	ax.set_title(plot_title)
	fig.tight_layout()

	fig.savefig(buf, dpi=dpi)

	buf.seek(0)
	buf.name = "plot.png"

	prog = ProgressChatAction(client, message.chat.id, action="upload_document")
	caption = "`→ ` Messages per weekday" + \
		("\n` → ` sent --globally--" if not target_group else f"\n` → ` in --{get_username(target_group)}--" if target_group.id != message.chat.id else "") + \
		(f"\n` → ` from **{get_username(target_user)}**" if target_user else '') + \
		(f"\n` → ` containing `{message.command['keyword']}`" if message.command['keyword'] else '')
	await client.send_photo(message.chat.id, buf, reply_to_message_id=message.message_id, caption=caption, progress=prog.tick)


@HELP.add(sudo=False)
@alemiBot.on_message(is_allowed & filterCommand(["shift", "timeshift"], list(alemiBot.prefixes), options={
	"group" : ["-g", "--group"],
	"user" : ["-u", "--user"],
	"keyword" : ["-k", "--keyword"],
	"dpi" : ["--dpi"],
	"limit" : ["-l", "--limit"],
	"offset" : ["-tz", "--timezone"],
}, flags=["-all"]))
@report_error(logger)
@set_offline
@cancel_chat_action
async def timeshift_cmd(client, message):
	"""show at which time users are more active

	Show messages sent per time of day in current group (superuser can specify group or search globally).
	Get values of messages from a single user with `-u`.
	Get values of messages containing a certain word with `-k`.
	Specify plot dpi with `--dpi` (default is 300).
	Add flag `--sunday` to put markers on sundays.
	Dates are UTC, so days may get split weirdly for you. You can compensate by specifying an hour offset (`-tz +6`, `-tz -4`).
	Set limit to amount of messages to query with (`-l`).
	Precision is locked at 1hr.
	If called in a private chat with bot, will show messages from user from any chat.
	"""
	prog = ProgressChatAction(client, message.chat.id, action="playing")
	dpi = int(message.command["dpi"] or 300)
	limit = int(message.command["limit"] or 10000)
	time_offset = 0 if "offset" not in message.command else \
			-int(message.command["offset"][1:]) if message.command["offset"].startswith("-") else \
			int(message.command["offset"]) if message.command["offset"].startswith("+") else \
			int(message.command["offset"]) # ye lmao
	target_group = message.chat
	target_user = None
	if client.me.is_bot and message.chat.type == "private":
		target_group = None
		target_user = message.from_user
	if "user" in message.command:
		u_input = message.command["user"]
		target_user = await client.get_users(int(u_input) if u_input.isnumeric() else u_input)
	if check_superuser(message):
		if "group" in message.command:
			arg = message.command["group"]
			target_group = await client.get_chat(int(arg) if arg.isnumeric() else arg)
		elif message.command["-all"]:
			target_group = None
	else:
		limit = min(limit, 100000)

	# Build query
	query = {"date":{"$ne":None}}
	if target_group:
		query["chat"] = target_group.id
	if target_user:
		query["user"] = target_user.id
	if "keyword" in message.command:
		query["text"] = {"$regex":f"{message.command['keyword']}"}

	# Create numpy holder
	vals = np.zeros(24, dtype=np.int32)
	count = 0
	await prog.tick()
	for msg in DRIVER.db.messages.find(query).limit(limit):
		await prog.tick()
		h = int((msg['date'].time().hour + time_offset) % 24)
		vals[h] += 1
		count += 1

	buf = io.BytesIO()
	# labels = [ f"{i:02d}:00-{i+1:02d}:00" for i in range(24) ]
	labels = [ f"{i:02d}" for i in range(24) ]

	fig = plt.figure()
	plt.bar(labels, vals)
	plot_title = "Msgs at hour of day" + \
		(f" ({get_username(target_group)})" if target_group else '') + \
		(f" [from {get_username(target_user)}]" if target_user else '') + \
		(f" containing `{message.command['keyword']}`" if message.command["keyword"] else '') + \
		f" | last {sep(count)}"
	plt.title(plot_title)
	fig.savefig(buf, dpi=dpi)

	buf.seek(0)
	buf.name = "plot.png"

	prog = ProgressChatAction(client, message.chat.id, action="upload_document")
	caption = f"`→ ` Messages per hour [`UTC{time_offset:+02d}`] last **{sep(count)}** messages" + \
		("\n` → ` sent --globally--" if not target_group else f"\n` → ` in --{get_username(target_group)}--" if target_group.id != message.chat.id else "") + \
		(f"\n` → ` from **{get_username(target_user)}**" if target_user else '') + \
		(f"\n` → ` containing `{message.command['keyword']}`" if message.command['keyword'] else '')
	await client.send_photo(message.chat.id, buf, reply_to_message_id=message.message_id, caption=caption, progress=prog.tick)
