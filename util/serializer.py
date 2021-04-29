from datetime import datetime

from typing import Union, List

from pyrogram.types import Message, User, Chat, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardMarkup

from util.message import parse_media_type
from util.getters import get_text
from util.serialization import convert_to_dict

import logging

logger = logging.getLogger(__name__)

def diff(old:Union[dict,str,int], new:Union[dict,str,int]):
	if not isinstance(old, dict):
		return new
	elif not isinstance(new, dict):
		logger.warning(f"Replacing dict {old} with value {new} while serializing")
		return new
	out = {}
	for key in new:
		if key not in old:
			out[key] = new[key]
		elif old[key] != new[key]:
			out[key] = diff(old[key], new[key])
	return out

def extract_message(msg:Message):
	doc = {
		"id" : msg.message_id,
		"user" : msg.from_user.id if msg.from_user else \
			msg.sender_chat.id if msg.sender_chat else None,
		"chat" : msg.chat.id if msg.chat else None,
		"date" : datetime.utcfromtimestamp(msg.date) if type(msg.date) is int else msg.date,
	}
	if parse_media_type(msg):
		doc["media"] = parse_media_type(msg)
	if get_text(msg, raw=True):
		doc["text"] = get_text(msg, raw=True)
	if msg.from_scheduled:
		doc["scheduled"] = True
	if msg.reply_to_message:
		doc["reply"] = msg.reply_to_message.message_id
	if msg.reply_markup:
		if isinstance(msg.reply_markup, ReplyKeyboardMarkup):
			doc["keyboard"] = msg.reply_markup.keyboard
		elif isinstance(msg.reply_markup, InlineKeyboardMarkup):
			doc["inline"] = convert_to_dict(msg.reply_markup.inline_keyboard) # ewww do it slimmer!
		elif isinstance(msg.reply_markup, ReplyKeyboardRemove):
			doc["keyboard"] = []
	if msg.poll:
		doc["poll"] = {
			"question" : msg.poll.question,
			"options" : [ opt.text for opt in msg.poll.options ]
		}
	if msg.contact:
		doc["contact"] = {"phone": msg.contact.phone_number}
		if msg.contact.first_name:
			doc["contact"]["first_name"] = msg.contact.first_name
		if msg.contact.last_name:
			doc["contact"]["last_name"] = msg.contact.last_name
		if msg.contact.user_id:
			doc["contact"]["user_id"] = msg.contact.user_id
		if msg.contact.vcard:
			doc["contact"]["vcard"] = msg.contact.vcard
	if msg.via_bot:
		doc["via_bot"] = msg.via_bot.username
	return doc

def extract_service_message(msg:Message):
	doc = {
		"id" : msg.message_id,
		"user" : msg.from_user.id if msg.from_user else \
			msg.sender_chat.id if msg.sender_chat else None,
		"chat" : msg.chat.id,
		"date" : datetime.utcfromtimestamp(msg.date) if type(msg.date) is int else msg.date,
	}
	if hasattr(msg, "new_chat_members") and msg.new_chat_members:
		doc["new_chat_members"] = [ u.id for u in msg.new_chat_members ]
	if hasattr(msg, "left_chat_member") and msg.left_chat_member:
		doc["left_chat_member"] = msg.left_chat_member.id
	if hasattr(msg, "new_chat_title") and msg.new_chat_title:
		doc["new_chat_title"] = msg.new_chat_title
	if hasattr(msg, "new_chat_photo") and msg.new_chat_photo:
		doc["new_chat_photo"] = msg.new_chat_photo.file_unique_id
	if hasattr(msg, "delete_chat_photo") and msg.delete_chat_photo:
		doc["delete_chat_photo"] = True
	for tp in ("group_chat_created", "supergroup_chat_created", "channel_chat_created"):
		if hasattr(msg, tp) and getattr(msg, tp):
			doc[tp] = True
	for tp in ("migrate_to_chat_id", "migrate_from_chat_id"):
		if hasattr(msg, tp) and getattr(msg, tp):
			doc[tp] = getattr(msg, tp)
	if hasattr(msg, "pinned_message") and msg.pinned_message:
		doc["pinned_message"] = msg.pinned_message.message_id
	if hasattr(msg, "game_high_score") and msg.game_high_score:
		doc["game_high_score"] = msg.game_high_score.score
	if hasattr(msg, "voice_chat_started") and msg.voice_chat_started:
		doc["voice_chat_started"] = True
	if hasattr(msg, "voice_chat_ended") and msg.voice_chat_ended:
		doc["voice_chat_ended"] = msg.voice_chat_ended.duration
	if hasattr(msg, "voice_chat_members_invited") and msg.voice_chat_members_invited:
		doc["voice_chat_members_invited"] = [ u.id for u in msg.voice_chat_members_invited.users ]
	return doc

def extract_user(msg:Message):
	obj = {
		"id" : msg.from_user.id,
		"first_name" : msg.from_user.first_name,
		"last_name" : msg.from_user.last_name,
		"username" : msg.from_user.username,
		"dc_id" : msg.from_user.dc_id,
		"flags" : {
			"self" : msg.from_user.is_self,
			"contact" : msg.from_user.is_contact,
			"mutual_contact" : msg.from_user.is_mutual_contact,
			"deleted" : msg.from_user.is_deleted,
			"bot" : msg.from_user.is_bot,
			"verified" : msg.from_user.is_verified,
			"restricted" : msg.from_user.is_restricted,
			"scam" : msg.from_user.is_scam,
			"fake" : msg.from_user.is_fake,
			"support" : msg.from_user.is_support,
		},
	}
	if msg.from_user.photo:
		obj["photo"] = {
			"small_file_id" : msg.from_user.photo.small_file_id,
			"small_photo_unique_id" : msg.from_user.photo.small_photo_unique_id,
			"big_file_id" : msg.from_user.photo.big_file_id,
			"big_photo_unique_id" : msg.from_user.photo.big_photo_unique_id,
		}
	return obj

def extract_chat(msg:Message):
	obj = {
		"id" : msg.chat.id,
		"title" : msg.chat.title,
		"type" : msg.chat.type,
		"flags" : {
			"verified" : msg.chat.is_verified,
			"restricted" : msg.chat.is_restricted,
			"scam" : msg.chat.is_scam,
			"fake" : msg.chat.is_fake,
			"support" : msg.chat.is_support,
			"created" : msg.chat.is_creator,
		},
	}
	if msg.chat.photo:
		obj["photo"] = {
			"small_file_id" : msg.chat.photo.small_file_id,
			"small_photo_unique_id" : msg.chat.photo.small_photo_unique_id,
			"big_file_id" : msg.chat.photo.big_file_id,
			"big_photo_unique_id" : msg.chat.photo.big_photo_unique_id,
		}
	return obj

def extract_delete(deletions:List[Message]):
	out = []
	for deletion in deletions:
		out.append({
			"id": deletion.message_id,
			"chat": deletion.chat.id if deletion.chat else None,
			"date": datetime.now(), # It isn't included! Assume it happened when it was received
		})
	return out