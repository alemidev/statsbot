from datetime import datetime
from collections.abc import Iterable

from typing import Union, List
from pyrogram.methods.chats import join_chat

from pyrogram.types import (
	Message, Chat, User, ChatMember, ChatMemberUpdated, ReplyKeyboardMarkup,
	ReplyKeyboardRemove, InlineKeyboardMarkup
)

from util.message import parse_media_type
from util.getters import get_text
from util.serialization import convert_to_dict

import logging

logger = logging.getLogger(__name__)

def diff(old:Union[dict,str,int], new:Union[dict,str,int]):
	if not isinstance(old, dict):
		return new
	elif not isinstance(new, dict):
		logger.warning("Replacing dict %s with value %s while serializing", str(old), str(new))
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
		"date" : datetime.utcfromtimestamp(msg.date),
	}
	if msg.empty:
		doc["empty"] = True
	if parse_media_type(msg):
		doc["media"] = parse_media_type(msg)
	if get_text(msg, raw=True):
		doc["text"] = get_text(msg, raw=True)
		if msg.entities:
			doc["formatted"] = get_text(msg, html=True) # Also get markdown formatted text
	if msg.from_scheduled:
		doc["scheduled"] = True
	if msg.author_signature:
		doc["author"] = msg.author_signature
	if msg.reply_to_message:
		doc["reply"] = msg.reply_to_message.message_id
	if msg.forward_date:
		doc["forward"] = {
			"user": msg.forward_from.id if msg.forward_from else msg.forward_sender_name,
			"date": datetime.utcfromtimestamp(msg.forward_date),
		}
		if msg.forward_from_message_id:
			doc["id"] = msg.forward_from_message_id
		if msg.forward_from_chat:
			doc["chat"] = msg.forward_from_chat.id
	if msg.via_bot:
		doc["via_bot"] = msg.via_bot.username
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
	if msg.web_page:
		doc["web_page"] = {
			"url": msg.web_page.url,
			"type": msg.web_page.type,
		}
	return doc

def extract_edit_message(msg:Message):
	doc = { "date": datetime.utcfromtimestamp(msg.edit_date) }
	if get_text(msg, raw=True):
		doc["text"] = get_text(msg, raw=True)
		if msg.entities:
			doc["formatted"] = get_text(msg, html=True)
	if msg.reply_markup:
		if isinstance(msg.reply_markup, ReplyKeyboardMarkup):
			doc["keyboard"] = msg.reply_markup.keyboard
		elif isinstance(msg.reply_markup, InlineKeyboardMarkup):
			doc["inline"] = convert_to_dict(msg.reply_markup.inline_keyboard) # TODO do it slimmer!
		elif isinstance(msg.reply_markup, ReplyKeyboardRemove):
			doc["keyboard"] = []
	return doc

def extract_service_message(msg:Message):
	doc = {
		"id" : msg.message_id,
		"user" : msg.from_user.id if msg.from_user else \
			msg.sender_chat.id if msg.sender_chat else None,
		"chat" : msg.chat.id,
		"date" : datetime.utcfromtimestamp(msg.date),
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
		doc["game_high_score"] = {
			"game": msg.reply_to_message.game.id,
			"score": msg.game_high_score.score,
		}
	if hasattr(msg, "voice_chat_started") and msg.voice_chat_started:
		doc["voice_chat_started"] = True
	if hasattr(msg, "voice_chat_ended") and msg.voice_chat_ended:
		doc["voice_chat_ended"] = msg.voice_chat_ended.duration
	if hasattr(msg, "voice_chat_members_invited") and msg.voice_chat_members_invited:
		doc["voice_chat_members_invited"] = [ u.id for u in msg.voice_chat_members_invited.users ]
	return doc

def extract_user(user:User):
	obj = {
		"id" : user.id,
		"first_name" : user.first_name,
		"last_name" : user.last_name,
		"username" : user.username,
		"dc_id" : user.dc_id,
		"flags" : {
			"self" : user.is_self,
			"contact" : user.is_contact,
			"mutual_contact" : user.is_mutual_contact,
			"deleted" : user.is_deleted,
			"bot" : user.is_bot,
			"verified" : user.is_verified,
			"restricted" : user.is_restricted,
			"scam" : user.is_scam,
			"fake" : user.is_fake,
			"support" : user.is_support,
		},
	}
	if user.photo:
		obj["photo"] = {
			"small_file_id" : user.photo.small_file_id,
			"small_photo_unique_id" : user.photo.small_photo_unique_id,
			"big_file_id" : user.photo.big_file_id,
			"big_photo_unique_id" : user.photo.big_photo_unique_id,
		}
	return obj

def extract_chat(chat:Chat):
	obj = {
		"id" : chat.id,
		"title" : chat.title,
		"type" : chat.type,
		"flags" : {
			"verified" : chat.is_verified,
			"restricted" : chat.is_restricted,
			"scam" : chat.is_scam,
			"fake" : chat.is_fake,
			"support" : chat.is_support,
			"created" : chat.is_creator,
		},
	}
	if chat.username:
		obj["username"] = chat.username
	if chat.invite_link:
		obj["invite"] = chat.invite_link
	if chat.dc_id:
		obj["dc_id"] = chat.dc_id
	if chat.photo:
		obj["photo"] = {
			"small_file_id" : chat.photo.small_file_id,
			"small_photo_unique_id" : chat.photo.small_photo_unique_id,
			"big_file_id" : chat.photo.big_file_id,
			"big_photo_unique_id" : chat.photo.big_photo_unique_id,
		}
	return obj

def extract_delete(deletions:List[Message]):
	out = []
	if not isinstance(deletions, Iterable): # Sometimes it's not a list for some reason?
		return [{
			"id": deletions.message_id,
			"chat": deletions.chat.id if deletions.chat else None,
			"date": datetime.now(), # It isn't included! Assume it happened when it was received
		}]
	for deletion in deletions:
		out.append({
			"id": deletion.message_id,
			"chat": deletion.chat.id if deletion.chat else None,
			"date": datetime.now(), # It isn't included! Assume it happened when it was received
		})
	return out

def extract_chat_member(member:ChatMember):
	obj = {
		"user": member.user.id,
		"status": member.status,
		"title": member.title,
	}
	if member.until_date:
		obj["until"] = datetime.utcfromtimestamp(member.until_date)
	if member.joined_date:
		obj["joined"] = datetime.utcfromtimestamp(member.joined_date)
	if member.invited_by and member.invited_by.id != member.user.id:
		obj["invited_by"] = member.invited_by.id
	if member.promoted_by:
		obj["promoted_by"] = member.promoted_by.id
	if member.restricted_by:
		obj["restricted_by"] = member.restricted_by.id
	if member.is_member is not None:
		obj["is_member"] = member.is_member
	if member.is_anonymous:
		obj["anonymous"] = member.is_anonymous
	perms = [
		"can_manage_chat", "can_post_messages", "can_edit_messages", "can_delete_messages",
  		"can_restrict_members", "can_promote_members", "can_change_info", "can_invite_users",
		"can_pin_messages", "can_manage_voice_chats",
	
		"can_send_messages", "can_send_media_messages", "can_send_stickers",
		"can_send_animations", "can_send_games", "can_use_inline_bots",
		"can_add_web_page_previews", "can_send_polls"
	]
	for perm in perms:
		if hasattr(member, perm) and getattr(member, perm) is not None:
			if "perms" not in obj:
				obj["perms"] = {}
			obj["perms"][perm] = getattr(member, perm)
	return obj

def extract_member_update(update:ChatMemberUpdated):
	obj = {
		"chat": update.chat.id,
		"date": datetime.utcfromtimestamp(update.date),
		"user": (update.new_chat_member or update.old_chat_member).user.id,
		"performer": update.from_user.id,
	}
	if update.invite_link:
		obj["invite"] = {
			"url": update.invite_link.invite_link,
			"created": datetime.utcfromtimestamp(update.invite_link.date),
			"primary": update.invite_link.is_primary,
		}
		if update.invite_link.creator:
			obj["invite"]["creator"] = update.invite_link.creator.id
		if update.invite_link.expire_date:
			obj["invite"]["expires"] = datetime.utcfromtimestamp(update.invite_link.expire_date)
		if update.invite_link.member_limit:
			obj["invite"]["use_limit"] = update.invite_link.member_limit
		if update.invite_link.member_count:
			obj["invite"]["use_count"] = update.invite_link.member_count
	if update.old_chat_member and not update.new_chat_member:
		obj["left"] = extract_chat_member(update.old_chat_member)
	elif update.new_chat_member and not update.old_chat_member:
		obj["joined"] = extract_chat_member(update.new_chat_member)
	elif update.new_chat_member and update.old_chat_member:
		if update.old_chat_member.user.id != update.new_chat_member.user.id:
			raise ValueError("Cannot serialize: new_chat_member.id different from old_chat_member.id")
		obj["updated"] = extract_chat_member(update.new_chat_member)
	else:
		raise ValueError("Empty ChatMemberUpdated")
	return obj
