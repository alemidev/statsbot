from datetime import datetime
from collections.abc import Iterable

from typing import Union, List, Dict, Any
from pyrogram.methods.chats import join_chat

from pyrogram.types import (
	Message, Chat, User, ChatMember, ChatMemberUpdated, ReplyKeyboardMarkup,
	ReplyKeyboardRemove, InlineKeyboardMarkup, ChatPrivileges
)

from alemibot.util import convert_to_dict

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
	doc : Dict[str, Any] = {
		"id" : msg.id,
		"user" : msg.from_user.id if msg.from_user else \
			msg.sender_chat.id if msg.sender_chat else None,
		"chat" : msg.chat.id if msg.chat else None,
		"date" : msg.date,
	}
	if msg.empty:
		doc["empty"] = True
	if msg.media: # TODO maybe get enum value? idk enums are new
		doc["media"] = str(msg.media)
	if msg.text or msg.caption:
		doc["text"] = msg.text or msg.caption
		if msg.entities:
			if msg.text:  # could be a oneliner but mypy gets angry
				doc["formatted"] = msg.text.html
			elif msg.caption:
				doc["formatted"] = msg.caption.html
	if msg.from_scheduled:
		doc["scheduled"] = True
	if msg.author_signature:
		doc["author"] = msg.author_signature
	if msg.reply_to_message:
		doc["reply"] = msg.reply_to_message.id
	if msg.forward_date:
		doc["forward"] = {
			"user": msg.forward_from.id if msg.forward_from else msg.forward_sender_name,
			"date": msg.forward_date,
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
	doc : Dict[str, Any] = { "date": msg.edit_date }
	if msg.text or msg.caption:
		doc["text"] = msg.text or msg.caption
		if msg.entities:
			if msg.text:  # could be a oneliner but mypy gets angry
				doc["formatted"] = msg.text.html
			elif msg.caption:
				doc["formatted"] = msg.caption.html
	if msg.reply_markup:
		if isinstance(msg.reply_markup, ReplyKeyboardMarkup):
			doc["keyboard"] = msg.reply_markup.keyboard
		elif isinstance(msg.reply_markup, InlineKeyboardMarkup):
			doc["inline"] = convert_to_dict(msg.reply_markup.inline_keyboard) # TODO do it slimmer!
		elif isinstance(msg.reply_markup, ReplyKeyboardRemove):
			doc["keyboard"] = []
	return doc

def extract_service_message(msg:Message):
	doc : Dict[str, Any] = {
		"id" : msg.id,
		"user" : msg.from_user.id if msg.from_user else \
			msg.sender_chat.id if msg.sender_chat else None,
		"chat" : msg.chat.id if msg.chat else None,
		"date" : msg.date,
	}
	if msg.reply_to_message:
		doc["reply"] = msg.reply_to_message.id
	if msg.new_chat_members:
		doc["new_chat_members"] = [ u.id for u in msg.new_chat_members ]
	if msg.left_chat_member:
		doc["left_chat_member"] = msg.left_chat_member.id
	if msg.new_chat_title:
		doc["new_chat_title"] = msg.new_chat_title
	if msg.new_chat_photo:
		doc["new_chat_photo"] = msg.new_chat_photo.file_unique_id
	if msg.delete_chat_photo:
		doc["delete_chat_photo"] = msg.delete_chat_photo
	if msg.group_chat_created:
		doc["group_chat_created"] = msg.group_chat_created
	if msg.supergroup_chat_created:
		doc["supergroup_chat_created"] = msg.supergroup_chat_created
	if msg.channel_chat_created:
		doc["channel_chat_created"] = msg.channel_chat_created
	if msg.migrate_to_chat_id:
		doc["migrate_to_chat_id"] = msg.migrate_to_chat_id
	if msg.migrate_from_chat_id:
		doc["migrate_from_chat_id"] = msg.migrate_from_chat_id
	if msg.pinned_message:
		doc["pinned_message"] = msg.pinned_message.id
	if msg.game_high_score and msg.reply_to_message and msg.reply_to_message.game:
		doc["game_high_score"] = {
			"game": msg.reply_to_message.game.id,
			"score": msg.game_high_score,
		}
	if msg.video_chat_started:
		doc["video_chat_started"] = True
	if msg.video_chat_ended:
		doc["video_chat_ended"] = msg.video_chat_ended.duration
	if msg.video_chat_members_invited:
		doc["video_chat_members_invited"] = [ u.id for u in msg.video_chat_members_invited.users ]
	return doc

def extract_user(user:User):
	obj : Dict[str, Any] = {
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
	obj : Dict[str, Any] = {
		"id" : chat.id,
		"title" : chat.title,
		"type" : chat.type.value,
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

def extract_delete(deletions:Union[Message, List[Message]]):
	out = []
	if not isinstance(deletions, Iterable): # Sometimes it's not a list for some reason?
		return [{
			"id": deletions.id,
			"chat": deletions.chat.id if deletions.chat else None,
			"date": datetime.now(), # It isn't included! Assume it happened when it was received
		}]
	for deletion in deletions:
		out.append({
			"id": deletion.id,
			"chat": deletion.chat.id if deletion.chat else None,
			"date": datetime.now(), # It isn't included! Assume it happened when it was received
		})
	return out

def extract_chat_member(member:ChatMember):
	obj : Dict[str, Any] = {
		"user": member.user.id if member.user else None,
		"status": member.status._name_,  # TODO is this reliable?
		"title": member.custom_title,
	}
	if member.until_date:
		obj["until"] = member.until_date
	if member.joined_date:
		obj["joined"] = member.joined_date
	if member.user and member.invited_by and member.invited_by.id != member.user.id:
		obj["invited_by"] = member.invited_by.id
	if member.promoted_by:
		obj["promoted_by"] = member.promoted_by.id
	if member.restricted_by:
		obj["restricted_by"] = member.restricted_by.id
	if member.is_member is not None:
		obj["is_member"] = member.is_member
	if member.privileges and member.privileges.is_anonymous:
		obj["anonymous"] = member.privileges.is_anonymous
	for perm in dir(member.privileges): # TODO there's probably a better way
		if perm.startswith('_'):
			continue
		if hasattr(member.privileges, perm) and getattr(member.privileges, perm) is not None:
			if "perms" not in obj:
				obj["perms"] = {}
			obj["perms"][perm] = getattr(member.privileges, perm)
	return obj

def extract_member_update(update:ChatMemberUpdated):
	m = update.new_chat_member or update.old_chat_member
	obj : Dict[str, Any] = {
		"chat": update.chat.id,
		"date": update.date,
		"user": m.user.id if m.user else None,
		"performer": update.from_user.id,
	}
	if update.invite_link:
		obj["invite"] = {
			"url": update.invite_link.invite_link,
			"created": update.invite_link.date,
			"primary": update.invite_link.is_primary,
		}
		if update.invite_link.creator:
			obj["invite"]["creator"] = update.invite_link.creator.id
		if update.invite_link.expire_date:
			obj["invite"]["expires"] = update.invite_link.expire_date
		if update.invite_link.member_limit:
			obj["invite"]["use_limit"] = update.invite_link.member_limit
		if update.invite_link.member_count:
			obj["invite"]["use_count"] = update.invite_link.member_count
	if update.old_chat_member and not update.new_chat_member:
		obj["left"] = extract_chat_member(update.old_chat_member)
	elif update.new_chat_member and not update.old_chat_member:
		obj["joined"] = extract_chat_member(update.new_chat_member)
	elif update.new_chat_member and update.old_chat_member:
		if update.old_chat_member.user and update.new_chat_member.user \
		and update.old_chat_member.user.id != update.new_chat_member.user.id:
			raise ValueError("Cannot serialize: new_chat_member.id different from old_chat_member.id")
		obj["updated"] = extract_chat_member(update.new_chat_member)
	else:
		raise ValueError("Empty ChatMemberUpdated")
	return obj
