from typing import Union, List

from pyrogram.types import Message, User, Chat

from util.message import parse_media_type
from util.getters import get_text

def diff(old:Union[dict,str,int], new:Union[dict,str,int]):
	if not isinstance(old, dict):
		if old != new:
			return new
		return None
	elif not isinstance(new, dict):
		return new
	out = {}
	for key in new:
		if key not in old:
			out[key] = new[key]
		elif old[key] != new[key]:
			d = diff(old[key], new[key])
			if d:
				out[key] = d
	return out

def extract_message(msg:Message):
	return {
		"_" : "Message",
		"id" : msg.message_id,
		"user" : msg.from_user.id if msg.from_user else None,
		"chat" : msg.chat.id,
		"date" : msg.date,
		"scheduled" : msg.from_scheduled,
		"text" : get_text(msg, raw=True),
		"edits" : [],
		"media" : parse_media_type(msg),
	}

def extract_user(user:Union[Message,User]):
	if isinstance(user, Message):
		user = user.from_user
	if not user: # if message was sent anonymously
		return {}
	obj = {
		"_" : "User",
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

def extract_chat(chat:Union[Message,Chat]):
	if isinstance(chat, Message):
		chat = chat.chat
	if chat.type == "private":
		return {} # these are already logged as users!
	obj = {
		"_" : "Chat",
		"id" : chat.id,
		"title" : chat.title,
		"type" : chat.type,
		"flags" : {
			"verified" : chat.is_verified,
			"restricted" : chat.is_restricted,
			"scam" : chat.is_scam,
			"fake" : chat.is_fake,
			"support" : chat.is_support,
		},
	}
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
	for deletion in deletions:
		out.append({
			"_" : "Delete",
			"id": deletion.message_id,
			"chat": deletion.chat.id if deletion.chat else None,
			"date": deletion.date,
		})
	return out