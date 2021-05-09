# statsbot
This is a plugin for alemiBot. Will log events to a db and provide useful (*and useless*) statistics.

To work it will require a db. Right now, only [MongoDB](https://www.mongodb.com/) is supported, but more drivers will be implemented eventually.

### The bot
You can try [@stats_trackerbot](https://t.me/stats_trackerbot) on Telegram. Add it to your group and check commands with `/help`

# Setup
### Install mongodb
To get most recent `mongodb` version, [follow mongodb docs](https://docs.mongodb.com/manual/tutorial/install-mongodb-on-ubuntu/#import-the-public-key-used-by-the-package-management-system)
### Make user
[Add a user](https://docs.mongodb.com/manual/tutorial/create-users/) for alemiBot with `readWrite` permissions on at least one database
### Add to config
```ini
[database]
username = <usr>
password = <pwd>
dbname = <db>
```
### Install plugin
Install `alemidev/statsbot` (since it's private, either install with `-ssh` or insert user+pwd in terminal for `git clone` via https)

# Data Structure
Data is kept in 5 (+1) collections. Fields marked with `[opt]` are optional.
## Chats
Each chat encountered is serialized as
```json
{
	"id" : <int>,
	"title" : <str>,
	"type" : <str>,
	"flags" : {
		"verified" : <bool>,
		"restricted" : <bool>,
		"scam" : <bool>,
		"fake" : <bool>,
		"support" : <bool>,
		"created" : <bool>
	},
	[opt] "photo" : {
		"small_file_id" : <str>,
		"small_photo_unique_id" : <str>,
		"big_file_id" : <str>,
		"big_photo_unique_id" : <str>
	}
}
```

## Users
Users met are serialized when first seen and fields are updated when needed
```json
{
	"id" : <int>,
	"first_name" : <str>,
	"last_name" : <str|null>,
	"username" : <str|null>,
	"dc_id" : <int|null>,
	"flags" : {
		"self" : <bool>,
		"contact" : <bool>,
		"mutual_contact" : <bool>,
		"deleted" : <bool>,
		"bot" : <bool>,
		"verified" : <bool>,
		"restricted" : <bool>,
		"scam" : <bool>,
		"fake" : <bool>,
		"support" : <bool>
	},
	[opt] "photo" : {
		"small_file_id" : <str>,
		"small_photo_unique_id" : <str>,
		"big_file_id" : <str>,
		"big_photo_unique_id" : <str>
	}
}
```

## Messages
Each message contains only the bare minimun information
```json
{
	"id": <int>,
	"user" : <int>,
	"chat" : <int>,
	"date" : <iso-date>,
	[opt] "empty" : <bool>,
	[opt] "media" : <str>,
	[opt] "text" : <str>,
	[opt] "formatted" : <str>,
	[opt] "scheduled" : <bool>,
	[opt] "author" : <str>,
	[opt] "reply" : <int>,
	[opt] "via_bot" : <bool>,
	[opt] "forward" : {
		"user" : <int|str>,
		"date" : <iso-date>,
		[opt] "id" : <int>,
		[opt] "chat" : <int>
	},
	[opt] "poll" : {
		"question" : <str>,
		"options" : [ <str> ],
	},
	[opt] "keyboard" : [ [ <str> ] ],
	[opt] "inline" : { <inline_keyboard> }, // TODO serialize these
	[opt] "contact" : {
		"phone" : <str>,
		[opt] "first_name" : <str>,
		[opt] "last_name" : <str>,
		[opt] "user_id" : <int>,
		[opt] "vcard" : <str>
	},
	[opt] "web_page" : {
		"url" : <str>,
		"type" : <str>
	},
	[opt] "edits" : [
		{
			"date" : <iso-date>,
			[opt] "text" : <str>,
			[opt] "formatted" : <str>,
			[opt] "keyboard" : [ [ <str> ] ],
			[opt] "inline" : { <inline_keyboard> }, // TODO serialize these
		},
		...
	],
	[opt] "deleted" : <bool>
}
```

## Service
Service messages are stored in a separate collection
```json
{
	"id": <int>,
	"user" : <int|null>,
	"chat" : <int>,
	"date" : <iso-date>,
	[opt] "new_chat_members" : [ <str> ],
	[opt] "left_chat_member" : <int>,
	[opt] "new_chat_title" : <str>,
	[opt] "new_chat_photo" : <str>,
	[opt] "delete_chat_photo" : true,
	[opt] "group_chat_created" : true,
	[opt] "supergroup_chat_created" : true,
	[opt] "channel_chat_created" : true,
	[opt] "migrate_to_chat_id" : <int>,
	[opt] "migrate_from_chat_id" : <int>,
	[opt] "pinned_message" : <int>,
	[opt] "game_high_score" : <int|float>,
	[opt] "voice_chat_started" : true,
	[opt] "voice_chat_ended" : <int>,
	[opt] "voice_chat_members_invited" : [ <int> ]
}
```

## Deletions
Each deletion is marked directly on the message itself, but a log of all deletion events is still kept
```json
{
	"id" : <int>,
	"chat" : <int|null>
	"date" : <iso-date>, // added by bot as it gets received
}
```

### Exceptions
If an exception is raised during serialization, the event object is dumped as-is in an `exceptions` collection with added fields with exception details and the stacktrace itself
