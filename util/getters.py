def get_doc_username(doc:dict, mention=True) -> str:
	if "username" in doc and doc["username"]:
		if mention:
			return "@" + doc["username"]
		return doc["username"]
	if "last_name" in doc and doc["last_name"]:
		if mention:
			return f'<a href="tg://user?id={doc["id"]}">{doc["first_name"]} {doc["last_name"]}</a>'
		return doc["first_name"] + " " + doc["last_name"]
	if "first_name" in doc and doc["first_name"]:
		if mention:
			return f'<a href="tg://user?id={doc["id"]}">{doc["first_name"]}</a>'
		return doc["first_name"]
	if "title" in doc and doc["title"]:
		return doc["title"]
	if "id" in doc:
		return str(doc["id"])
	return None
