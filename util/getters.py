def get_doc_username(doc:dict, mention=True) -> str:
	if "username" in doc:
		if mention:
			return "@" + doc["username"]
		return doc["username"]
	if "last_name" in doc:
		if mention:
			return f'<a href="tg://user?id={doc["id"]}>{doc["first_name"]} {doc["last_name"]}</a>'
		return doc["first_name"] + " " + doc["last_name"]
	if mention:
		return f'<a href="tg://user?id={doc["id"]}>{doc["first_name"]}</a>'
	return doc["first_name"]