import sys
import pprint as pp
import shutil
from fuzzywuzzy import fuzz
import fnmatch
from fuzzywuzzy import process
import json

file = open(sys.argv[1])

fields = []
for line in file:
	line = line.replace("\n", "")
	line = line.split("\t")
	field = {}
	field["table_name"] = line[0]
	field["field_name"] = line[1]
	field["dataset"] = sys.argv[1][:-4]
	if line[2] == "":
		field["dist"] = "random"
	elif line[2] == "pk":
		field["dist"] = "pk"
	else:
		field["from"] = line[2]
		field["dist"] = "fk"
	fields.append(field)

for field in fields:
	if field["dist"] == "fk":
		# print(field["from"])
		fieldnames = []
		for fieldy in fields:
			if fieldy["table_name"] == field["from"]:
				fieldnames.append(fieldy["field_name"])
		if process.extractOne(field["field_name"], fieldnames) == None:
			user_input = str(input(f'No field match found. Enter referencing field name: {field["field_name"]} '))
			field["from"] = field["from"] + "." + user_input
		else:
			field["from"] = field["from"] + "." + process.extractOne(field["field_name"], fieldnames)[0]
	elif field["dist"] == "pk":
		field["fk_type"] = ""
		field["fk_args"] = {}
	else:
		field["fk_type"] = ""
		field["fk_args"] = {}

table_names = []
for field in fields:
	if field["table_name"] in table_names:
		pass
	# elif field["dist"] == "pk":
	# 	print(field["field_name"])
	else:
		table_names.append(field["table_name"])

# print(table_names)

for field in fields:
	if field["dist"] == "pk":
		if (fnmatch.fnmatch(field["field_name"], "*at")):
			field["fk_type"] = "past_datetime"
			field["fk_args"] = {"start_date": "-3y"}
			field["bq_type"] = "STRING"
		else:
			field["fk_type"] = "pyint"
			field["bq_type"] = "INT64"
			field["fk_args"] = {"min_value": (table_names.index(field["table_name"])*1000)+1,"max_value": (table_names.index(field["table_name"])+1)*1000}

# js = json.loads(fields)

# pp.pprint(fields)
# pp.pprint(fields)
# check for matches

rules = {
	"pyint": {
		"matches": ["duration", "*id", "value", "price"],
		"default_args": {
			"min": 0,
			"max": 1000
		},
		"bq_type": "INT64",
	},
	"city": {
		"matches": ["city"],
		"default_args": {},
		"bq_type": "STRING",
	},
	"latitude": {
		"matches": ["latitude"],
		"default_args": {},
		"bq_type": "FLOAT64",
	},
	"longitude": {
		"matches": ["longitude"],
		"default_args": {},
		"bq_type": "FLOAT64",
	},
	"state": {
		"matches": ["state"],
		"default_args": {},
		"bq_type": "STRING",
	},
	"state": {
		"matches": ["state"],
		"default_args": {},
		"bq_type": "STRING",
	},
	"past_datetime": {
		"matches": ["*_at", "*date", "time"],
		"default_args": {
			"start_date": "-3y"
		},
		"bq_type": "STRING",
	},
	"safe_email": {
		"matches": ["*email", "email*"],
		"default_args": {
		},
		"bq_type": "STRING",
	},
	"first_name": {
		"matches": ["first_name"],
		"default_args": {
		},
		"bq_type": "STRING",
	},
	"last_name": {
		"matches": ["last_name"],
		"default_args": {
		},
		"bq_type": "STRING",
	},
	"paragraph": {
		"matches": ["description"],
		"default_args": {
			"nb_sentences": 3,
			"variable_nb_sentences": True
		},
		"bq_type": "STRING",
	},
	"domain_name": {
		"matches": ["url", "*_url"],
		"default_args": {
		},
		"bq_type": "STRING",
	},
	"safe_color_name": {
		"matches": ["*type", "category", "timezone"],
		"default_args": {
		},
		"bq_type": "STRING",
	},
	"cryptocurrency_name": {
		"matches": ["name"],
		"default_args": {
		},
		"bq_type": "STRING",
	},
	"pybool": {
		"matches": ["notify_recipients", "channel*"],
		"default_args": {
		},
		"bq_type": "BOOL",
	},
}


for field in fields:
	if field["dist"] == "random":
		matches = 0
		for rule in rules:
			for pat in rules[rule]["matches"]:
				if fnmatch.filter([field["field_name"]], pat):
					field["fk_type"] = rule
					field["fk_args"] = rules[rule]["default_args"]
					field["bq_type"] = rules[rule]["bq_type"]
					matches += 1
		if matches == 0:
			field["fk_type"] = "TODO"
			field["fk_args"] = "TODO"
			field["bq_type"] = "TODO"

output = open(sys.argv[1][:-4]+".bogus", "w")
json.dump(fields, output, indent=2)
output.close()


