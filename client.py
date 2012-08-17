import sqlite3
from urllib import urlopen, urlencode
import httplib, json
from sys import exit
from ConfigParser import ConfigParser
from datetime import datetime
from hashlib import md5
import os
import zipfile
from schemaprops import SchemaProps
from boto.s3.connection import S3Connection
from boto.s3.key import Key
	
CONFIG_FILE = "client.cfg"
DEFAULT_FILES = ["source.txt", "election.txt"]

def get_schema_file():
	location_type = config.get("schema", "location_type")
	location = config.get("schema", "location")
	
	if location_type == "url":
		return urlopen(location)
	elif location_type == "file":
		return open(location)

def setup_db():
	cursor.execute("CREATE TABLE IF NOT EXISTS file_data (file_name TEXT, hash TEXT)")

def write_logs(status):
	
	w = open(config.get("app_settings", "log_file"), "ab")
	w.write("******************"+str(datetime.now())+"*********************\n\n")
	
	if status == "invalid":
		w.write("Could not process data, missing files and/or no xml file provided\n")
	if status == "empty":
		w.write("No update sent due to lack of file changes\n\n")
	if status == "success":
		w.write("Files successfully sent: " + str(files_to_send) + "\n")

def has_changed(fname):
	cursor.execute("SELECT hash FROM file_data WHERE file_name = '" + fname + "'")
	new_hash = file_hash(fname)
	old_vals = cursor.fetchone()
	if not old_vals: 
		cursor.execute("INSERT INTO file_data (file_name, hash) VALUES('" + fname + "','" + new_hash + "')")
		connection.commit()
		return True
	elif old_vals[0] != new_hash: 
		cursor.execute("UPDATE file_data SET hash = '" + new_hash + "' WHERE file_name = '" + fname + "'")
		connection.commit()
		return True
	return False

def file_hash(fname):
	m = md5()
	with open(fname, "rb") as fh:
		for data in fh.read(8192):
			m.update(data)
	return m.hexdigest()

def send_files(files_to_send, key, bucket, directory):
	
	output_file = config.get("local_settings", "output_file")
	f = zipfile.ZipFile(output_file, "w")
	for name in files_to_send:
		f.write(name, os.path.basename(name), zipfile.ZIP_DEFLATED)
	f.close()
	setup_headers = {"Content-type": "application/json", "Accept": "text/plain"}
	conn = httplib.HTTPSConnection("px558.o1.gondor.io")
	conn.request("GET", "/api/data/upload-request/", '', setup_headers)
	response = conn.getresponse()
	aws_setup = json.loads(response.read())
	access_val = aws_setup["AWSAccessKeyId"]
	conn = S3Connection(access_val, key)
	b = conn.create_bucket(bucket)
	k = Key(b)
	k.key = directory + output_file
	print "sending files"
	k.set_contents_from_filename(output_file)
	conn.close()

def clean_directory(directory):
	if not directory.endswith("/"):
		return directory + "/"
	return directory

config = ConfigParser()
config.read(CONFIG_FILE)

schema_file = get_schema_file()
sp = SchemaProps(schema_file)
db_file_list = sp.key_list("db")

file_directory = config.get("local_settings", "file_directory")
file_directory = clean_directory(file_directory)
key = config.get("connection_settings", "key")
bucket = config.get("connection_settings", "bucket")
directory = config.get("connection_settings", "output_folder")

connection = sqlite3.connect(config.get("app_settings", "db_host"))
cursor = connection.cursor()
setup_db()

files_to_send = []
xml_file = None
config_file = None
new_data = False

files_list = os.listdir(file_directory)

if config.has_option("local_settings","feed_data") and len(config.get("local_settings","feed_data")) > 0:
	feed_file = config.get("local_settings","feed_data")
	if os.exists(feed_file):
		xml_file = feed_file
	if has_changed(feed_file):
		new_data = True
		files_to_send.append(xml_file)
else:
	for f in files_list:
		if f.endswith(".xml"):
			xml_file = file_directory+f
			if has_changed(xml_file):
				new_data = True
				files_to_send.append(xml_file)
		elif f == CONFIG_FILE:
			config_file = file_directory + f
		elif f.lower().split(".")[0] in db_file_list:
			if has_changed(file_directory + f):
				new_data = True
				files_to_send.append(file_directory + f)
			
if new_data:
	if xml_file and xml_file not in files_to_send:
		files_to_send.append(xml_file)
	else:
		for d in DEFAULT_FILES:
			if (file_directory + d) not in files_to_send and d in files_list:
				files_to_send.append(file_directory + d)
			elif d not in files_list:
				write_logs("invalid")
				exit(0)
	if config_file:
		files_to_send.append(config_file)
	send_files(files_to_send, key, bucket, directory)
	write_logs("success")
else:
	write_logs("empty")
