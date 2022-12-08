#================================
# Send messaging data to UCOPReports
# 2022-12-08 Rewrite
#================================

import sys
import os
import re
from datetime import datetime
import pprint

# _scproxy is a local-testing bug fix. Comment out on server.
import _scproxy
import pymssql
import creds




#================================
# Directory with logs passed as arg
message_file_directory = sys.argv[1]
print("Processing files in:" + message_file_directory)
os.chdir(message_file_directory)



#----------------
# Return a list of of files from the arg dir. matching the regex
def glob_re(pattern, strings):
    return list(filter(re.compile(pattern).match, strings))

log_file_array = glob_re(r'.*_messaging.*log', os.listdir())
print("Logs files to process:")
pprint.pprint(log_file_array)



#----------------
#Connect to database -- change as needed
# mssql_creds = creds.sql_creds_server
mssql_creds = creds.sql_creds_local

conn = pymssql.connect(
	server=mssql_creds['server'],
	port=mssql_creds['port'],
	user=mssql_creds['user'],
	password=mssql_creds['password'],
	database=mssql_creds['database'])

# Create cursor
cursor = conn.cursor(as_dict=True)



#----------------
# Get the existing log files and diff to find the new ones
cursor.execute('SELECT [File Name] FROM UCOPReports.[Message Files]')  
db_filenames_dicts = cursor.fetchall()
db_filenames_list = [i['File Name'] for i in db_filenames_dicts]

# Check db file names
print("Log files in UCOPReports:")
pprint.pprint(db_filenames_list)

# Compare the logs in the directory with the sql query
log_file_array = list(set(log_file_array).difference(db_filenames_list))
print('Remaining logs to process:')
pprint.pprint(log_file_array)

if len(log_file_array) < 1:
	print("No log files to process. Exiting.")
	exit()


#================================
# Loop and process the log files
for log_file in log_file_array:

	print("\nProcessing file: " + log_file)
	try:
		fhand = open(log_file)
	except:
		print('File cannot be opened:',log_file)
		exit()

	if (log_file.find("delegate") != -1
		or log_file.find("nonuc") != -1):
		print("Delegate or non-uc file. Skipping...")
		continue

	#---------------------
	# Check for test runs and timeouts
	send_mode_check = False
	timeout_check = True
	for index, line in enumerate(fhand):

		if (line.find("SEND MODE") != -1):
			send_mode_check = True

		if (line.find("Adaptive Server connection timed out") != -1):
			timeout_check = False

		if (index > 15): break

	if not (send_mode_check and timeout_check):
		print("File not processed: It's either a test run, or contains a server timeout.")
		continue
	else:
		print("File contains SEND MODE, and no CONNECTION TIMED OUT. Proceeding...")


	#---------------------
	# get the email addresses
	email_addresses = []
	for line in fhand:

		# add the email address to the list
		if line != None and "sending message to" in line:
			email_ad = (line.split(' ')[3]).strip('\n')
			email_addresses.append(email_ad)


	#---------------------
	# Set the log_file date
	lf_date_string = log_file.split(".")[0].split("_")[-1]
	lf_datetime = datetime.strptime(lf_date_string, '%d-%b-%Y')
	lf_sql_date = lf_datetime.strftime("%Y-%m-%d")


	#---------------------
	# Insert message file
	sql_message_file = ("INSERT INTO UCOPReports.[Message Files] "
		+ "([File Name], [Email Date]) "
		+ "VALUES ('" + log_file + "', '" + lf_sql_date + "');")

	cursor.execute(sql_message_file)
	conn.commit()

	# Gets the newly-inserted [Message File].id, convert to str
	lf_id = str(cursor.lastrowid)



	#================================
	# Insert messages
	# Split the email array into 900-unit chunks
	def divide_chunks(l, n):
		for i in range(0, len(l), n):
			yield l[i:i + n]

	# An array of arrays
	chunk_size = 900
	email_ads_chunks = list(divide_chunks(email_addresses, chunk_size))

	# Base transaction SQL (XXX will be replaced)
	tr_base = ("BEGIN TRANSACTION\n"
		+ "INSERT INTO UCOPReports.[Messages] "
		+ "([Email], [Message File ID]) "
		+ "VALUES XXX \n\n"
		+ "\nCOMMIT TRANSACTION\nGO\n\n")

	# full insert sql
	messages_sql = ""

	# Loop the chunks
	for chunk in email_ads_chunks:

		# insert values go here
		values_string = ""

		# loop the chunks
		for index, e in enumerate(chunk):

			# Add escape chars if needed
			if e.find("'"):
				e = e.replace("'", "''")

			# Add to values string
			values_string += ("('" + e + "'," + lf_id + ")")

			# If not the end of the chunk, add comma
			if index != (len(chunk) - 1):
				values_string += ",\n"

		# Add the chunk transaction to the full sql
		chunk_tr = tr_base
		messages_sql += chunk_tr.replace("XXX", values_string)

	# Close out the insert
	messages_sql += ";"

	# run the sql
	print("Inserting email addresses for: " + log_file)
	cursor.execute(messages_sql)
	conn.commit()

	print("-------------")


# Close SQL connections
cursor.close()
conn.close()
print("Process complete.")
