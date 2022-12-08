#===============================
# CAMPUS MESSAGING LOG PARSER
#
# Given a directory as an input
# local the .log files in that directory
# compare them with the files already in UCOPReports.[Message Files]
# process the new files
# output an .sql file, with inserts for
# UCOPReports.[Message Files] and UCOPReports.[Messages]
#===============================

import sys
import os
import re
from datetime import datetime

# _scproxy required to run pymssql locally (bug fix)
import _scproxy
import pymssql

#===============================
# Server configs


#===============================
# Main program

# Get the current working directory
script_dir = os.getcwd()
print("Current working directory: {0}".format(script_dir))

# Directory in which to look for log files
# Move to the supplied directory
message_file_directory = sys.argv[1]
print("\nProcessing files in:" + message_file_directory + "\n")
os.chdir(message_file_directory)

# Checks the message file dir
# for .log files matching the regex
def glob_re(pattern, strings):
    return list(filter(re.compile(pattern).match, strings))

log_file_array = glob_re(r'(campus|lbl|rgpo)_messaging.*log', os.listdir())
print("Logs files to process:")
print(log_file_array)
print()

#-------------------------
# Query the db for extant log files
print("Querying UCOPReports.[Message Files] for extant log files...")

# Connect to database
conn = pymssql.connect(
	server=active_config['server'],
	port=active_config['port'],
	user=active_config['user'],
	password=active_config['password'],
	database=active_config['database'])

cursor = conn.cursor()  
cursor.execute('SELECT [File Name] FROM UCOPReports.[Message Files]')  
row = cursor.fetchone()

logs_in_message_files = []
while row:
	logs_in_message_files.append(str(row[0]))
	row = cursor.fetchone()

# Debug query results:
# print( "Logs in message files:\n", logs_in_message_files)
print("Query complete.")

# Compare the logs in the directory with the sql query
log_file_array = list(set(log_file_array).difference(logs_in_message_files))
print('Remaining logs to process:')
print(log_file_array)

if len(log_file_array) < 1:
	print("No log files to process. Exiting.\n")
	exit()

#-------------------------
# Process the new log files

# First time var declaration:
init_declare = False

# Set to true to output email address CSV
output_csv = False

# Text file for sql insert commands
# sql_file = open (script_dir + "/Test" + "-insert.sql", "a")
sql_insert = "";

# Loop through the log files and create the SQL inert code
for log_file in log_file_array:
	fname = log_file

	# Open file / exception
	try:
		fhand = open(fname)
	except:
		print('File cannot be opened:',fhand)
		exit()

	#--------------------------
	# Check for "send mode" text.
	send_mode_check = False
	timeout_check = False
	check_counter = 0
	for line in fhand:
		if (send_mode_check == False) and (line.find("SEND MODE") != -1):
			send_mode_check = True
		elif (timeout_check == False) and (line.find("Adaptive Server connection timed out") != -1):
			timeout_check = True

		check_counter += 1
		if (check_counter > 15):
			break

	if (send_mode_check == False):
		print("Not processed: " + fname + " - File does not contain SEND MODE.")
		continue
	elif (timeout_check == True):
		print("Not processed: " + fname + " contains a an 'Adaptive Server Connection timed out' error line.")
		continue
	else:
		print("Processing:\n" + fname +"\nFile contains SEND MODE.")


	#--------------------------
	# Pull the emails and set the date
	email_addresses = []
	email_datetime = None

	# Find the sending email message
	for line in fhand:
		if line != None and "sending message to" in line:

			# add the email address to the list
			email_ad = (line.split(' ')[3]).strip('\n')
			email_addresses.append(email_ad)

		# Find the date
		if line != None and email_datetime == None and "Date: " in line:

			# Split line to get just the date
			email_date = ""
			line_split = line.split(' ')
			for i in range(2, 5):
				email_date += line_split[i]

			# convert to date object
			email_datetime = datetime.strptime(email_date, '%d%b%Y')


	#--------------------------
	# Output files

	# Message Files SQL
	
	table_name = "UCOPReports.[Message Files]"
	column_names = "([File Name],[Email Date])"
	sql_insert += ("INSERT INTO " + table_name + "\n")
	sql_insert += (column_names + "\n")
	sql_insert += ("VALUES" + "\n")
	sql_insert += ("('" + fname + "','" + email_datetime.strftime("%Y-%m-%d") + "');\n\n")

	# CSV file
	if (output_csv):
		csv_file = open(fname + "-email-list.csv", "a")

	# Messages SQL
	table_name = "UCOPReports.[Messages]"
	columns_name = "([Email], [Message File ID])"

	# sql_insert = open ("email-list-" + email_datetime.strftime("%Y-%m-%d") +"-insert.sql", "a")

	# Build the initial transaction header 
	init_tran_header = ""

	if init_declare == False:
		init_tran_header = "BEGIN TRANSACTION\n"
		init_tran_header += "DECLARE @MFID AS INT=(SELECT mf.[id]\n"
		init_tran_header += "FROM UCOPReports.[Message Files] mf\n"
		init_tran_header += "WHERE mf.[File Name]\n"
		init_tran_header += "LIKE('%" + fname + "%'))\n"
		init_tran_header += "INSERT INTO " + table_name + "\n" + columns_name + "\n"
		init_tran_header += "VALUES" + "\n"
	
	# Build the standard transaction header
	tran_header = "BEGIN TRANSACTION\n"
	tran_header += "SET @MFID = (SELECT mf.[id]\n"
	tran_header += "FROM UCOPReports.[Message Files] mf\n"
	tran_header += "WHERE mf.[File Name]\n"
	tran_header += "LIKE('%" + fname + "%'))\n"
	tran_header += "INSERT INTO " + table_name + "\n" + columns_name + "\n"
	tran_header += "VALUES" + "\n"

	if init_declare == False:
		init_declare = True;
		sql_insert += (init_tran_header)
	else:
		sql_insert += (tran_header)

	counter = 0
	for i in email_addresses:

		# Add escape chars if needed
		if i.find("'"):
			i = i.replace("'", "''")

		# write to CSV
		if (output_csv):
			csv_file.write(i + "," + email_datetime.strftime("%Y-%m-%d"))
			csv_file.write("\n")

		# write to SQL
		sql_insert += ("('" + i + "',@MFID)")

		counter += 1

		if (counter % 999 == 0):
			sql_insert += ("\nCOMMIT TRANSACTION\n")
			sql_insert += ("GO\n")
			if (counter != len(email_addresses)):
				sql_insert += (tran_header)
		elif (counter == len(email_addresses)):
			sql_insert += ("\nCOMMIT TRANSACTION\n")
			sql_insert += ("GO\n\n")
		else:
			sql_insert += (",\n")


	# close files
	if (output_csv):
		csv_file.close()

	print("File Complete: ", len(email_addresses), " email addresses found.\n")

sql_insert += (";")

print("-------------")
print("Excuting SQL INSERTs.")

cursor.execute(sql_insert)
conn.commit()

print("Process complete.")
