#--------------------------
# CAMPUS MESSAGING LOG PARSER
#
# Given a directory as an input
# This script locates all the .log files
# TK Checks them agains the log files already in the database.
# New log files are processed
# and an output .sql file is produced with inserts into 
# UCOPRerports.[Message Files] and UCOPRerports.[Messages]
#--------------------------

import glob
import sys
from datetime import datetime

# Directory in which to look for log files
message_file_directory = sys.argv[1]
print("\nProcessing files in:" + message_file_directory)

# First time var declaration:
init_declare = False

# Set to true to output email address CSV
output_csv = False

# Gets all the log files in the folder where the python script is run
print("Log files found: ", glob.glob(message_file_directory + "/*.log"), "\n") 
log_file_array = glob.glob(message_file_directory + "/*.log")

# TK TK TK check UCOPReports.[Message Files] for contents of log_file_array.
# Return only the array members which are not in the 

if len(log_file_array) < 1:
	print("No log files found. Exiting.\n")
	exit()


sql_file = open ("Test" + "-insert.sql", "a")

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
	for line in fhand:
		if (send_mode_check == False) and (line.find("SEND MODE") != -1):
			send_mode_check = True
			break

	if (send_mode_check == False):
		print("Not processed: " + fname + " - File does not contain SEND MODE.")
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
	sql_file.write("INSERT INTO " + table_name + "\n")
	sql_file.write(column_names + "\n")
	sql_file.write("VALUES" + "\n")
	sql_file.write("('" + fname + "','" + email_datetime.strftime("%Y-%m-%d") + "');\n\n")

	# CSV file
	if (output_csv):
		csv_file = open(fname + "-email-list.csv", "a")

	# Messages SQL
	table_name = "UCOPReports.[Messages]"
	columns_name = "([Email], [Message File ID])"

	# sql_file = open ("email-list-" + email_datetime.strftime("%Y-%m-%d") +"-insert.sql", "a")

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
		sql_file.write(init_tran_header)
	else:
		sql_file.write(tran_header)

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
		sql_file.write("('" + i + "',@MFID)")

		counter += 1

		if (counter % 999 == 0):
			sql_file.write("\nCOMMIT TRANSACTION\n")
			sql_file.write("GO\n")
			if (counter != len(email_addresses)):
				sql_file.write(tran_header)
		elif (counter == len(email_addresses)):
			sql_file.write("\nCOMMIT TRANSACTION\n")
			sql_file.write("GO\n\n")
		else:
			sql_file.write(",\n")


	# close files
	if (output_csv):
		csv_file.close()

	print("File Complete: ", len(email_addresses), " email addresses found.\n")

sql_file.write(";")
sql_file.close()

print("Process complete.")