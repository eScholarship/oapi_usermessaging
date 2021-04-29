#!/usr/bin/env ruby

# Use bundler to keep dependencies local
require 'rubygems'
require 'bundler/setup'

require 'json'
require 'optparse'
require 'tiny_tds'
require 'erb'
require 'mail'
require_relative 'lastsentdb'

include Lastsentdb

# Autoflush STDOUT
STDOUT.sync = true
$wel=LastSent.new
$bouncing=File.readlines('bounces.txt').map{|x| x.gsub("\n","")}
$donotsend =File.readlines('doNotSend.txt').map{|x| x.gsub("\n","")}

def parseArgs()

    $args = {:group => 'lbl', :message_type => 'compound', :mode => 'test'}

    parser = OptionParser.new do|opts|
    	opts.banner = "Usage: user_messaging.rb [options]"
    	opts.on('-g', '--group group', 'Group') do |group|
    		$args[:group] = group;
    	end
    	opts.on('-t', '--message_type message_type', 'Message Type') do |message_type|
    		$args[:message_type] = message_type;
    	end
    	opts.on('-m', '--mode mode', 'Mode') do |mode|
    		$args[:mode] = mode;
    	end
    	opts.on('-h', '--help', 'Displays Help') do
    		puts opts
    		exit!
    	end
    end

    parser.parse!

    if $args[:group] == nil or $args[:message_type] == nil
      puts "You must indicate both group and message type"
      exit!
    end
end

def chooseMessageFrom(group)
  $from = "Scientific Publications Team <sender@email.org>"
end

def chooseMessageSubject(group)
  case group
  when /cbcrp/
    $subject = "Your CBCRP Open Access Compliance"
  when /chrp/
    $subject = "Your CHRP Open Access Compliance"
  when /trdrp/
    $subject = "Your TRDRP Open Access Compliance"
  when /other/
    $subject = "Your UCRI Open Access Compliance"
  when /lbl/
    $subject = "Your LBNL Publications’ Compliance Status"
  else
    raise "subject not found for group: #{group}"
  end
end

def chooseMessageTemplate(group)
  $template = File.read('compound.html.erb')
end

def sendCompound()
  if (!$need_action.empty?)
    $need_action.each do |id|
      group = $user_data[id][:group]
      chooseMessageFrom(group)
      chooseMessageSubject(group)
      chooseMessageTemplate(group)
      first_name = $user_data[id][:first_name]
      last_name = $user_data[id][:last_name]
      position = $user_data[id][:position]
      total_pubs = $user_data[id][:total_pubs]
      new_pubs = $user_data[id][:new_pubs]
      need_upload = $user_data[id][:need_upload]
      need_grants = $user_data[id][:need_grants]
      upload_titles = $user_data[id][:upload_titles]
      grant_titles = $user_data[id][:grant_titles]
      pending_titles = $user_data[id][:pending_titles]
      logged_in = $user_data[id][:logged_in]
      not_participating = $user_data[id][:not_participating]
      pub_plural = (new_pubs==1 ? "publication" : "publications")
      pub_verb = (new_pubs==1 ? "appears" : "appear")
      pub_article = (new_pubs==1 ? "This" : "These")
      pub_article_lower = (new_pubs==1 ? "this" : "these")
      upload_plural = (need_upload==1 ? "publication" : "publications")
      to = $user_data[id][:email]
      is_welcome = $user_data[id][:is_welcome]
      body = ERB.new($template).result(binding)
      sendEmail($from,to,$subject,body)

      if ($args[:mode] == 'send')
        puts "updating email last sent"
        $wel.UpdateInfo(id,to,is_welcome)
      end
    end
  else
    puts "No compound messages to send"
  end
end

def sendNotification()
  if (!$need_to_claim.empty?)
    $need_to_claim.each do |id|
      group = $user_data[id][:group]
      chooseMessageFrom(group)
      chooseMessageSubject(group)
      chooseMessageTemplate(group)
      first_name = $user_data[id][:first_name]
      last_name = $user_data[id][:last_name]
      position = $user_data[id][:position]
      total_pubs = $user_data[id][:total_pubs]
      new_pubs = $user_data[id][:new_pubs]
      plural = (new_pubs==1 ? "publication" : "publications")
      conjunction = (new_pubs==1 ? "is" : "are")
      to = $user_data[id][:email]
      body = ERB.new($template).result(binding)
      sendEmail($from,to,$subject,body)
    end
  else
    puts "No notifications to send"
  end
end

def sendUploadReminder()
  if (!$need_to_upload.empty?)
    $need_to_upload.each do |id|
      group = $user_data[id][:group]
      chooseMessageFrom(group)
      chooseMessageSubject(group)
      chooseMessageTemplate(group)
      first_name = $user_data[id][:first_name]
      last_name = $user_data[id][:last_name]
      position = $user_data[id][:position]
      need_upload = $user_data[id][:need_upload]
      plural = (need_upload==1 ? "publication" : "publications")
      conjunction = (need_upload==1 ? "is" : "are")
      to = $user_data[id][:email]
      body = ERB.new($template).result(binding)
      sendEmail($from,to,$subject,body)
    end
  else
    puts "No reminders to send"
  end
end

def sendEmail(from,to,subject,body)

  if $bouncing.include? to or $donotsend.include? to
    puts to
    puts 'SKIPPING emails in bounce or do not send list'
    return
  end

  mail_options = { :address              => "email-smtp.com",
                   :port                 => 587,
                   :domain               => "emailer-domain.org",
                   :user_name            => "#{$smtp_username}",
                   :password             => "#{$smtp_password}",
                   :authentication       => "plain",
                   :enable_starttls_auto => true  }

  # create email
  mail = Mail.new do
    content_type "text/html; charset=UTF-8"
    from    "#{from}"
    if ($args[:mode] == 'send')
      to      "#{to}"
    elsif ($args[:mode] == 'sample')
      # For sending a sample
	    to      'Sample Test <sample@test.org>'
    end
    subject "#{$subject}"
    body    "#{body}"
  end

  puts "\n----------------------------------------\nsending message to #{to}\n----------------------------------------\n"
  puts "\n#{mail.to_s}"

  # Do not actually send messages if in test mode
  if ($args[:mode] == 'send' or $args[:mode] == 'sample')
    # for local testing
    # mail.delivery_method :sendmail
    mail.delivery_method :smtp, mail_options
    # send message
    mail.deliver
    # sleep for 1/2 a second to keep the rate at around 2 messages per second
    sleep(0.5)
  end
end

# Main Loop
begin

  # parse command line args
  parseArgs()

  # read credentials
  creds = File.read("creds.json")
  parsed_creds = JSON.parse(creds)
  db_username = parsed_creds["db_username"]
  db_password = parsed_creds["db_password"]
  $smtp_username = parsed_creds["smtp_username"]
  $smtp_password = parsed_creds["smtp_password"]

  sql = File.read("user_report.sql")

  if ($args[:mode] == 'send')
    puts "\n---------- SEND MODE ----------\n"
    puts "\n---------- THIS IS NOT A TEST!!!! ----------\n"
  else
    puts "\n---------- TEST MODE ----------\n"
  end

  # open connection to reporting DB
  puts "Connecting to reporting DB"
  client = TinyTds::Client.new(username: "#{db_username}", password: "#{db_password}",
                               host: "oapolicy.hostname.org",
                               database: "elements-reporting-dbname",
                               timeout: "1800")

  # run query
  puts "Running DB query"
  results = client.execute(sql)

  # Arrays and hashes
  # Martin had a cleaner way to do this
  $no_login = Array.new
  $total_pending = Array.new
  $need_to_upload = Array.new
  $need_to_claim = Array.new
  $need_to_add_grant = Array.new
  $need_action = Array.new
  $need_to_log_in = Array.new
  $user_data = Hash.new

  # loading up arrays and hashes
  puts "Processing DB query results"
  results.each do |row|

    user_id = row['ID']
    first_name = row['First Name']
    last_name = row['Last Name']
    group = row['Primary Group']

    puts "\nName: #{last_name}, #{first_name} (#{user_id})"
    puts "Group: #{group}"

    pending = row['Pending Publications']
    pending_new = row['New Pending Publications']
    oa_completed = row['Completed OA Publications'].to_i + row['OA Policy Exception Publications'].to_i
    logged_in = row['Last Login'].nil? ? "no" : "yes"
    raw_titles = row['OA Titles Needing Upload']
    raw_grants = row['OA Titles Without Grants']
    raw_pending = row['Titles Pending']
    # getting rid of extra characters
    oa_titles_needing_upload = raw_titles.to_s.gsub(/\|\|$/,'')
    oa_titles_without_grants = raw_grants.to_s.gsub(/\|\|$/,'')
    titles_pending = raw_pending.to_s.gsub(/\|\|$/,'')

    # create lists of user_ids that need some sort of messaging
    if row['Last Login'].nil?
      puts "    This user has never logged in."
      $no_login << user_id
    end
    # CHANGE to 0
    if pending > 0
      puts "    This user has #{pending.to_s} pending publications"
      $total_pending << user_id
    end
    # CHANGE to 0
    if pending_new > 0
      puts "    This user has #{pending_new.to_s} new pending publications"
      $need_to_claim << user_id
    end
    # By Group
    if $args[:group] == 'rgpo'
      oa_need_grants = row['RGPO Claimed OA Pubs without Grant Links']
      oa_need_upload = row['RGPO OA Publications Needing Upload']
      # CHANGE to 0
      if oa_need_upload > 0
        puts "    This user has #{oa_need_upload.to_s} publications requiring upload"
        $need_to_upload << user_id
      end
      # CHANGE to 0
      if oa_need_grants > 0
        puts "    This user has #{oa_need_grants.to_s} publications requiring a grant link"
        $need_to_add_grant << user_id
      end
    elsif $args[:group] == 'lbl'
        oa_need_grants = row['LBL Claimed OA Pubs without Grant Links']
        oa_need_upload = row['LBL OA Publications Needing Upload']
        # CHANGE to 0
        if oa_need_upload > 0
          puts "    This user has #{oa_need_upload.to_s} publications requiring upload"
          $need_to_upload << user_id
        end
        # CHANGE to 0
        if oa_need_grants > 0
          puts "    This user has #{oa_need_grants.to_s} publications requiring a grant link"
          $need_to_add_grant << user_id
        end
        # CHANGE to 0
        if pending > 0 and oa_need_upload == 0 and oa_completed == 0 and logged_in == 'no'
          puts "    This user has #{pending.to_s} pending publications, but has never logged in or claimed"
          $need_to_log_in << user_id
          not_participating = "yes"
        end
    end

    if ($need_to_claim.include? user_id or $need_to_upload.include? user_id or $need_to_add_grant.include? user_id or $need_to_log_in.include? user_id)
      puts "    Email will be sent"
    else
      puts "    No message necessary"
    end

    isWelcome = $wel.IsWelcome(user_id);
    $user_data[user_id]={first_name: row['First Name'],
                         last_name: row['Last Name'],
                         position: row['Position'],
                         group: row['Primary Group'],
                         email: row['Email'],
                         total_pubs: pending,
                         new_pubs: pending_new,
                         need_upload: oa_need_upload,
                         need_grants: oa_need_grants,
                         upload_titles: oa_titles_needing_upload,
                         grant_titles: oa_titles_without_grants,
                         pending_titles: titles_pending,
                         logged_in: logged_in,
                         not_participating: not_participating,
                         is_welcome: isWelcome}

  end

  # For compound messages
  if ($args[:mode] == 'send' or $args[:mode] == 'test')
    $need_action = ($need_to_claim + $need_to_upload + $need_to_add_grant + $need_to_log_in).uniq
  elsif ($args[:mode] == 'sample')
    # For sending a sample
    # you can monkey with which array to use (e.g. $need_to_claim, $need_to_upload, etc.)
    $need_action.push($need_to_claim.first)
  end

  if $args[:message_type] == 'compound'
    # send compound messages
    puts "\n----------------------------------------\nSending #{$need_action.count.to_s} new compound messages\n----------------------------------------\n"
    sendCompound()
  elsif $args[:message_type] == 'notification'
    # send notifications
    puts "\n----------------------------------------\nSending #{$need_to_claim.count.to_s} new publication notifications\n----------------------------------------\n"
    sendNotification()
  elsif $args[:message_type] == 'reminder'
    # send reminders
    puts "\n----------------------------------------\nSending #{$need_to_upload.count.to_s} upload reminders\n----------------------------------------\n"
    sendUploadReminder()
  else
    puts "Unknown message type: #{$args[:message_type]}"
    exit!
  end

rescue Exception => e
  puts e.to_s
  puts e.backtrace
ensure
  puts "\n\nAll Messages Sent\n\n"
  $client.close if $client
end
