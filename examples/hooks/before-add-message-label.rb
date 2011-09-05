testlabels = labels

message.recipients.each { |person| if person.email =~ /cert-advisory@cert.org/; testlabels += [ "cert"]; testlabels -= ["inbox"]; break; end }

message.recipients.each { |person| if person.email =~ /(testuser@test.com|testuser2@test.com)/; testlabels += [ "testuser" ]; testlabels -= ["inbox"]; break; end }

if rawbody =~ /bugtraq.list-id.securityfocus.com/
  testlabels += [ "bugtraq"]
  testlabels -= ["inbox"]
  $stderr.puts "Rule list bugtraq - Archiving #{message.safe_msgid}, subject is '#{message.subject}'"
end

if rawbody =~ /X-Spam-Flag: YES/
  testlabels += [ "Spam"]
  testlabels -= ["inbox"]
  $stderr.puts "Rule spam - Archiving #{message.safe_msgid}, subject is '#{message.subject}'"
end

if message.from.name =~ /Cron Daemon/
  testlabels += [ "logs"]
  testlabels -= ["inbox"]
  $stderr.puts "Rule From Cron - Archiving #{message.safe_msgid}, from is '#{message.from}'"
end

if message.subject =~ /Cron/
  testlabels += [ "logs" ]
  testlabels -= ["inbox"]
  $stderr.puts "Rule Subject Cron - Archiving #{message.safe_msgid}, subject is '#{message.subject}'"
end

# Not actually allowed to set the label 'sent'
if message.from.email =~ /(testuser@test.com|testuser2@test.com)/
  testlabels += [ "sent" ]
  $stderr.puts "Rule Sent - #{message.safe_msgid}, from is '#{message.from.email}'"
end

return testlabels
