#!/usr/bin/ruby

require 'mail'
require 'time'

mail_string = File.open('message.txt', 'r'){|f| f.read}
mail = Mail.read_from_string mail_string

a = mail[:received_spf]

puts a.decoded.class

puts mail.attachments.inspect
