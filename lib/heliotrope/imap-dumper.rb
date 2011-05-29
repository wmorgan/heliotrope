# encoding: UTF-8

require 'net/imap'

module Heliotrope
class IMAPDumper
  def initialize opts
    host = opts[:host] or raise ArgumentError, "need :host"
    username = opts[:username] or raise ArgumentError, "need :username"
    password = opts[:password] or raise ArgumentError, "need :password"

    ssl = opts.member?(:ssl) ? opts[:ssl] : true
    port = opts[:port] || (ssl ? 993 : 143)
    folder = opts[:folder] || "inbox"
  end

  def cur_message; "an imap message" end # lame

  BUF_SIZE = 100

  def load!
    puts "; connecting to #{host}:#{port}..."
    @imap = Net::IMAP.new host, port, :ssl => ssl
    puts "; logging in as #{username}..."
    @imap.login username, password
    puts "; selecting #{folder}..."
    @imap.select folder
    puts "; downloading message ids..."
    @ids = @imap.search(["NOT", "DELETED"])
    puts "; got #{@ids.size}"

    @msgs = []
  end

  def skip! num
    @ids = @ids[num .. -1]
    @msgs = []
  end

  def next_message
    if @msgs.empty?
      ids = @ids.shift BUF_SIZE
      query = ids.first .. ids.last
      puts "; requesting messages #{query.inspect} from imap server"
      startt = Time.now
      @msgs = @imap.fetch(query, "RFC822")
      elapsed = Time.now - startt
      @msgs = @msgs.map do |m|
        body = m.attr["RFC822"]
        body.force_encoding("binary") if body.respond_to?(:force_encoding)
        body.gsub("\r\n", "\n")
      end
      printf "; the imap server loving gave us #{@msgs.size} messages in %.1fs = a whopping %.1fm/s\n", elapsed, @msgs.size / elapsed
    end

    @msgs.shift
  end

  def done?; @ids && @ids.empty? && @msgs.empty? end
  def finish!; @imap.close end
end
end
