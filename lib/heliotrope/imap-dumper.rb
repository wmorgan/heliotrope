# encoding: UTF-8
require "net/imap"
require 'json'

module Heliotrope
class ImapDumper
  def initialize opts
    @host = opts[:host] or raise ArgumentError, "need :host"
    @username = opts[:username] or raise ArgumentError, "need :username"
    @password = opts[:password] or raise ArgumentError, "need :password"
    @fn = opts[:fn] or raise ArgumentError, "need :fn"

    @ssl = opts.member?(:ssl) ? opts[:ssl] : true
    @port = opts[:port] || (ssl ? 993 : 143)
    @folder = opts[:folder] || "inbox"

    @msgs = []
  end

  def save!
    return unless @last_added_uid && @last_uidvalidity

    File.open(@fn, "w") do |f|
      f.puts [@last_added_uid, @last_uidvalidity].to_json
    end
  end

  def load!
    @last_added_uid, @last_uidvalidity = begin
      JSON.parse IO.read(@fn)
    rescue SystemCallError => e
      nil
    end

    puts "; connecting..."
    @imap = Net::IMAP.new @host, @port, :ssl => @ssl
    puts "; login as #{@username} ..."
    @imap.login @username, @password
    @imap.examine @folder

    @uidvalidity = @imap.responses["UIDVALIDITY"].first
    @uidnext = @imap.responses["UIDNEXT"].first

    @ids = if @uidvalidity == @last_uidvalidity
      puts "; found #{@uidnext - @last_added_uid} new messages..."
      ((@last_added_uid + 1) .. @uidnext).to_a
    else
      puts "; rescanning everything..."
      @imap.uid_search(["NOT", "DELETED"]) || []
    end

    @last_uidvalidity = @uidvalidity
    puts "; found #{@ids.size} messages to scan"
  end

  def skip! num
    @ids = @ids[num .. -1] || []
    @msgs = []
  end

  NUM_MESSAGES_PER_ITERATION = 50

  def next_message
    if @msgs.empty?
      imapdata = []
      while imapdata.empty?
        ids = @ids.shift NUM_MESSAGES_PER_ITERATION
        query = ids.first .. ids.last
        puts "; requesting messages #{query.inspect} from imap server"
        startt = Time.now
        imapdata = @imap.uid_fetch query, ["UID", "FLAGS", "BODY.PEEK[]"]
        elapsed = Time.now - startt
        printf "; gmail loving gave us %d messages in %.1fs = a whopping %.1fm/s\n", imapdata.size, elapsed, imapdata.size / elapsed
      end

      @msgs = imapdata.map do |data|
        state = data.attr["FLAGS"].map { |flag| flag.to_s.downcase }
        if state.member? "seen"
          state -= ["seen"]
        else
          state += ["unread"]
        end

        body = data.attr["BODY[]"].gsub "\r\n", "\n"
        uid = data.attr["UID"]

        [body, [], state, uid]
      end
    end

    body, labels, state, uid = @msgs.shift
    @last_added_uid = @prev_uid || @last_added_uid
    @prev_uid = uid

    [body, labels, state, uid]
  end

  def done?; @ids && @ids.empty? && @msgs.empty? end
  def finish!
    begin
      save!
      @imap.close if @imap
    rescue Net::IMAP::BadResponseError, SystemCallError
    end
  end
end
end
