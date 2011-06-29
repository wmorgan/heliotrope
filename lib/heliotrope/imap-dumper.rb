# encoding: UTF-8
require "net/imap"
require 'json'

# Monkeypatch Net::IMAP to support GMail IMAP extensions.
# http://code.google.com/apis/gmail/imap/
module Net
  class IMAP

    # Implement GMail XLIST command
    def xlist(refname, mailbox)
      synchronize do
        send_command("XLIST", refname, mailbox)
        return @responses.delete("XLIST")
      end
    end

    class ResponseParser
      def response_untagged
        match(T_STAR)
        match(T_SPACE)
        token = lookahead
        if token.symbol == T_NUMBER
          return numeric_response
        elsif token.symbol == T_ATOM
          case token.value
          when /\A(?:OK|NO|BAD|BYE|PREAUTH)\z/ni
            return response_cond
          when /\A(?:FLAGS)\z/ni
            return flags_response
          when /\A(?:LIST|LSUB|XLIST)\z/ni  # Added XLIST
            return list_response
          when /\A(?:QUOTA)\z/ni
            return getquota_response
          when /\A(?:QUOTAROOT)\z/ni
            return getquotaroot_response
          when /\A(?:ACL)\z/ni
            return getacl_response
          when /\A(?:SEARCH|SORT)\z/ni
            return search_response
          when /\A(?:THREAD)\z/ni
            return thread_response
          when /\A(?:STATUS)\z/ni
            return status_response
          when /\A(?:CAPABILITY)\z/ni
            return capability_response
          else
            return text_response
          end
        else
          parse_error("unexpected token %s", token.symbol)
        end
      end

      def response_tagged
        tag = atom
        match(T_SPACE)
        token = match(T_ATOM)
        name = token.value.upcase
        match(T_SPACE)
        #puts "AAAAAAAA  #{tag} #{name} #{resp_text} #{@str}"
        return TaggedResponse.new(tag, name, resp_text, @str)
      end

      def msg_att
        match(T_LPAR)
        attr = {}
        while true
          token = lookahead
          case token.symbol
          when T_RPAR
            shift_token
            break
          when T_SPACE
            shift_token
            token = lookahead
          end
          case token.value
          when /\A(?:ENVELOPE)\z/ni
            name, val = envelope_data
          when /\A(?:FLAGS)\z/ni
            name, val = flags_data
          when /\A(?:X-GM-LABELS)\z/ni  # Added X-GM-LABELS extension
            name, val = flags_data
          when /\A(?:INTERNALDATE)\z/ni
            name, val = internaldate_data
          when /\A(?:RFC822(?:\.HEADER|\.TEXT)?)\z/ni
            name, val = rfc822_text
          when /\A(?:RFC822\.SIZE)\z/ni
            name, val = rfc822_size
          when /\A(?:BODY(?:STRUCTURE)?)\z/ni
            name, val = body_data
          when /\A(?:UID)\z/ni
            name, val = uid_data
          when /\A(?:X-GM-MSGID)\z/ni  # Added X-GM-MSGID extension
            name, val = uid_data
          when /\A(?:X-GM-THRID)\z/ni  # Added X-GM-THRID extension
            name, val = uid_data
          else
            parse_error("unknown attribute `%s'", token.value)
          end
          attr[name] = val
        end
        return attr
      end
    end
  end
end

module Heliotrope
class IMAPDumper
  def can_provide_labels?; false end

  def initialize opts
    %w(host port username password state_fn folder ssl).each do |x|
      v = opts[x.to_sym]
      raise ArgumentError, "need #{x}" unless v
      instance_variable_set "@#{x}", v
    end
    @msgs = []
  end

  attr_reader :folder

  def save!
    return unless @last_added_uid && @last_uidvalidity

    File.open(@state_fn, "w") do |f|
      f.puts [@last_added_uid, @last_uidvalidity].to_json
    end
  end

  def load!
    @last_added_uid, @last_uidvalidity = begin
      JSON.parse IO.read(@state_fn)
    rescue SystemCallError => e
      nil
    end

    puts "; connecting to #{@host}:#{@port} (ssl: #{!!@ssl})..."
    @imap = Net::IMAP.new @host, @port, :ssl => @ssl
    puts "; login as #{@username} ..."
    @imap.login @username, @password

    @imap.examine folder

    @uidvalidity = @imap.responses["UIDVALIDITY"].first
    @uidnext = @imap.responses["UIDNEXT"].first

    @ids = if @uidvalidity == @last_uidvalidity
      puts "; found #{@uidnext - @last_added_uid} new messages..."
      ((@last_added_uid + 1) .. @uidnext).to_a
    else
      if @last_uidvalidity
        puts "; UID validity has CHANGED! your server sucks. downloading ALL uids..."
      else
        puts "; is this your first time, sweetie? downloading all uids..."
      end
      @imap.uid_search(["NOT", "DELETED"]) || []
    end

    @last_uidvalidity = @uidvalidity

    puts "; found #{@ids.size} messages to import"
  end

  def skip! num
    @ids = @ids[num .. -1] || []
    @msgs = []
  end

  NUM_MESSAGES_PER_ITERATION = 50

  def imap_query_columns
    %w(UID FLAGS BODY.PEEK[])
  end

  def next_message
    if @msgs.empty?
      imapdata = []
      while imapdata.empty?
        ids = @ids.shift NUM_MESSAGES_PER_ITERATION
        query = ids.first .. ids.last
        puts "; requesting messages #{query.inspect} from imap server"
        startt = Time.now
        imapdata = begin
          @imap.uid_fetch(query, imap_query_columns) || []
        rescue Net::IMAP::NoResponseError => e
          puts "warning: skipping messages #{query}: #{e.message}"
          []
        end
        elapsed = Time.now - startt
        puts "; got #{imapdata.size} messages"
        #printf "; the imap server loving gave us %d messages in %.1fs = a whopping %.1fm/s\n", imapdata.size, elapsed, imapdata.size / elapsed
      end

      @msgs = imapdata.map do |data|
        state = data.attr["FLAGS"].map { |flag| flag.to_s.downcase }
        if state.member? "seen"
          state -= ["seen"]
        else
          state += ["unread"]
        end

        ## it's a little funny to do this gmail-specific label parsing here, but
        ## i'm hoping that other imap servers might one day support this extension
        labels = (data.attr["X-GM-LABELS"] || []).map { |label| label.to_s.downcase }
        if labels.member? "sent"
          labels -= ["Sent"]
          state += ["sent"]
        end
        if labels.member? "starred"
          labels -= ["Starred"]
          state += ["starred"]
        end
        labels -= ["important"] # fuck that noise

        body = data.attr["BODY[]"].gsub "\r\n", "\n"
        uid = data.attr["UID"]

        [body, labels, state, uid]
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
