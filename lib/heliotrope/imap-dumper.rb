# encoding: UTF-8
require "net/imap"
require 'json'
require 'timeout' # TODO: system timer for 1.8?

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
            name, val = astrings_data
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

      def astrings_data
        token = match(T_ATOM)
        name = token.value.upcase
        match(T_SPACE)
        return name, astrings_list
      end

      def astrings_list
        if @str.index(/\(([^)]*)\)/i, @pos)
          @pos = $~.end(0)
          return $1.scan(BEG_REGEXP).collect { |space, nilmatch, number, atom, quoted, lpar, rpar, bslash, star, lbra, rbra, literal, plus, percent, crlf, eof|
            if atom
              atom
            elsif quoted
              quoted
            elsif literal
              literal
            else
              nil
            end
          }.compact
        else
          parse_error("invalid astring list")
        end
      end
    end
  end
end

module Heliotrope
class IMAPDumper
  def can_provide_labels?; false end
  def imap_query_columns; %w(UID FLAGS BODY.PEEK[]) end

  def initialize opts
    %w(host port username password folder ssl).each do |x|
      v = opts[x.to_sym]
      raise ArgumentError, "need :#{x} option" if v.nil?
      instance_variable_set "@#{x}", v
    end
    @ids = nil
  end

  attr_reader :folder

  def load! state
    if state
      @last_added_uid = state["last_added_uid"]
      @last_uidvalidity = state["last_uidvalidity"]
    end
    get_ids! # sets @ids

    puts "; found #{@ids.size} unadded messages on server"
  end

  def skip! num
    @ids = @ids[num .. -1] || []
    @msgs = []
  end

  NUM_MESSAGES_PER_ITERATION = 50

  def each_message
    until done?
      get_more_messages! if @msgs.nil? || @msgs.empty? # sets @msgs
      break if @msgs.empty?

      body, labels, state, uid = @msgs.shift
      yield body, labels, state, uid
      @last_added_uid = uid
    end
  end

  def done?; @ids && @ids.empty? && @msgs && @msgs.empty? end

  def finish!
    state = { "last_added_uid" => @last_added_uid, "last_uidvalidity" => @last_uidvalidity }
    begin
      @imap.close if @imap
    rescue Net::IMAP::BadResponseError, SystemCallError
    end
    state
  end

private

  def get_ids!
    puts "; connecting to #{@host}:#{@port} (ssl: #{!!@ssl})..."
    begin
      @imap = Net::IMAP.new @host, :port => @port, :ssl => @ssl
    rescue TypeError
      ## 1.8 compatibility. sigh.
      @imap = Net::IMAP.new @host, @port, @ssl
    end
    puts "; login as #{@username} ..."
    @imap.login @username, @password

    @imap.examine folder

    @uidvalidity = @imap.responses["UIDVALIDITY"].first
    @uidnext = @imap.responses["UIDNEXT"].first

    @ids = if @uidvalidity == @last_uidvalidity
      puts "; found #{@uidnext - @last_added_uid - 1} new messages..."
      ((@last_added_uid + 1) .. (@uidnext - 1)).to_a
    else
      if @last_uidvalidity
        puts "; UID validity has changed! your server sucks. re-downloading all uids as punishment..."
      else
        puts "; awww, is this your first time? don't be shy now. downloading all uids..."
      end
      @imap.uid_search(["NOT", "DELETED"]) || []
    end

    @last_uidvalidity = @uidvalidity
  end

  def get_more_messages!
    if @ids.empty?
      @msgs = []
      return
    end

    imapdata = []
    while imapdata.empty? && !@ids.empty?
      ids = @ids.shift NUM_MESSAGES_PER_ITERATION
      query = ids.first .. ids.last
      puts "; requesting messages #{query.inspect} from imap server"
      startt = Time.now
      imapdata = begin
        Timeout.timeout(30) { @imap.uid_fetch(query, imap_query_columns) || [] }
      rescue Timeout::Error => e
        puts "warning: timeout. retrying"
        retry
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

      if state.member? "flagged"
        state -= ["flagged"]
        state += ["starred"]
      end

      ## it's a little funny to do this gmail-specific label parsing here, but
      ## i'm hoping that other imap servers might one day support this extension
      labels = (data.attr["X-GM-LABELS"] || []).map { |label| Net::IMAP.decode_utf7(label.to_s).downcase.gsub(/\\/, '').gsub(/\ /, '_') }
      if labels.member? "sent"
        labels -= ["sent"]
        state += ["sent"]
      end
      if labels.member? "starred"
        labels -= ["starred"]
        state += ["starred"]
      end
      labels -= ["important"] # fuck that noise

      body = data.attr["BODY[]"].gsub "\r\n", "\n"
      uid = data.attr["UID"]

      [body, labels, state, uid]
    end
  end
end
end
