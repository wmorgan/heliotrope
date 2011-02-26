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

    puts "; connecting to #{host}:#{port}..."
    @imap = Net::IMAP.new host, port, :ssl => ssl
    puts "; logging in as #{username}..."
    @imap.login username, password
    puts "; selecting #{folder}..."
    @imap.select folder
    puts "; ready!"
    @ids = nil
    @msgs = []
  end

  BUF_SIZE = 100

  def next_message
    @ids ||= begin
      puts "; downloading message ids..."
      ids = @imap.search(["NOT", "DELETED"])
      puts "; got #{ids.size}"
      ids
    end

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
