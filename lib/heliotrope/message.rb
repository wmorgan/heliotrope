# encoding: UTF-8

require 'rmail'
require 'open3'
require 'digest/md5'
require 'json'

module Heliotrope
class InvalidMessageError < StandardError; end
class Message
  def initialize rawbody
    @rawbody = rawbody
    @mime_parts = {}
  end

  def parse!
    @m = RMail::Parser.read @rawbody

    @msgid = find_msgids(decode_header(validate_field(:message_id, @m.header["message-id"]))).first
    ## this next error happens if we have a field, but we can't find a <something> in it
    raise InvalidMessageError, "can't parse msgid: #{@m.header['message-id']}" unless @msgid
    @safe_msgid = munge_msgid @msgid

    @from = Person.from_string decode_header(validate_field(:from, @m.header["from"]))
    @date = begin
      Time.parse(validate_field(:date, @m.header["date"])).to_i
    rescue ArgumentError
      #puts "warning: invalid date field #{@m.header['date']}"
      Time.at 0
    end

    @to = Person.many_from_string decode_header(@m.header["to"])
    @cc = Person.many_from_string decode_header(@m.header["cc"])
    @bcc = Person.many_from_string decode_header(@m.header["bcc"])
    @subject = decode_header @m.header["subject"]
    @reply_to = Person.from_string @m.header["reply-to"]

    @refs = find_msgids decode_header(@m.header["references"] || "")
    in_reply_to = find_msgids decode_header(@m.header["in-reply-to"] || "")
    @refs += in_reply_to unless @refs.member? in_reply_to.first
    @safe_refs = @refs.map { |r| munge_msgid(r) }

    ## various other headers that you don't think we will need until we
    ## actually need them.

    ## this is sometimes useful for determining who was the actual target of
    ## the email, in the case that someone has aliases
    @recipient_email = @m.header["envelope-to"] || @m.header["x-original-to"] || @m.header["delivered-to"]

    @list_subscribe = @m.header["list-subscribe"]
    @list_unsubscribe = @m.header["list-unsubscribe"]
    @list_post = @m.header["list-post"] || @m.header["x-mailing-list"]

    self
  end

  attr_reader :msgid, :from, :to, :cc, :bcc, :subject, :date, :refs, :recipient_email, :list_post, :list_unsubscribe, :list_subscribe, :reply_to, :safe_msgid, :safe_refs

  ## we don't encode any non-text parts here, because json encoding of
  ## binary objects is crazy-talk, and because those are likely to be
  ## big anyways.
  def to_h message_id, preferred_type
    parts = mime_parts(preferred_type).map do |type, fn, cid, content, size|
      if type =~ /^text\//
        { :type => type, :filename => fn, :cid => cid, :content => content, :here => true }
      else
        { :type => type, :filename => fn, :cid => cid, :size => content.size, :here => false }
      end
    end.compact

    { :from => (from ? from.to_email_address : ""),
      :to => to.map(&:to_email_address),
      :cc => cc.map(&:to_email_address),
      :bcc => bcc.map(&:to_email_address),
      :subject => subject,
      :date => date,
      :refs => refs,
      :parts => parts,
      :message_id => message_id,
      :snippet => snippet,
      :reply_to => (reply_to ? reply_to.to_email_address : ""),

      :recipient_email => recipient_email,
      :list_post => list_post,
      :list_subscribe => list_subscribe,
      :list_unsubscribe => list_unsubscribe,

      :email_message_id => @msgid,
    }
  end

  def direct_recipients; to end
  def indirect_recipients; cc + bcc end
  def recipients; direct_recipients + indirect_recipients end

  def indexable_text
    @indexable_text ||= begin
      v = ([from.indexable_text] +
        recipients.map { |r| r.indexable_text } +
        [subject] +
        mime_parts("text/plain").map do |type, fn, id, content|
          if fn
            fn
          elsif type =~ /text\//
            content
          end
        end
      ).flatten.compact.join(" ")

      v.gsub(/\s+[\W\d_]+(\s|$)/, " "). # drop funny tokens
        gsub(/\s+/, " ")
    end
  end

  SIGNED_MIME_TYPE = %r{multipart/signed;.*protocol="?application/pgp-signature"?}m
  ENCRYPTED_MIME_TYPE = %r{multipart/encrypted;.*protocol="?application/pgp-encrypted"?}m
  SIGNATURE_ATTACHMENT_TYPE = %r{application\/pgp-signature\b}

  def snippet
    mime_parts("text/plain").each do |type, fn, id, content|
      if (type =~ /text\//) && fn.nil?
        head = content[0, 1000].split "\n"
        head.shift while !head.empty? && head.first.empty? || head.first =~ /^\s*>|\-\-\-|(wrote|said):\s*$/
        snippet = head.join(" ").gsub(/^\s+/, "").gsub(/\s+/, " ")[0, 100]
        return snippet
      end
    end
    ""
  end

  def has_attachment?
    @has_attachment ||=
      mime_parts("text/plain").any? do |type, fn, id, content|
        fn && (type !~ SIGNATURE_ATTACHMENT_TYPE)
    end
  end

  def signed?
    @signed ||= mime_part_types.any? { |t| t =~ SIGNED_MIME_TYPE }
  end

  def encrypted?
    @encrypted ||= mime_part_types.any? { |t| t =~ ENCRYPTED_MIME_TYPE }
  end

  def mime_parts preferred_type
    @mime_parts[preferred_type] ||= decode_mime_parts @m, preferred_type
  end

private

  ## hash the fuck out of all message ids. trust me, you want this.
  def munge_msgid msgid
    Digest::MD5.hexdigest msgid
  end

  def find_msgids msgids
    msgids.scan(/<(.+?)>/).map(&:first)
  end

  def mime_part_types part=@m
    ptype = part.header["content-type"] || ""
    [ptype] + (part.multipart? ? part.body.map { |sub| mime_part_types sub } : [])
  end

  ## unnests all the mime stuff and returns a list of [type, filename, content]
  ## tuples.
  ##
  ## for multipart/alternative parts, will only return the subpart that matches
  ## preferred_type. if none of them, will only return the first subpart.
  def decode_mime_parts part, preferred_type, level=0
    if part.multipart?
      if mime_type_for(part) =~ /multipart\/alternative/
        target = part.body.find { |p| mime_type_for(p).index(preferred_type) } || part.body.first
        if target # this can be nil
          decode_mime_parts target, preferred_type, level + 1
        else
          []
        end
      else # decode 'em all
        part.body.compact.map { |subpart| decode_mime_parts subpart, preferred_type, level + 1 }.flatten 1
      end
    else
      type = mime_type_for part
      filename = mime_filename_for part
      id = mime_id_for part
      content = mime_content_for part, preferred_type
      [[type, filename, id, content]]
    end
  end

private

  def validate_field what, thing
    raise InvalidMessageError, "missing '#{what}' header" if thing.nil?
    thing = thing.to_s.strip
    raise InvalidMessageError, "blank '#{what}' header: #{thing.inspect}" if thing.empty?
    thing
  end

  def mime_type_for part
    (part.header["content-type"] || "text/plain").gsub(/\s+/, " ").strip.downcase
  end

  def mime_id_for part
    header = part.header["content-id"]
    case header
      when /<(.+?)>/; $1
      else header
    end
  end

  ## a filename, or nil
  def mime_filename_for part
    cd = part.header["Content-Disposition"]
    ct = part.header["Content-Type"]

    ## RFC 2183 (Content-Disposition) specifies that disposition-parms are
    ## separated by ";". So, we match everything up to " and ; (if present).
    filename = if ct && ct =~ /name="?(.*?[^\\])("|;|\z)/im # find in content-type
      $1
    elsif cd && cd =~ /filename="?(.*?[^\\])("|;|\z)/m # find in content-disposition
      $1
    end

    ## filename could be RFC2047 encoded
    decode_header(filename).chomp if filename
  end

  ## rfc2047-decode a header, convert to utf-8, and normalize whitespace
  def decode_header v
    return "" if v.nil?

    v = if Decoder.is_rfc2047_encoded? v
      Decoder.decode_rfc2047 "utf-8", v
    else # assume it's ascii and transcode
      Decoder.transcode "utf-8", "ascii", v
    end

    v.gsub(/\s+/, " ").strip
  end

  CONVERSIONS = {
    ["text/html", "text/plain"] => :html_to_text
  }

  ## the content of a mime part itself. if the content-type is text/*,
  ## it will be converted to utf8. otherwise, it will be left in the
  ## original encoding
  def mime_content_for mime_part, preferred_type
    return "" unless mime_part.body # sometimes this happens. not sure why.

    mt = mime_type_for(mime_part) || "text/plain" # i guess
    content_type = if mt =~ /^(.+);/ then $1.downcase else mt end
    source_charset = if mt =~ /charset="?(.*?)"?(;|$)/i then $1 else "US-ASCII" end

    content = mime_part.decode
    converted_content, converted_charset = if(converter = CONVERSIONS[[content_type, preferred_type]])
      send converter, content, source_charset
    else
      [content, source_charset]
    end

    if content_type =~ /^text\//
      Decoder.transcode "utf-8", converted_charset, converted_content
    else
      converted_content
    end
  end

  require 'locale'
  SYSTEM_CHARSET = Locale.current.charset
  HTML_CONVERSION_CMD = "html2text"
  def html_to_text html, charset
    ## ignore charset. html2text produces output in the system charset.
    #puts "; forced to decode html. running #{HTML_CONVERSION_CMD} on #{html.size}b mime part..."
    content = Open3.popen3(HTML_CONVERSION_CMD) do |inn, out, err|
      inn.print html
      inn.close
      out.read
    end
    [content, SYSTEM_CHARSET]
  end
end
end
