# encoding: UTF-8

module Heliotrope
class Person

  AT_RE = "\s+(?:@|at|AT)\s+"

  def initialize name, email, handle
    @name = name
    @email = email
    @handle = handle
  end

  attr_reader :name, :email, :handle

  def to_email_address
    qname = name =~ /"/ ? name.inspect : name
    [qname, "<#{email}>"].compact.join(" ")
  end

  def display_name; name || handle || email end

  ## takes a string, returns a [name, email, emailnodomain] combo
  ## e.g. for William Morgan <wmorgan@example.com>, returns
  ##  ["William Morgan", wmorgan@example.com, wmorgan]
  def self.from_string string # ripped from sup
    return if string.nil? || string.empty?

    name, email, handle = case string
    when /^(["'])(.*?[^\\])\1\s*<((\S+?)#{AT_RE}\S+?)>/
      a, b, c = $2, $3, $4
      a = a.gsub(/\\(["'])/, '\1')
      [a, b, c]
    when /(.+?)\s*<((\S+?)#{AT_RE}\S+?)>/
      [$1, $2, $3]
    when /<((\S+?)#{AT_RE}\S+?)>/
      [nil, $1, $2]
    when /((\S+?)#{AT_RE}\S+)/
      [nil, $1, $2]
    when /((\S+?)#{AT_RE}(?:\S+)?)\s+\((\D+?)\)/
      [$3, $1, $2]
    else
      [nil, string, nil] # i guess...
    end

    Person.new name, email, handle
  end

  def self.many_from_string string
    return [] if string.nil? || string !~ /\S/
    emails = string.gsub(/[\t\r\n]+/, " ").split(/,\s*(?=(?:[^"]*"[^"]*")*(?![^"]*"))/)
    emails.map { |e| from_string e }.compact
  end

  def indexable_text; [name, email, handle].join(" ") end
end
end
