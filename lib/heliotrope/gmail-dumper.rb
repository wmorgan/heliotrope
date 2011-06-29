module Heliotrope
class GMailDumper < IMAPDumper
  GMAIL_HOST = "imap.gmail.com"
  GMAIL_PORT = 993

  def initialize opts
    super opts.merge(:host => GMAIL_HOST, :port => GMAIL_PORT, :ssl => true, :folder => "none")
  end

  def folder
    folders = @imap.xlist "", "*"
    allmail = folders.find { |x| x.attr.include? :Allmail }
    raise "can't find the all-mail folder" unless allmail
    allmail.name
  end

  def imap_query_columns
    %w(UID FLAGS X-GM-LABELS BODY.PEEK[])
  end

  ## we can figure out our own labels
  def can_provide_labels?; true end
end
end
