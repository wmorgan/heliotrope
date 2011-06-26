module Heliotrope
class GMailDumper < IMAPDumper
  GMAIL_HOST = "imap.gmail.com"
  GMAIL_PORT = 993
  GMAIL_FOLDER = "[Gmail]/All Mail"

  def initialize opts
    super opts.merge(:host => GMAIL_HOST, :port => GMAIL_PORT, :ssl => true, :folder => GMAIL_FOLDER)
  end

  def imap_query_columns
    %w(UID FLAGS X-GM-LABELS BODY.PEEK[])
  end

  ## we can figure out our own labels
  def can_provide_labels?; true end
end
end
