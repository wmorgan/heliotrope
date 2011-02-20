require 'time'

## a custom mbox splitter / from line detector. rmail has one, but it splits on
## occurrences of "From " in text lines too. we try and be a little smarter
## here and validate the format somewhat.
module Heliotrope
class MboxSplitter
  BREAK_RE = /^From \S+ .+ \d\d\d\d$/

  def initialize stream
    @stream = stream
  end

  def is_mbox_break_line? l
    l[0, 5] == "From " or return false
    l =~ BREAK_RE or return false
    return true

    ## this next check slows us down so i'm skipping it for now

    time = $1
    begin
      puts "parsing: #{time}"
      ## hack -- make Time.parse fail when trying to substitute values from Time.now
      Time.parse time, 0
      true
    rescue NoMethodError, ArgumentError
      puts "# skipping false positive From line #{l.inspect}"
      false
    end
  end

  def next_message
    message = ""
    start_offset = @stream.tell
    @stream.each_line do |l|
      break if is_mbox_break_line?(l)
      message << l
    end
    [start_offset, message]
  end

  def eof?; @stream.eof? end

  def message_at offset
    @stream.seek offset
    offset, message = next_message
    message
  end
end
end
