## a custom mbox splitter / from line detector. rmail has one, but it splits on
## occurrences of "From " in text lines too. we try and be a little smarter
## here and validate the format somewhat.
module Heliotrope
class MBoxStream
  def initialize stream
    @stream = stream
  end

  def next_message
    @stream.read
  end

  def done?; @stream.eof? end
  def finish!; end
end

class MboxSplitter
  BREAK_RE = /^From \S+ .+\d\d\d\d$/

  def initialize filename, opts={}
    @stream = File.open filename, "r:BINARY"
    @stream.seek opts[:start_offset] if opts[:start_offset]
  end

  def next_message
    message = ""
    while message.empty?
      @stream.each_line do |l|
        break if is_mbox_break_line?(l)
        message << l
      end
    end
    message
  end

  def eof?; @stream.eof? end
  def done?; eof? end
  def finish!
    puts "end offset is #{@stream.tell}"
    @stream.close
  end

  def message_at offset
    @stream.seek offset
    offset, message = next_message
    message
  end

private

  ## total hack. but all such things are.
  def is_mbox_break_line? l
    l[0, 5] == "From " or return false
    l =~ BREAK_RE or return false
    true
  end
end
end
