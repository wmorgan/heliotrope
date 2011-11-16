module Heliotrope

## a single mbox message on a stream
class MBoxStream
  def initialize stream
    @stream = stream
  end

  def next_message
    @stream.read
  end

  def cur_message; @stream end

  def done?; @stream.eof? end
  def finish!; end
  def load!; end
end

## a custom mbox splitter / from line detector. rmail has one, but it splits on
## occurrences of "From " in text lines too. we try and be a little smarter
## here and validate the format somewhat.
class MboxSplitter
  BREAK_RE = /^From \S+ .+\d\d\d\d$/

  def initialize filename, opts={}
    @stream = File.open filename, "r:BINARY"
    @stream.seek opts[:start_offset] if opts[:start_offset]
  end

  def can_provide_labels?; false end
  def load!; end # nothing to do
  def offset; @stream.tell end

  def next_message
    message = ""
    start_offset = @stream.tell
    while message.empty? && !@stream.eof?
      @stream.each_line do |l|
        break if is_mbox_break_line?(l) || l.nil?
        message << l
      end
    end
    [message, [], [], start_offset]
  end

  def skip! num
    num.times { next_message } # lame
  end

  def eof?; @stream.eof? end
  def done?; eof? end
  def finish!
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
