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
  def load! state; end
end

## a custom mbox splitter / from line detector. rmail has one, but it splits on
## occurrences of "From " in text lines too. we try and be a little smarter
## here and validate the format somewhat.
class MboxSplitter
  BREAK_RE = /^From \S+ .+\d\d\d\d$/

  def initialize filename
    @stream = File.open filename, "r:BINARY"
    @last_offset = 0
  end

  def can_provide_labels?; false end
  def load! state
    @last_offset = state["last_offset"] if state
  end
  
  def each_message
    until done?
      message = ""
      @stream.seek @last_offset
      while message.empty? && !@stream.eof?
        @stream.each_line do |l|
          break if is_mbox_break_line?(l) || l.nil?
          message << l
        end
      end
      yield message, [], [], @last_offset
      @last_offset = @stream.tell
    end
  end

  def skip! num
    count = 0
    each_message do |*a|
      count += 1
      break if count > num
    end
  end

  def done?; @stream.eof? end
  def finish!
    @stream.close
    { "last_offset" => @last_offset }
  end

private

  ## total hack. but all such things are.
  def is_mbox_break_line? l
    l[0, 5] == "From " or return false # quick check
    l =~ BREAK_RE or return false # longer check
    true
  end
end
end
