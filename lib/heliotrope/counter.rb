require 'rubygems'
require 'yaml'
require 'fastthread'
require 'heliotrope/util'

module Heliotrope

## a slow, disk-backed pessimistic counter class. saves state to disk before
## giving you a number.
class Counter
  def initialize fn
    @fn = fn
    @count = begin
      IO.read(fn).to_i
    rescue Errno::ENOENT => e
      1
    end
    @mutex = Mutex.new
  end

  ## count is the number of numbers you'd like
  def next count=1
    return unless count > 0
    @mutex.synchronize do
      r = (@count ... (@count += count)).to_a
      save!
      r
    end
  end

private

  def save!
    File.open(@fn, "w") { |f| f.puts @count }
  end
end

end
