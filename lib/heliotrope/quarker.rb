require 'rubygems'
require 'fastthread'

module Heliotrope

## a slow, disk-backed pessimistic string-to-integer mapping. saves state to
## disk before giving you a mapping.
class Quark
  def initialize fn
    @fn = fn
    @array = begin
      IO.readlines(fn).map { |l| l.chomp }
    rescue Errno::ENOENT => e
      []
    end
    @hash = Hash[*@array.zip((0 ... @array.size).to_a).flatten]
    @mutex = Mutex.new
  end

  def strings_to_ints names
    @mutex.synchronize do
      start = @array.size
      ret = names.map do |name|
        @hash[name] || begin
          @array << name
          @hash[name] = @array.size - 1
        end
      end

      if start != @array.size
        append @array[start .. -1]
      end

      ret
    end
  end

  def ints_to_strings ints
    ints.map do |i|
      @mutex.synchronize { @array[i] } or raise ArgumentError, "Unknown quark #{i} in array #{@array[0...10].inspect} (total size #{@array.size})"
    end
  end

private

  def append words
    File.open(@fn, "a") { |f| f.puts words }
  end
end

class Quarker
  def initialize fn_base
    @fn_base = fn_base
    @quarks = Dir["#{@fn_base}.*"].inject({}) do |h, fn|
      fn =~ /#{@fn_base}\.(\S+)$/
      h[$1] = Quark.new fn
      h
    end
    @mutex = Mutex.new
  end

  def ints_to_strings domain, ints
    quark_for(domain).ints_to_strings ints
  end

  def strings_to_ints domain, strings
    quark_for(domain).strings_to_ints strings
  end

  def int_to_string domain, int; ints_to_strings(domain, [int]).first end
  def string_to_int domain, string; strings_to_ints(domain, [string]).first end

private

  def quark_for domain
     @mutex.synchronize { @quarks[domain] ||= Quark.new "#{@fn_base}.#{domain}" }
  end
end

end

