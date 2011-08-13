# encoding: UTF-8

require 'stringio'
require 'zlib'

## a simple mbox with compressed messages

module Heliotrope
class ZMBox
  def initialize fn
    @io = File.open fn, "a+:BINARY"
  end

  def add string
    buf = StringIO.new
    zbuf = Zlib::GzipWriter.new buf
    zbuf.write string
    zbuf.close

    @io.seek 0, IO::SEEK_END
    offset = @io.tell
    @io.write [buf.string.bytesize].pack("L")
    @io.write buf.string
    @io.flush

    #printf "; compressed %dk => %dk (%.0f%% compression rate) and wrote at offset %d\n", string.bytesize / 1024, buf.string.bytesize / 1024, 100.0 - (100.0 * buf.string.bytesize / rawbody.bytesize), offset
    offset
  end

  def read offset
    @io.seek offset
    size = @io.read(4).unpack("L").first
    buf = StringIO.new @io.read(size)
    z = Zlib::GzipReader.new(buf)
    string = z.read
    z.close

    ## these come back in the system encoding. GzipReader doesn't seem to take
    ## an encoding spec. sigh. they need to be ascii.
    string.force_encoding(Encoding::BINARY) if Decoder.in_ruby19_hell?
    string
  end
end
end
