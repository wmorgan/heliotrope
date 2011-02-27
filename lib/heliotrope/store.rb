require 'stringio'
require 'zlib'

module Heliotrope
class Store
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
    string
  end
end
end
