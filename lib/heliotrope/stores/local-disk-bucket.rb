require 'rubygems'
require 'digest/sha1'
require 'find'
require 'fastthread'

module Heliotrope
module Stores

## key-value store backed by local disk
class LocalDiskBucket
  def initialize dir
    @dir = dir
    @size = nil
    @mutex = Mutex.new # over all disk access, and @size changing
    Dir.mkdir @dir unless File.directory? @dir
  end

  def get key
    dir1, dir2, fn, key_fn = path_components_for key
    @mutex.synchronize do
      return nil unless File.exist? fn
      IO.read fn
    end
  end

  def has_key? key
    dir1, dir2, fn, key_fn = path_components_for key
    @mutex.synchronize { File.exist? fn }
  end
  alias contains? has_key?

  def put key, value
    dir1, dir2, fn, key_fn = path_components_for key

    @mutex.synchronize do
      Dir.mkdir dir1 unless File.directory? dir1
      Dir.mkdir dir2 unless File.directory? dir2
      File.open(fn, "w") { |f| f.write value }
      File.open(key_fn, "w") { |f| f.write key }
      @size += 1 if @size
    end

    value
  end

  def delete key
    dir1, dir2, fn, key_fn = path_components_for key
    @mutex.synchronize do
      if File.exist?(fn)
        File.delete fn
        File.delete key_fn
        @size -= 1 if @size
        key
      end
    end
  end

  def list
    ret = []
    Find.find(@dir) do |f|
      next unless File.file? f
      @mutex.synchronize { ret << IO.read(f) } if File.basename(f) =~ /^key-/
    end
    ret
  end

  def size; @mutex.synchronize { @size ||= list.size } end

  def path_components_for key
    hash = Digest::SHA1.hexdigest key
    dir1 = File.join @dir, hash[0 ... 2]
    dir2 = File.join dir1, hash[2 ... 4]
    fn = File.join dir2, hash
    key_fn = File.join dir2, "key-#{hash}"
    [dir1, dir2, fn, key_fn]
  end
  private :path_components_for
end

end
end

