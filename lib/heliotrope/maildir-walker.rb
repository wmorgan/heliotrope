module Heliotrope
class MaildirWalker
  def initialize(*dirs)
    @dirs = dirs
    @files = nil
  end

  def next_message
    @files ||= get_files
    return nil if @files.empty?
    IO.read(@files.shift)
  end

  def done?
    @files ||= get_files
    @files.empty?
  end

  def finish!; end

private

  def get_files
    puts "; scanning #{@dirs.size} directories..."
    dirs = @dirs.map { |d| d.gsub(/([\*\?\[\]])/, '\\\\\1') } # have to escape for globbing
    files = dirs.map { |dir| Dir[File.join(dir, "cur", "*")] + Dir[File.join(dir, "new", "*")] }.flatten.sort
    puts "; found #{files.size} messages"
    files
  end
end
end
