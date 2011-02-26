module Heliotrope
class MaildirWalker
  def initialize dir
    @dir = dir
    @files = (Dir[File.join(@dir, "cur", "*")] + Dir[File.join(@dir, "new", "*")]).sort
    puts "; found #{@files.size} messages"
  end

  def next_message
    return nil if @files.empty?
    IO.read(@files.shift)
  end

  def done?; @files.empty? end
  def finish!; end
end
end
