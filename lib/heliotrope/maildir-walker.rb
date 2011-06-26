# encoding: UTF-8

require 'time'

module Heliotrope
class MaildirWalker
  def initialize(*dirs)
    @dirs = dirs
  end

  def can_provide_labels?; false end
  def load!; @files = get_files end

  def next_message
    return nil if @files.empty?
    fn = @files.shift
    message = IO.read fn
    [message, ["unread"], ["inbox"], fn]
  end

  def skip! num
    @files = @files[num .. -1]
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
    puts "; reading in dates..."
    file_dates = files.map { |fn| get_date_in_file fn }
    puts "; sorting..."
    files = files.zip(file_dates).sort_by { |fn, date| date }
    puts "; ready"
    files.map { |fn, date| fn }
  end

  def get_date_in_file fn
    File.open(fn) do |f|
      while(l = f.gets)
        if l =~ /^Date:\s+(.+\S)\s*$/
          date = $1
          pdate = Time.parse($1)
          return pdate
        end
      end
    end
    puts "; warning: no date in #{fn}"
    Time.at 0
  end
end
end
