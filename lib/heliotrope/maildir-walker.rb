# encoding: UTF-8

require 'time'

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
    puts "; reading in dates..."
    file_dates = files.map { |fn| get_date_in_file fn }
    puts "; sorting..."
    files = files.zip(file_dates).sort_by { |fn, date| date }
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
