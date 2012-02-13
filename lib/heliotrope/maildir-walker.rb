require 'time'

module Heliotrope
class MaildirWalker
  def initialize dirs
    @dirs = dirs
    @last_file_read = nil
  end

  def can_provide_labels?; true end
  def load! state
    @files = get_files
    if state
      @last_file_read = state["last_file_read"]
      if @last_file_read
        index = @files.index @last_file_read
        if index
          @files = @files[(index + 1) .. -1] || []
        end
      end
    end
  end

  def each_message
    until done?
      fn = @files.shift
      message = IO.read fn
      yield message, ["inbox"], state_from_filename(fn), fn
      @last_file_read = fn
    end
  end

  def skip! num
    @files = @files[num .. -1]
  end

  def done?
    @files ||= get_files
    @files.empty?
  end

  def finish!
    { "last_file_read" => @last_file_read } #state
  end

private

  def state_from_filename fn
    state = []
    flags = if fn =~ /\,([\w]+)$/
      $1.split(//)
    else
      []
    end

    state << "unread" unless flags.member?("S")
    state << "starred" if flags.member?("F")
    state << "deleted" if flags.member?("T")
    state << "draft" if flags.member?("D")
    state
  end

  def get_files
    puts "; scanning #{@dirs.size} directories..."
    dirs = @dirs.map { |d| d.gsub(/([\*\?\[\]])/, '\\\\\1') } # have to escape for globbing
    files = dirs.map { |dir| Dir[File.join(dir, "cur", "*")] + Dir[File.join(dir, "new", "*")] }.flatten.sort
    puts "; found #{files.size} messages"
    puts "; reading in dates..."
    file_dates = files.map { |fn| get_date_in_file fn }
    puts "; sorting..."
    files = files.zip(file_dates).select { |fn, date| date }.sort_by { |fn, date| date }
    puts "; sorted #{files.size} messages with dates"
    files.map { |fn, date| fn }
  end

  def get_date_in_file fn
    File.open(fn, "r:BINARY") do |f|
      while(l = f.gets)
        if l =~ /^Date:\s+(.+\S)\s*$/
          return begin
            Time.parse($1)
          rescue
            Time.at 0
          end
        end
      end
    end
    ## spam message don't have date headers
    # puts "; warning: no date in #{fn}"
  end
end
end
