module Heliotrope
  ## ruby 1.9 versions of this don't work with Timeout::timeout, so
  ## we use this ruby 1.8 backport.
  def popen3(*cmd)
    pw, pr, pe = IO::pipe, IO::pipe, IO::pipe # [0] = read, [1] = write

    pid = fork do
      fork do
        pw[1].close; STDIN.reopen pw[0]; pw[0].close
        pr[0].close; STDOUT.reopen pr[1]; pr[1].close
        pe[0].close; STDERR.reopen pe[1]; pe[1].close
        exec(*cmd)
      end
      exit!(0)
    end

    pw[0].close; pr[1].close; pe[1].close
    Process.waitpid pid
    pi = [pw[1], pr[0], pe[0]]
    pw[1].sync = true

    begin
      yield(*pi)
    ensure
      pi.each { |p| p.close unless p.closed? }
    end
  end

  module_function :popen3
end

require "heliotrope/decoder"
require "heliotrope/person"
require "heliotrope/message"
require "heliotrope/mbox-splitter"
require "heliotrope/imap-dumper"
require "heliotrope/gmail-dumper"
require "heliotrope/maildir-walker"
require "heliotrope/meta-index"
require "heliotrope/zmbox"
require "heliotrope/query"
require "heliotrope/hooks"
