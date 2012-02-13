require 'rubygems'
require 'json'
require "heliotrope"

## all the common functionality between heliotrope-add and -import
module Heliotrope
class MessageAdder
  def initialize opts
    @source = if opts.mbox_fn
      Heliotrope::MboxSplitter.new opts.mbox_fn
    elsif opts.maildir_dirs
      Heliotrope::MaildirWalker.new opts.maildir_dirs
    elsif opts.imap_host
      port = opts.imap_port || (opts.dont_use_ssl ? 143 : 993)
      username = opts.imap_username || ask("IMAP username: ")
      password = opts.imap_password || ask_secret("IMAP password: ")
      Heliotrope::IMAPDumper.new :host => opts.imap_host, :port => port, :ssl => !opts.dont_use_ssl, :username => username, :password => password, :folder => opts.imap_folder
    elsif opts.gmail_username
      username = opts.gmail_username || ask("GMail username: ")
      password = opts.gmail_password || ask_secret("GMail password: ")
      Heliotrope::GMailDumper.new :username => username, :password => password
    else
      Heliotrope::MBoxStream.new $stdin
    end
    @opts = opts
  end

  def each_message
    num_scanned = num_indexed = num_bad = num_seen = 0
    startt = lastt = Time.now
    state = if @opts.state_file && File.exist?(@opts.state_file)
      puts "Loading state..."
      JSON.parse IO.read(@opts.state_file)
    end

    puts "Loading mail source..."
    @source.load! state
    @source.skip! @opts.num_skip if @opts.num_skip

    puts "Adding mail..."
    begin
      @source.each_message do |rawbody, labels, state, desc|
        break if @opts.num_messages && (num_scanned >= @opts.num_messages)
        num_scanned += 1

        ## try to avoid being fucked by ruby 1.9
        rawbody.force_encoding("binary") if rawbody.respond_to?(:force_encoding)

        ## if the source can't set its own labels, we will just add everything to
        ## the inbox
        unless @source.can_provide_labels?
          labels += %w(inbox)
          state += %w(unread)
        end
        puts "; adding #{desc} with labels {#{labels.join ", "}} and state {#{state.join ", "}}" if @opts.verbose

        seen, indexed, bad = yield rawbody, state, labels
        num_seen += 1 if seen
        num_indexed += 1 if indexed
        num_bad += 1 if bad

        if (Time.now - lastt) > 5 # seconds
          elapsed = Time.now - startt
          printf "; scanned %d, indexed %d, skipped %d bad and %d seen messages in %.1fs = %.1f m/s\n", num_scanned, num_indexed, num_bad, num_seen, elapsed, num_scanned / elapsed
          lastt = Time.now
        end
      end
    ensure
      state = @source.finish!
      if @opts.state_file
        puts "Saving state..."
        File.open(@opts.state_file, "w") { |f| f.puts state.to_json }
      end
    end

    elapsed = Time.now - startt
    printf "; scanned %d, indexed %d, skipped %d bad and %d seen messages in %.1fs = %.1f m/s\n", num_scanned, num_indexed, num_bad, num_seen, elapsed, num_scanned / elapsed

    puts "Done."
  end

private

  def ask q
    print q
    $stdout.flush
    (gets || abort).chomp
  end

  def ask_secret q
    begin
      `stty -echo`
      ask q
    ensure
      `stty echo`
    end
  end
end
end
