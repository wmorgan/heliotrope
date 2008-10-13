require 'rubygems'
require 'riddle'
require 'fastthread'
require 'fileutils'
require "heliotrope" # we'll need the thrift definitions as well

module Heliotrope
module Indexes

class SphinxConfig
  attr_reader :main_index_name, :delta_index_name, :delta_source_fn,
    :main_index_path, :delta_index_path, :searchd_log_fn, :searchd_query_log_fn,
    :index_dir, :config_fn, :searchd_pid_fn, :sphinx_bin_dir,
    :attrs, :fields

  def initialize index_dir, config_fn, sphinx_bin_dir
    @index_dir = index_dir
    @config_fn = config_fn
    @sphinx_bin_dir = sphinx_bin_dir

    @main_index_name = "index-main"
    @delta_index_name = "index-delta"
    @delta_source_fn = File.join @index_dir, "delta.xml"
    @main_index_path = File.join @index_dir, "main"
    @delta_index_path = File.join @index_dir, "delta"
    @searchd_log_fn = File.join @index_dir, "searchd.log"
    @searchd_query_log_fn = File.join @index_dir, "query.log"
    @searchd_pid_fn = File.join @index_dir, "searchd.pid"

    @fields = []
    @attrs = []

    yield self if block_given?
    
    File.open(@config_fn, "w") { |f| f.write configuration_text }
  end

  def field f, opts={}; @fields << [f, opts] end
  def attr a, opts={}; @attrs << [a, opts] end

  def template_body
    <<EOS
### Heliotrope Sphinx configuration file.
### 
### DO NOT EDIT (unless you really know what you're doing)
###
### This file is generated programmatically by Heliotrope. Any configuration
### should be done when defining your Heliotrope service.

source main
{
  type = xmlpipe
  xmlpipe_command = echo '<?xml version="1.0" encoding="utf-8"?><sphinx:docset></sphinx:docset>'

  __HELIOTROPE_CONFIG_field_definitions__
  __HELIOTROPE_CONFIG_attr_definitions__
}

source delta : main
{
  xmlpipe_command = echo '<?xml version="1.0" encoding="utf-8"?><sphinx:docset>'; cat __HELIOTROPE_CONFIG_delta_source_fn__; echo '</sphinx:docset>'
}

index __HELIOTROPE_CONFIG_main_index_name__
{
  source = main
  path = __HELIOTROPE_CONFIG_main_index_path__

  docinfo = extern
  mlock = 0 # change to 1 and run as root to prevent any swapping... if you're crazy
  morphology = none
  min_word_len = 1
  charset_type = utf-8
}

index __HELIOTROPE_CONFIG_delta_index_name__ : __HELIOTROPE_CONFIG_main_index_name__
{
  source = delta
  path = __HELIOTROPE_CONFIG_delta_index_path__
}

indexer
{
  mem_limit = 256M
}

searchd
{
  port = 3312
  log = __HELIOTROPE_CONFIG_searchd_log_fn__
  query_log = __HELIOTROPE_CONFIG_searchd_query_log_fn__
  pid_file = __HELIOTROPE_CONFIG_searchd_pid_fn__
}
EOS
  end

private

  def field_definitions
    @fields.map do |f, opts|
      "xmlpipe_field = #{f}"
    end.join("\n")
  end

  def attr_definitions
    attrs = @attrs + [["deleted", {:type => :bool}]]
    attrs.map do |a, opts|
      type = opts[:type] or raise "no type specified for attribute #{a}"
      "xmlpipe_attr_#{type} = #{a}"
    end.join("\n")
  end

  def configuration_text
    template_body.gsub(/__HELIOTROPE_CONFIG_.+?__/) do |x|
      m = x =~ /__HELIOTROPE_CONFIG_(.+?)__/ && $1
      send m
    end
  end
end

## a wrapper around the sphinx indexer
class SphinxIndex
  DELTA_MAX_NUM_OBJECTS = 1000

  def initialize config
    @config = config
    @quarkfarm = Heliotrope::Quarker.new File.join(@config.index_dir, "quarks")
    @counter = Heliotrope::Counter.new File.join(@config.index_dir, "docid-counter")
    @searchd_bin = File.join @config.sphinx_bin_dir, "searchd"
    @indexer_bin = File.join @config.sphinx_bin_dir, "indexer"

    unless File.exist? @config.delta_source_fn
      FileUtils.touch @config.delta_source_fn
      reindex @config.main_index_name
      reindex @config.delta_index_name
    end

    @client = nil
    @delta_size = 0
    @delta_mutex = Mutex.new
    @next_ok_time = Time.at 0
  end

  def running?; !!@client end

  def start
    run "#@searchd_bin -c #{@config.config_fn}"
    @client = Riddle::Client.new
    @client.match_mode = :extended
  end

  def stop
    return false unless running?
    run "#@searchd_bin -c #{@config.config_fn} --stop"
    @client = nil
    true
  end

  def flush; flush_delta end

  def add objects
    objects = [objects].flatten
    ids = @counter.next objects.size
    dfn = @config.delta_source_fn
    @delta_mutex.synchronize do
      File.open(dfn, "a") do |f|
        objects.zip(ids).each { |o, i| f.puts make_sphinx_xml(o, i) }
      end
      reindex @config.delta_index_name
      @delta_size += objects.size
      flush_delta if @delta_size > DELTA_MAX_NUM_OBJECTS
    end

    ids
  end

  def search query, index="*"
    query, filters = parse_query query
    @client.filters = filters
    @client.filters << Riddle::Client::Filter.new("deleted", [0])
    wait_until_server_is_ready
    results = @client.query query, index
    puts ">>> for query: #{query.inspect} with filters #{filters.inspect}" if $DEBUG
    puts ">>> have results: #{results.inspect}" if $DEBUG

    raise results[:warning] if results[:warning]
    raise results[:error] if results[:error]
    results[:matches]
  end

  def delete ids
    ids = [ids].flatten
    return if ids.empty?

    ## we have to flush the delta index here because the deleted document might
    ## be in it, in which case the next time we reindex it will magically
    ## become undeleted.
    puts "DDD flushing" if $DEBUG
    @delta_mutex.synchronize { flush_delta }
    ids.each do |id|
      puts "DDD updating #{id} in main to deleted=1" if $DEBUG
      @client.update @config.main_index_name, %w(deleted), { id => 1 }
    end
  end

  def update ids, values
    ids = [ids].flatten
    values = [values].flatten

    delete ids
    add values
  end

private

  ## sphinx takes a couple seconds after indexing before updates become
  ## available. this is how we get around that.
  def dont_use_server_for t
    t = Time.now + t
    @next_ok_time = t if t > @next_ok_time
  end

  def wait_until_server_is_ready
    t = Time.now
    sleep(@next_ok_time - t) if @next_ok_time && t < @next_ok_time
  end

  def make_sphinx_xml doc, id
    "<sphinx:document id=\"#{id}\">\n" + 
      @config.fields.map do |f, opts|
        m = opts[:method] || f
        "  <#{f}>#{escape doc.send(m).to_s}</#{f}>\n"
      end.join +
      @config.attrs.map do |a, opts|
        m = opts[:method] || a
        val = case opts[:type]
          when :timestamp; doc.send(m).to_i
          when :uint
            x = doc.send(m)
            case x
              when Fixnum; x
              when String; @quarkfarm.string_to_int(a, x)
              else raise "wtf: #{x.inspect} for #{a}"
            end
          when :bool; doc.send("#{m}?") ? 1 : 0
          else raise "wtf2: #{opts.inspect}"
        end

        "  <#{a}>#{val}</#{a}>\n"
      end.join +
      "  <deleted>0</deleted>\n</sphinx:document>\n"
  end 

  def escape s
    s =~ /^\s*$/ ? "" : "<![CDATA[#{s.strip}]]>"
  end

  def parse_query query
    print ">>>TRANFORMING #{query}" if $DEBUG
    filters = []
    query = query.gsub(/([\+\-]?)(\w+):(([^(]\S*)|\(.*?\))\b/) do |x|
      neg, field, q = $1, $2, $3
      "@#{field} #{neg == "-" ? '-' : ''}#{q} @body"
    end
    puts " into #{query.inspect} and #{filters.size} filters" if $DEBUG
    [query, filters]
  end

  def reindex index_name
    ## TODO: merge delta + main when delta is too big
    ## don't forget to exclude all docs with delete == 1
    ## something like:
    ##   indexer --merge main delta --merge-dst-range deleted 0 0
    ###
    if running?
      run "#@indexer_bin -c #{@config.config_fn} --rotate #{index_name}"
      dont_use_server_for 2
    else
      run "#@indexer_bin -c #{@config.config_fn} #{index_name}"
    end
  end

  def flush_delta
    return if @delta_size == 0
    #$DEBUG = true
    #puts "_____FLUSH___ #{@delta_size}"
    run "#@indexer_bin -c #{@config.config_fn} --merge #{@config.main_index_name} #{@config.delta_index_name} --rotate --merge-dst-range deleted 0 0"

    sleep 1 # sigh

    File.open(@config.delta_source_fn, "w") { }
    run "#@indexer_bin -c #{@config.config_fn} --rotate #{@config.delta_index_name}"
    @delta_size = 0
    dont_use_server_for 2
    #$DEBUG = false
  end

  def run cmd
    wait_until_server_is_ready
    if $DEBUG
      puts
      puts ">>> #{cmd}"
      puts
    else
      cmd += " > /dev/null 2> /dev/null" 
    end
    system cmd or raise "can't run: #{cmd.inspect}"
  end
end

end
end
