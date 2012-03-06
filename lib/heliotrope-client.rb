require 'rubygems'
require 'curb'
require 'uri'
require 'json'
require 'set'

class HeliotropeClient
  class Error < StandardError; end

  MESSAGE_MUTABLE_STATE = Set.new %w(starred unread deleted)
  MESSAGE_IMMUTABLE_STATE = Set.new %w(attachment signed encrypted draft sent)
  MESSAGE_STATE = MESSAGE_MUTABLE_STATE + MESSAGE_IMMUTABLE_STATE

  attr_reader :url
  def initialize url
    @url = url
    @curl = Curl::Easy.new
  end

  def search query, num=20, start=0
    v = get_json "search", :q => query, :start => start, :num => num
    v["results"]
  end

  def count query
    get_json("count", :q => query)["count"]
  end

  def thread id; get_json("thread/#{id}")["messageinfos"] end
  def threadinfo id; get_json("thread/#{id}/info") end

  def message id, mime_type_pref="text/plain"
    get_json "message/#{id}", :mime_type_pref => mime_type_pref
  end

  def send_message message, opts={}
    opts[:labels] ||= []
    opts[:state] ||= []
    post_json "message/send", :message => message, :labels => opts[:labels].to_json, :state => opts[:state].to_json
  end

  def bounce_message message, opts={}
    opts[:force_recipients] ||= []
    post_json "message/bounce", :message => message, :force_recipients => opts[:force_recipients].to_json
  end

  def message_part message_id, part_id
    ## not a json blob, but a binary region
    @curl.url = "#{URI.join(@url, "/message/#{message_id}/part/#{part_id}")}"
    @curl.http_get
    if @curl.response_code != 200
      raise Error, "Unexpected HTTP response code #{@curl.response_code}"
    end
    return @curl.body_str
  end

  def raw_message message_id
    ## not a json blob, but a binary region
    @curl.url = "#{URI.join(@url, "/message/#{message_id}/raw")}"
    @curl.http_get
    if @curl.response_code != 200
      raise Error, "Unexpected HTTP response code #{@curl.response_code} getting #{@curl.url}"
    end
    return @curl.body_str
  end

  def labels; get_json("labels")["labels"] end
  def info; get_json("info") end
  def size; get_json("size")["size"] end

  def prune_labels!; post_json("labels/prune")["labels"] end

  def set_labels! thread_id, labels
    post_json "thread/#{thread_id}/labels", :labels => labels.to_json
  end

  def set_state! message_id, state
    post_json "message/#{message_id}/state", :state => state.to_json
  end

  def set_thread_state! thread_id, state
    post_json "thread/#{thread_id}/state", :state => state.to_json
  end

private

  def get_json path, params={}
    handle_errors do
      @curl.url = "#{URI.join(@url, path + ".json")}?#{URI.encode_www_form(params)}"
      @curl.http_get
      if @curl.response_code != 200
        raise Error, "Unexpected HTTP response code #{@curl.response_code} getting #{@curl.url}"
      end
      response = @curl.body_str
      response.force_encoding Encoding::UTF_8 if in_ruby19_hell?
      JSON.parse response
    end
  end

  def post_json path, params={}
    handle_errors do
      @curl.url = "#{URI.join(@url, path + ".json")}"
      @curl.post_body = URI.encode_www_form(params)
      @curl.http_post
      if @curl.response_code != 200
        raise Error, "Unexpected HTTP response code #{@curl.response_code} posting to #{@curl.url}"
      end
      response = @curl.body_str
      response.force_encoding Encoding::UTF_8 if in_ruby19_hell?
      JSON.parse response
    end
  end

  def handle_errors
    begin
      v = yield
      raise Error, "invalid response: #{v.inspect[0..200]}" unless v.is_a?(Hash)
      case v["response"]
        when "ok"; v
        when "error"; raise Error, v["message"]
        else raise Error, "invalid response: #{v.inspect[0..200]}"
      end
    rescue SystemCallError, Curl::Err, JSON::ParserError => e
      raise Error, "#{e.message} (#{e.class})"
    end
  end

  def in_ruby19_hell?
    @in_ruby19_hell = "".respond_to?(:encoding) if @in_ruby19_hell.nil?
    @in_ruby19_hell
  end
end
