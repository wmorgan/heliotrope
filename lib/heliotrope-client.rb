require 'rubygems'
require 'rest_client'
require 'json'

class HeliotropeClient
  class Error < StandardError; end

  attr_reader :url
  def initialize url
    @url = url
    @resource = RestClient::Resource.new url
  end

  def search query, num=20, start=0
    get_json "search", :q => query, :start => start, :num => num
  end

  def count query
    get_json "count", :q => query
  end

  def thread id; get_json("thread/#{id}") end

  def thread_labels id; get_json("thread/#{id}/labels") end
  def thread_state id; get_json("thread/#{id}/state") end

  def message id, preferred_mime_type="text/plain"
    get_json "message/#{id}", :mime_type_pref => preferred_mime_type
  end

  def message_part message_id, part_id
    ## this is not a json blob, but a binary attachment
    @resource["message/#{message_id}/part/#{part_id}"].get
  end

  def labels; get_json("labels") end
  def ping; get_json("status") end

  def prune_labels!; post_json("labels/prune") end

  def set_labels! thread_id, labels
    post_json "thread/#{thread_id}/labels", :labels => labels.to_json
  end

  def set_state! message_id, state
    post_json "message/#{message_id}/state", :state => state.to_json
  end

private

  def get_json path, params={}
    convert_errors do
      response = @resource[path + ".json"].get :params => params
      response.force_encoding Encoding::UTF_8 if in_ruby19_hell?
      JSON.parse response
    end
  end

  def post_json path, params={ :please => "1" } # you need to have at least one param for RestClient to work... lame
    convert_errors do
      response = @resource[path + ".json"].post params
      response.force_encoding Encoding::UTF_8 if in_ruby19_hell?
      JSON.parse response
    end
  end

  def convert_errors
    begin
      yield
    rescue SystemCallError, RestClient::Exception => e
      raise Error, "#{e.message} (#{e.class})"
    end
  end

  def in_ruby19_hell?
    @in_ruby19_hell = "".respond_to?(:encoding) if @in_ruby19_hell.nil?
    @in_ruby19_hell
  end
end
