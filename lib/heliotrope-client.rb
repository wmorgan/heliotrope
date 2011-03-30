require 'rubygems'
require 'rest_client'
require 'json'

class HeliotropeClient
  def initialize url
    @resource = RestClient::Resource.new url
  end

  def search query, num=20, start=0
    response = @resource["search.json"].get :params => { :q => query, :start => start, :num => num }
    response.force_encoding Encoding::UTF_8 if in_ruby19_hell?
    JSON.parse response
  end

  def thread id
    response = @resource["thread/#{id}.json"].get
    response.force_encoding Encoding::UTF_8 if in_ruby19_hell?
    JSON.parse response
  end

  def message id, preferred_mime_type="text/plain"
    response = @resource["message/#{id}.json"].get :params => { :mime_type_pref => preferred_mime_type }
    response.force_encoding Encoding::UTF_8 if in_ruby19_hell?
    JSON.parse response
  end

  def message_part message_id, part_id
    ## this is not a json blob, but a binary attachment
    @resource["message/#{message_id}/part/#{part_id}"].get
  end

  def labels
    JSON.parse @resource["labels"].get
  end

  def prune_labels!
    JSON.parse @resource["labels/prune"].post
  end

  def set_labels! thread_id, labels
    JSON.parse @resource["thread/#{thread_id}/labels"].post :labels => labels.to_a.to_json
  end

private

  def in_ruby19_hell?
    @in_ruby19_hell = "".respond_to?(:encoding) if @in_ruby19_hell.nil?
    @in_ruby19_hell
  end
end
