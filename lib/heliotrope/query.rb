## just a wrapper around a Whistlepig query. trying to protect the user from
## explicit whistlepig dependencies
require 'whistlepig'

module Heliotrope
class Query
  class ParseError < StandardError; end

  def initialize field, query, q=nil
    @whistlepig_q = q || begin
      Whistlepig::Query.new(field, query)
    rescue Whistlepig::ParseError => e
      raise ParseError, e.message
    end
  end

  attr_reader :whistlepig_q
  def clone; Query.new(nil, nil, @whistlepig_q.clone) end
  def and other; Query.new(nil, nil, @whistlepig_q.and(other)) end
  def query; @whistlepig_q.query end
end
end
