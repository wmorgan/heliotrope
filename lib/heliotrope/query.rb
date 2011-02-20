## a wrapper around a Whistlepig query
require 'whistlepig'

module Heliotrope
class Query
  def initialize field, query, q=nil
    @whistlepig_q = q || Whistlepig::Query.new(field, query)
  end

  attr_reader :whistlepig_q
  def clone; Query.new(nil, nil, @whistlepig_q.clone) end
  def and other; Query.new(nil, nil, @whistlepig_q.and(other)) end
  def query; @whistlepig_q.query end
end
end
