require 'fileutils'
require 'test/unit'
require 'yaml'
require 'heliotrope/counter'

require File.dirname(__FILE__) + "/test-helper.rb"

module Heliotrope
module Test

class CounterTest < ::Test::Unit::TestCase
  DIR = "/tmp/counter-test"
  FN = File.join DIR, "counter"

  def setup
    FileUtils.rm_rf DIR
    FileUtils.mkdir DIR

    @c = Counter.new FN
  end
  
  def test_returns_consecutive_values
    a = @c.next.first
    b = @c.next.first
    assert_equal 1, b - a
  end

  def test_saves_state_to_disk
    a = @c.next.first
    @c = nil

    d = Counter.new FN
    b = d.next.first
    assert_equal 1, b - a
  end

  def test_returns_multiple_values
    a = @c.next 5
    assert_kind_of Array, a
    assert_equal 5, a.size
  end

  def test_multiple_values_saved_to_disk
    a = @c.next 5
    @c = nil

    d = Counter.new FN
    b = d.next.first
    assert_equal 1, b - a.last
  end
end

end
end

