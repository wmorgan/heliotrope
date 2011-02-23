require 'test/unit'
require 'fileutils'
require 'heliotrope'

include Heliotrope

class HeliotropeTest < ::Test::Unit::TestCase
  TEST_DIR = "/tmp/heliotrope-test"

  class MockMessage
    def initialize opts={}
      @@ids ||= 0

      @opts = {
        :signed? => false,
        :has_attachment? => false,
        :encrypted? => false,

        :msgid => "msg-#{@@ids += 1}",
        :from => Person.from_string("Egg Zample <egg@example.com>"),
        :to => Person.from_string("Eggs Ample <eggs@example.com>"),
        :cc => [],
        :bcc => [],
        :subject => "test message",
        :date => Time.now,
        :indexable_text => "i love mice",
        :refs => []
      }.merge opts

      @opts[:recipients] ||= ([@opts[:to]] + @opts[:cc] + @opts[:bcc]).flatten.compact
    end

    def method_missing m, *a
      raise "no value for #{m.inspect}" unless @opts.member? m
      @opts[m]
    end
  end

  def setup
    FileUtils.rm_rf TEST_DIR
    FileUtils.mkdir TEST_DIR
    @store = Store.new TEST_DIR
  end

  def teardown
    @store.close
    FileUtils.rm_rf TEST_DIR
  end

  def test_size
    assert_equal 0, @store.size

    m1 = MockMessage.new
    x = @store.add_message m1, 0, {}, {}
    assert_equal 1, @store.size

    m2 = MockMessage.new
    @store.add_message m2, 0, {}, {}
    assert_equal 2, @store.size
  end

  def test_adding_duplicate_messages_does_nothing
    m1 = MockMessage.new :msgid => "a"
    @store.add_message m1, 0, {}, {}
    assert_equal 1, @store.size

    m2 = MockMessage.new :msgid => "a"
    @store.add_message m2, 0, {}, {}
    assert_equal 1, @store.size
  end

  def test_added_messages_are_available_in_search
    m1 = MockMessage.new :indexable_text => "hello bob"
    docid, threadid = @store.add_message m1, 0, {}, {}
    @store.set_query Query.new("body", "hello")
    results = @store.get_some_results 100
    assert_equal 1, results.size
    assert_equal threadid, results.first[:thread_id]
  end

end
