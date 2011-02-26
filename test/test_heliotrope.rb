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
    x = @store.add_message m1
    assert_equal 1, @store.size

    m2 = MockMessage.new
    @store.add_message m2
    assert_equal 2, @store.size
  end

  def test_adding_duplicate_messages_does_nothing
    m1 = MockMessage.new :msgid => "a"
    @store.add_message m1
    assert_equal 1, @store.size

    m2 = MockMessage.new :msgid => "a"
    @store.add_message m2
    assert_equal 1, @store.size
  end

  def test_added_messages_are_available_in_search
    @store.set_query Query.new("body", "hello")
    results = @store.get_some_results 100
    assert_equal 0, results.size

    m1 = MockMessage.new :indexable_text => "hello bob"
    docid, threadid = @store.add_message m1

    @store.reset_query!
    results = @store.get_some_results 100
    assert_equal 1, results.size
    assert_equal threadid, results.first[:thread_id]
  end

  def test_added_message_state_is_preserved
    m1 = MockMessage.new
    docid, threadid = @store.add_message m1, %w(unread), []

    summary = @store.load_messageinfo docid
    assert_equal Set.new(%w(unread)), summary[:state]
  end

  def test_added_message_state_is_searchable_via_labels
    @store.set_query Query.new("body", "~unread")
    assert_equal 0, @store.count_results

    m1 = MockMessage.new
    docid, threadid = @store.add_message m1, %w(unread), []

    assert_equal 1, @store.count_results
  end

  def test_message_state_is_modifiable
    m1 = MockMessage.new
    docid, threadid = @store.add_message m1
    assert_equal Set.new, @store.load_messageinfo(docid)[:state]

    @store.update_message_state docid, %w(unread)
    assert_equal Set.new(%w(unread)), @store.load_messageinfo(docid)[:state]

    @store.update_message_state docid, %w(starred)
    assert_equal Set.new(%w(starred)), @store.load_messageinfo(docid)[:state]

    @store.update_message_state docid, %w(unread deleted)
    assert_equal Set.new(%w(unread deleted)), @store.load_messageinfo(docid)[:state]
  end

  def test_message_state_ignores_random_stuff
    m1 = MockMessage.new
    docid, threadid = @store.add_message m1, %w(hello there bob inbox unread is nice), []
    assert_equal Set.new(%w(unread)), @store.load_messageinfo(docid)[:state]
  end

  def test_added_thread_labels_are_applied_to_the_whole_thread
    m1 = MockMessage.new
    docid, threadid = @store.add_message m1, [], %w(tired hungry)

    summary = @store.load_threadinfo threadid
    assert_equal Set.new(%w(tired hungry)), summary[:labels]
  end

  def test_thread_labels_are_available_in_search
    m1 = MockMessage.new :indexable_text => "hello bob"
    docid, threadid = @store.add_message m1

    @store.set_query Query.new("body", "~tired")
    assert_equal 0, @store.count_results

    @store.update_thread_labels threadid, %w(tired)
    assert_equal 1, @store.count_results

    results = @store.get_some_results 100
    assert_equal threadid, results.first[:thread_id]
  end

  def test_thread_labels_from_added_messages_are_available_in_search
    @store.set_query Query.new("body", "~tired")
    results = @store.get_some_results 100
    assert_equal 0, results.size

    m1 = MockMessage.new :indexable_text => "hello bob"
    docid, threadid = @store.add_message m1, [], %w(tired hungry)

    @store.reset_query!
    results = @store.get_some_results 100
    assert_equal 1, results.size
 end

 def test_thread_labels_are_modifiable
    m1 = MockMessage.new :indexable_text => "hello bob"
    docid, threadid = @store.add_message m1
    assert_equal Set.new, @store.load_threadinfo(threadid)[:labels]

    @store.update_thread_labels threadid, %w(hungry)
    assert_equal Set.new(%w(hungry)), @store.load_threadinfo(threadid)[:labels]

    @store.update_thread_labels threadid, %w(tired)
    assert_equal Set.new(%w(tired)), @store.load_threadinfo(threadid)[:labels]

    @store.update_thread_labels threadid, %w(hungry tired)
    assert_equal Set.new(%w(hungry tired)), @store.load_threadinfo(threadid)[:labels]
 end

 def test_messages_are_threaded
    m1 = MockMessage.new :msgid => "1"
    docid1, threadid1 = @store.add_message m1

    m2 = MockMessage.new :msgid => "2", :refs => ["1"]
    docid2, threadid2 = @store.add_message m2

    assert_equal threadid1, threadid2

    m3 = MockMessage.new :msgid => "3", :refs => ["2"]
    docid3, threadid3 = @store.add_message m3

    assert_equal threadid2, threadid3

    m4 = MockMessage.new :msgid => "4", :refs => ["1"]
    docid4, threadid4 = @store.add_message m4

    assert_equal threadid3, threadid4
 end

  def test_message_state_is_propagated_to_thread_as_a_disjunction_in_threadinfo
    m1 = MockMessage.new :msgid => "1"
    m2 = MockMessage.new :msgid => "2", :refs => ["1"]
    m3 = MockMessage.new :msgid => "3", :refs => ["2"]

    docid1, threadid1 = @store.add_message m1, %w(unread)
    docid2, threadid2 = @store.add_message m2, %w(unread)
    docid3, threadid3 = @store.add_message m3, %w(unread)

    assert_equal threadid1, threadid2
    assert_equal threadid2, threadid3

    assert_equal Set.new(%w(unread)), @store.load_threadinfo(threadid1)[:state]

    @store.update_message_state docid1, []
    assert_equal Set.new(%w(unread)), @store.load_threadinfo(threadid1)[:state]

    @store.update_message_state docid2, []
    assert_equal Set.new(%w(unread)), @store.load_threadinfo(threadid1)[:state]

    @store.update_message_state docid3, []
    assert_equal Set.new, @store.load_threadinfo(threadid1)[:state]

    ## now add some back
    @store.update_message_state docid3, %w(starred)
    assert_equal Set.new(%w(starred)), @store.load_threadinfo(threadid1)[:state]
  end

  ## this captures a bug i had
  def test_message_state_is_propagated_to_threadinfo_even_if_it_is_just_on_the_root
    m1 = MockMessage.new :msgid => "1", :has_attachment? => true
    m2 = MockMessage.new :msgid => "2", :refs => ["1"]
    m3 = MockMessage.new :msgid => "3", :refs => ["2"]

    docid1, threadid1 = @store.add_message m1
    docid2, threadid2 = @store.add_message m2
    docid3, threadid3 = @store.add_message m3

    assert_equal threadid1, threadid2
    assert_equal threadid2, threadid3

    assert_equal Set.new(%w(attachment)), @store.load_threadinfo(threadid1)[:state]
  end

  def test_message_state_is_propagated_to_thread_as_a_disjunction_in_search
    @store.set_query Query.new("body", "~unread")
    assert_equal 0, @store.count_results

    m1 = MockMessage.new :msgid => "1"
    m2 = MockMessage.new :msgid => "2", :refs => ["1"]
    m3 = MockMessage.new :msgid => "3", :refs => ["2"]

    docid1, threadid1 = @store.add_message m1, %w(unread)
    docid2, threadid2 = @store.add_message m2, %w(unread)
    docid3, threadid3 = @store.add_message m3, %w(unread)

    assert_equal threadid1, threadid2
    assert_equal threadid2, threadid3

    assert_equal 1, @store.count_results
    @store.update_message_state docid1, []
    assert_equal 1, @store.count_results
    @store.update_message_state docid2, []
    assert_equal 1, @store.count_results
    @store.update_message_state docid3, []
    assert_equal 0, @store.count_results

    @store.set_query Query.new("body", "~starred")
    assert_equal 0, @store.count_results
    @store.update_message_state docid3, %w(starred)
    assert_equal 1, @store.count_results
  end

  def test_adding_messages_can_join_threads
    m1 = MockMessage.new :msgid => "1"
    m3 = MockMessage.new :msgid => "3", :refs => ["2"]

    docid1, threadid1 = @store.add_message m1
    docid3, threadid3 = @store.add_message m3

    assert_not_equal threadid1, threadid3

    m2 = MockMessage.new :msgid => "2", :refs => ["1"]
    docid2, threadid2 = @store.add_message m2

    threadid1 = @store.load_messageinfo(docid1)[:thread_id]
    threadid2 = @store.load_messageinfo(docid2)[:thread_id]
    threadid3 = @store.load_messageinfo(docid3)[:thread_id]

    assert_not_nil threadid1
    assert_equal threadid1, threadid2
    assert_equal threadid2, threadid3
  end

  def test_adding_messages_applies_labels_to_everything_in_thread_and_that_is_reflected_in_search
    m1 = MockMessage.new :msgid => "1"
    m2 = MockMessage.new :msgid => "2", :refs => ["1"]
    m3 = MockMessage.new :msgid => "3", :refs => ["2"]

    docid1, threadid1 = @store.add_message m1
    docid2, threadid2 = @store.add_message m2

    @store.set_query Query.new("body", "~hungry")
    assert_equal 0, @store.count_results

    docid3, threadid3 = @store.add_message m3, [], %w(hungry)
    assert_equal 1, @store.count_results

    results = @store.get_some_results 100
    assert_equal threadid3, results.first[:thread_id]
    docids = results.first[:structure].flatten
    assert_includes docid1, docids
    assert_includes docid2, docids
    assert_includes docid3, docids
  end

  def test_adding_messages_can_join_threads_and_labels_are_unionized
    m1 = MockMessage.new :msgid => "1"
    m3 = MockMessage.new :msgid => "3", :refs => ["2"]

    docid1, threadid1 = @store.add_message m1, [], %w(fluffy)
    assert_equal Set.new(%w(fluffy)), @store.load_threadinfo(threadid1)[:labels]

    docid3, threadid3 = @store.add_message m3, [], %w(bunny)
    assert_equal Set.new(%w(bunny)), @store.load_threadinfo(threadid3)[:labels]

    m2 = MockMessage.new :msgid => "2", :refs => ["1"]
    docid2, threadid2 = @store.add_message m2

    assert_equal threadid2, @store.load_messageinfo(docid1)[:thread_id]
    assert_equal threadid2, @store.load_messageinfo(docid2)[:thread_id]
    assert_equal threadid2, @store.load_messageinfo(docid3)[:thread_id]

    assert_equal Set.new(%w(fluffy bunny)), @store.load_threadinfo(threadid2)[:labels]
  end

  def test_adding_messages_can_join_threads_and_their_labels_are_unionized_and_that_is_reflected_in_search
    m1 = MockMessage.new :msgid => "1"
    m3 = MockMessage.new :msgid => "3", :refs => ["2"]

    docid1, threadid1 = @store.add_message m1, [], %w(fluffy)
    docid3, threadid3 = @store.add_message m3, [], %w(bunny)

    @store.set_query Query.new("body", "~fluffy")
    results = @store.get_some_results 100
    assert_equal 1, results.size
    assert_equal threadid1, results.first[:thread_id]

    m2 = MockMessage.new :msgid => "2", :refs => ["1"]
    docid2, threadid2 = @store.add_message m2

    @store.reset_query!
    results = @store.get_some_results 100
    assert_equal 1, results.size

    assert_equal threadid2, results.first[:thread_id]
    docids = results.first[:structure].flatten
    assert_includes docid1, docids
    assert_includes docid2, docids
    assert_includes docid3, docids
  end

private

  def assert_includes v, set # standard one seems to have these things reversed
    assert set.include?(v), "#{set.inspect[0..50]} does not include #{v.inspect}"
  end
end
