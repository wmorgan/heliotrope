require 'fileutils'
require 'test/unit'
require 'heliotrope/stores/local-disk-bucket'

module Heliotrope
module Test

## this set of tests is applied below to every class in Heliotrope::Stores.
module StoreTest
  def test_returns_nil_for_non_existent_keys
    assert_nil @b.get("key")
  end

  def test_get_retrieves_what_put_puts
    @b.put "key", "value"
    assert_equal "value", @b.get("key")
  end

  def test_get_retrieves_overridden_values
    @b.put "key", "value"
    @b.put "key", "value2"
    assert_equal "value2", @b.get("key")
  end

  def test_delete_returns_nil_for_non_existent_keys
    assert_nil @b.delete("key")
  end

  def test_delete_returns_deleted_object
    @b.put "key", "value"
    assert_equal "key", @b.delete("key")
  end

  def test_get_returns_nil_for_deleted_objects
    @b.put "key", "value"
    @b.delete "key"
    assert_nil @b.get("key")
  end

  def test_size_is_zero_at_startup
    assert_equal 0, @b.size
  end

  def test_size_increments_upon_put
    s = @b.size
    @b.put "key", "value"
    assert_equal s + 1, @b.size
    @b.put "key2", "value2"
    assert_equal s + 2, @b.size
  end

  def test_haskey_returns_false_for_nonexistant_objects
    assert !@b.has_key?("hello")
  end

  def test_haskey_returns_true_for_existing_objects
    @b.put "hello", "there"
    assert @b.has_key?("hello")
  end
  
  def test_haskey_returns_false_for_deleted_objects
    @b.put "hello", "there"
    @b.delete "hello"
    assert !@b.has_key?("hello")
  end
end

class LocalDiskBucketTest < ::Test::Unit::TestCase
  DIR = "/tmp/local-disk-bucket-test"
  def setup
    FileUtils.rm_rf DIR
    @b = Heliotrope::Stores::LocalDiskBucket.new DIR
  end

  include StoreTest
end

end
end
