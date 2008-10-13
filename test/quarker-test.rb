require 'fileutils'
require 'test/unit'
require 'heliotrope/quarker'

module Heliotrope
module Test

class QuarkerTest < ::Test::Unit::TestCase
  DIR = "/tmp/quarker-test/"
  FN = File.join DIR, "quarks"

  def setup
    FileUtils.rm_rf DIR
    FileUtils.mkdir DIR
    @q = Quarker.new FN
  end
  
  def test_throws_exception_for_nonexistent_ints
    assert_raises(ArgumentError) { @q.ints_to_strings [0] }
  end

  def test_doesnt_throw_exceptions_for_nonexistent_strings
    assert_nothing_raised { @q.strings_to_ints "domain", "potato" }
  end

  def test_returns_int_for_a_string
    assert_kind_of Fixnum, @q.string_to_int("domain", "hello")
  end

  def test_returns_string_for_an_int
    x = @q.string_to_int("domain", "hello")
    assert_kind_of String, @q.int_to_string("domain", x)
  end

  def test_ints_are_sequential
    w = %w(hello there bob how are you)
    x = @q.strings_to_ints "domain", w
    (1 ... x.size).each do |i|
      assert_equal x[i], x[i - 1] + 1
    end
  end

  def test_int_string_mapping_is_saved
    w = %w(hello there bob how are you)
    x = @q.strings_to_ints "domain", w
    assert_equal x, @q.strings_to_ints("domain", w)
  end

  def test_ints_and_strings_are_reversible
    w = %w(hello there bob how are you)
    x = @q.strings_to_ints "domain", w
    assert_equal w, @q.ints_to_strings("domain", x)
  end

  def test_domains_dont_share_strings
    x = @q.strings_to_ints "domain1", %w(hello)
    y = @q.strings_to_ints "domain2", %w(goodbye)
    assert_not_equal x, @q.strings_to_ints("domain2", %w(hello))
    assert_not_equal y, @q.strings_to_ints("domain1", %w(goodbye))
  end

  def test_mapping_is_saved_to_disk
    w = %w(hello there bob how are you)
    x = @q.strings_to_ints "domain", w
    @q = nil

    @r = Quarker.new FN
    assert_equal x, @r.strings_to_ints("domain", w)
  end
end
end
end
