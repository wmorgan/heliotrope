require 'test/unit'
require 'fileutils'
require 'heliotrope'

class HeliotropeTest < ::Test::Unit::TestCase
  TEST_DIR = "/tmp/heliotrope-test"

  def setup
    FileUtils.rm_rf TEST_DIR
    FileUtils.mkdir TEST_DIR
    @store = Heliotrope::Store.new TEST_DIR
  end

  def test_size
    assert_equal 0, @store.size
  end
end
