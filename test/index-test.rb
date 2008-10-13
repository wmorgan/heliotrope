require 'fileutils'
require 'test/unit'
require 'time'
require "heliotrope"

require File.join(File.dirname(__FILE__), "test-helper.rb")

module Heliotrope
module Test

class Document
  attr_accessor :id, :body, :subject, :date, :labels
  def initialize h; h.each { |k, v| instance_variable_set "@#{k}", v } end
  def label_text; @labels.join(" ") end
end

MOCKS = {
  :simple_test => Document.new(
    :body => "This is a test email. Thanks for reading it! I hope that you have a nice day.",
    :subject => "test email 1 subject",
    :date => Time.parse("2008-07-13 08:43:58.479625 -07:00"),
    :labels => [:inbox, :unread, :fluffy]
  ),
  :platypus => Document.new(
    :body => "This is a another test email. Thanks for reading it! Unlike the first email, this one contains the word \"platypus\".",
    :subject => "test email 2 subject",
    :date => Time.parse("2008-07-13 08:43:58.479625 -07:00"),
    :labels => [:inbox, :unread, :fluffy]
  ),
  :label_purple => Document.new(
    :body => "This is a another test email. This one is the only one with a particular label, not mentioned in the body.",
    :subject => "test email 3 subject",
    :date => Time.parse("2008-07-13 08:43:58.479625 -07:00"),
    :labels => [:inbox, :unread, :purple]
  ),
}

## this set of tests is applied below to every class in Heliotrope::Indexes.
module IndexTest 
  def test_empty_index_has_no_docs
    assert_equal [], @i.search("test")
  end

  def test_single_document_is_retrievable_by_text
    ids = @i.add MOCKS.values
    MOCKS.values.zip(ids).each { |d, i| d.id = i }
    assert_equal [MOCKS[:platypus].id], @i.search("platypus").map { |r| r[:doc] }
  end

  def test_single_document_is_retrievable_by_label
    ids = @i.add MOCKS.values
    MOCKS.values.zip(ids).each { |d, i| d.id = i }
    assert_equal [MOCKS[:label_purple].id], @i.search("label:purple").map { |r| r[:doc] }
  end

  def test_single_document_is_retrievable_by_label_and_text
    ids = @i.add MOCKS.values
    MOCKS.values.zip(ids).each { |d, i| d.id = i }
    assert_equal [MOCKS[:platypus].id], @i.search("label:inbox platypus").map { |r| r[:doc] }
  end

  def test_multiple_documents_are_retrievable
    ids = @i.add MOCKS.values
    MOCKS.values.zip(ids).each { |d, i| d.id = i }
    assert_includes [MOCKS[:platypus].id, MOCKS[:simple_test].id], @i.search("test email").map { |r| r[:doc] }
  end

  def test_multiple_additions_are_the_same_as_a_single_addition
    pid = @i.add MOCKS[:platypus]
    stid = @i.add MOCKS[:simple_test]
    pid, stid = pid.first, stid.first
    assert_includes [pid, stid], @i.search("test email").map { |r| r[:doc] }
  end

  def test_deleted_documents_dont_appear_in_search
    ids = @i.add MOCKS.values
    MOCKS.values.zip(ids).each { |d, i| d.id = i }
    @i.delete MOCKS[:platypus].id
    assert_doesnt_include [MOCKS[:platypus].id], @i.search("platypus").map { |r| r[:doc] }
  end

  def test_updates_replace_previous_content
    ids = @i.add MOCKS.values
    MOCKS.values.zip(ids).each { |d, i| d.id = i }

    MOCKS[:label_purple].labels = [:one, :two, :three]
    newid = @i.update MOCKS[:label_purple].id, MOCKS[:label_purple]

    MOCKS[:label_purple].labels = [:one, :two, :three]
    newid = @i.update MOCKS[:label_purple].id, MOCKS[:label_purple]
    newid = newid.first

    ret = @i.search("label:purple").map { |r| r[:doc] }
    assert_doesnt_include [MOCKS[:label_purple].id], ret

    ret = @i.search("label:three").map { |r| r[:doc] }
    assert_includes [newid], ret
  end
end

class SphinxIndexTest < ::Test::Unit::TestCase
  include IndexTest

  DIR = "/tmp/sphinx-index-test"
  CONF_FN = File.join DIR, "sphinx.conf"
  BIN_DIR = "sphinx-binaries"

  def setup
    FileUtils.rm_rf DIR
    FileUtils.mkdir DIR

    c = Heliotrope::Indexes::SphinxConfig.new DIR, CONF_FN, BIN_DIR do |c|
      c.field :body
      c.field :subject
      c.field :label, :method => :label_text
      c.attr :date, :type => :timestamp
    end

    @i = Heliotrope::Indexes::SphinxIndex.new c
    @i.start
  end

  def teardown
    @i.stop
  end
end

end
end
