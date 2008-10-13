require 'test/unit'

class Test::Unit::TestCase
  def assert_includes value, stuff
    value = [*value].sort
    assert((stuff & value).sort == value, "#{stuff.inspect} does not include #{value.inspect}")
  end

  def assert_doesnt_include value, stuff
    value = [*value].sort
    assert((stuff & value).empty?, "#{stuff.inspect} includes #{value.inspect}")
  end
end
