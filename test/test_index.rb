require "test/unit"
require 'fileutils'
require "feeder-ng/index"

class TestFeederNGIndex < Test::Unit::TestCase
  include FeederNG
  def test_init
    Dir.mkdir('/tmp/bar') unless File.exists? '/tmp/bar' # TODO auto-create directory
    assert_nothing_raised do
      Index.new '/tmp/bar'
    end
  end

  def test_post
    Dir.mkdir('/tmp/bar') unless File.exists? '/tmp/bar' #TODO auto-create directory
    assert_nothing_raised do
      w = Index.new '/tmp/bar'
      w.post 'foo'
      assert_equal 1, w.instance_variable_get(:@last_id)
    end
  end
  
  def test_parse_headers
    i = Index.new '/tmp/foo'
    [
      ["1 2008-01-08T13:04:40-08:00 0 3 acbd18db4cc2f85cedef654fccc4a4d8\n", 
       {:id => 1, :time => "2008-01-08T13:04:40-08:00", :offset => 0, :length => 3, :hash => "acbd18db4cc2f85cedef654fccc4a4d8"}
      ]
    ].each do |(str, ret)|
      assert_equal ret, i.parse_headers(str)
    end
  end
end