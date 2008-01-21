require "test/unit"
require 'fileutils'
require "feeder-ng/replicated_channel"

class TestReplicatedChannel < Test::Unit::TestCase
  def test_basic
    FileUtils.rm_r '/tmp/foo' rescue nil
    FileUtils.rm_r '/tmp/bar' rescue nil
    
    c1 = FeederNG::ReplicatedChannel.new '/tmp/foo'
    c2 = FeederNG::ReplicatedChannel.new '/tmp/bar'
    
    c1.peers << c2
    c2.peers << c1

    c1.post('foo', Time.now)
    c2.post('bar', Time.now)

    c1.post('not', Time.now)
    c2.post('not', Time.now)

    c1.commit_thread.run
    c2.commit_thread.run

    assert_equal 'foo', c1.get(1)[1]
    assert_equal 'foo', c2.get(1)[1]

    assert_equal 'bar', c1.get(2)[1]
    assert_equal 'bar', c2.get(2)[1]
    
    assert_equal c1.get(1), c2.get(1)
    assert_equal c1.get(2), c2.get(2)
  end
  
  def test_more
    FileUtils.rm_r '/tmp/foo' rescue nil
    FileUtils.rm_r '/tmp/bar' rescue nil
    
    c1 = FeederNG::ReplicatedChannel.new '/tmp/foo'
    c2 = FeederNG::ReplicatedChannel.new '/tmp/bar'
    
    c1.peers << c2
    c2.peers << c1

    channels = [c1, c2]
    data = %w[1 2 3 4 5 6 7 8 9 10]

    data.each_with_index do |d, i|
      channels[i % 2].post(d, Time.now)
    end

    c1.commit_thread.run
    c2.commit_thread.run

    c1d = (1..10).collect{|i| c1.get(i)}
    c2d = (1..10).collect{|i| c2.get(i)}

    assert_equal c1d, c2d
  end
end