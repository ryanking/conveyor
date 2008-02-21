require "test/unit"
require 'fileutils'
require "conveyor/channel"

class TestConveyorChannel < Test::Unit::TestCase
  include Conveyor
  def test_init
    FileUtils.rm_r('/tmp/bar')
    assert_nothing_raised do
      Channel.new '/tmp/bar'
    end
  end

  def test_post
    FileUtils.rm_r('/tmp/bar')
    assert_nothing_raised do
      w = Channel.new '/tmp/bar'
      w.post 'foo'
      assert_equal 1, w.instance_variable_get(:@last_id)
    end
  end
  
  def test_parse_headers
    i = Channel.new '/tmp/foo'
    [
      ["1 2008-01-08T13:04:40-08:00 0 3 acbd18db4cc2f85cedef654fccc4a4d8\n", 
       {:id => 1, :time => "2008-01-08T13:04:40-08:00", :offset => 0, :length => 3, :hash => "acbd18db4cc2f85cedef654fccc4a4d8", :file => nil}
      ]
    ].each do |(str, ret)|
      assert_equal ret, i.parse_headers(str)
    end
    
    [
      ["2 2008-01-08T13:04:40-08:00 0 3 acbd18db4cc2f85cedef654fccc4a4d8 1\n", 
       {:id => 2, :time => "2008-01-08T13:04:40-08:00", :offset => 0, :length => 3, :hash => "acbd18db4cc2f85cedef654fccc4a4d8", :file => 1}
      ]
    ].each do |(str, ret)|
      assert_equal ret, i.parse_headers(str, true)
    end
  end

  def test_init_existing
    FileUtils.rm_r('/tmp/bar') rescue nil
    c = Channel.new('/tmp/bar')
    c.post 'foo'
    c.post 'bar'
    c = nil
    GC.start
    
    d = Channel.new('/tmp/bar')
    assert_equal 'foo', d.get(1)[1]
    assert_equal 'bar', d.get(2)[1]
    
    d.post('bam')
    assert_equal 'bam', d.get(3)[1]
  end

  def test_get_next
    FileUtils.rm_r('/tmp/bar') rescue nil
    c = Channel.new('/tmp/bar')
    c.post 'foo'
    c.post 'bar'
    c.post 'bam'

    assert_equal 'foo', c.get_next[1]
    assert_equal 'bar', c.get_next[1]
    assert_equal 'bam', c.get_next[1]
    assert_equal nil, c.get_next
    assert_equal 4, c.status[:iterator][:position]
  end

  def test_get_next_interupted
    FileUtils.rm_r('/tmp/bar') rescue nil
    c = Channel.new('/tmp/bar')
    c.post 'foo'
    c.post 'bar'
    c.post 'bam'
    
    assert_equal 'foo', c.get_next[1]
    assert_equal 'bar', c.get_next[1]

    d = Channel.new('/tmp/bar')
    assert_not_equal c, d
    assert_equal 'bam', d.get_next[1]
    assert_equal nil, d.get_next
  end

  def test_get_next_by_group
    FileUtils.rm_r('/tmp/bar') rescue nil
    c = Channel.new('/tmp/bar')
    c.post 'foo'
    c.post 'bar'
    c.post 'bam'
    
    assert_equal 'foo', c.get_next_by_group('foo')[1]
    assert_equal 'bar', c.get_next_by_group('foo')[1]
    assert_equal 'bam', c.get_next_by_group('foo')[1]
    assert_equal nil, c.get_next_by_group('foo')
    
    assert_equal 'foo', c.get_next_by_group('bar')[1]
    assert_equal 'bar', c.get_next_by_group('bar')[1]
    assert_equal 'bam', c.get_next_by_group('bar')[1]
    assert_equal nil, c.get_next_by_group('bar')
  end

  def test_get_next_by_group_interupted
    FileUtils.rm_r('/tmp/bar') rescue nil
    c = Channel.new('/tmp/bar')
    c.post 'foo'
    c.post 'bar'
    c.post 'bam'
    
    assert_equal 'foo', c.get_next_by_group('foo')[1]
    assert_equal 'bar', c.get_next_by_group('foo')[1]
    assert_equal 'foo', c.get_next_by_group('bar')[1]
    assert_equal 'bar', c.get_next_by_group('bar')[1]

    c = nil
    GC.start
    c = Channel.new('/tmp/bar')

    assert_equal 'bam', c.get_next_by_group('foo')[1]
    assert_equal nil, c.get_next_by_group('foo')    
    assert_equal 'bam', c.get_next_by_group('bar')[1]
    assert_equal nil, c.get_next_by_group('bar')
  end

  def test_channel_status
    FileUtils.rm_r('/tmp/bar') rescue nil
    c = Channel.new('/tmp/bar')
    c.post 'foo'
    c.post 'bar'
    c.post 'bam'
    
    status = {
      :directory => '/tmp/bar',
      :index => {:size => 3},
      :data_files => [
        {:path => '/tmp/bar/0', :bytes => 273}
        ],
      :iterator => {:position => 1},
      :iterator_groups => {}
    }
    
    assert_equal(status, c.status)
  end

  def test_rewind
    FileUtils.rm_r('/tmp/bar') rescue nil
    c = Channel.new('/tmp/bar')
    c.post 'foo'

    assert_equal 'foo', c.get_next[1]
    c.rewind(:id => 1)
    assert_equal 'foo', c.get_next[1]
    c.rewind(:id => 1)

    d = Channel.new('/tmp/bar')
    assert_equal 'foo', d.get_next[1]
  end
  
  def test_group_rewind
    FileUtils.rm_r('/tmp/bar') rescue nil
    c = Channel.new('/tmp/bar')
    c.post 'foo'
    
    assert_equal 'foo', c.get_next_by_group('bar')[1]
    c.rewind(:id => 1, :group => 'bar')
    assert_equal 'foo', c.get_next_by_group('bar')[1]
    c.rewind(:id => 1, :group => 'bar')

    d = Channel.new('/tmp/bar')
    assert_equal 'foo', d.get_next_by_group('bar')[1]
  end
  
  def test_valid_name
    assert BaseChannel.valid_channel_name?(('a'..'z').to_a.join)
    assert BaseChannel.valid_channel_name?(('A'..'Z').to_a.join)
    assert BaseChannel.valid_channel_name?(('0'..'9').to_a.join)
    assert BaseChannel.valid_channel_name?('-')
    assert BaseChannel.valid_channel_name?('_')
  end

  def test_get_next_n
    FileUtils.rm_r '/tmp/asdfasdf' rescue nil
    c = Conveyor::Channel.new '/tmp/asdfasdf'
    100.times {|i| c.post i.to_s}

    12.times do |j|
      r = c.get_next_n 10
      r.each_with_index do |f, i|
        assert_equal Digest::MD5.hexdigest((j*10 + i).to_s), f[0][:hash]
        assert_equal((j*10 + i)+1,                           f[0][:id])
        assert_equal((j*10 + i).to_s,                        f[1])
      end
    end
    
    100.times {|i| c.post i.to_s}

    12.times do |j|
      r = c.get_next_n 10
      r.each_with_index do |f, i|
        assert_equal Digest::MD5.hexdigest((j*10 + i).to_s), f[0][:hash]
        assert_equal((100 + j*10 + i)+1,                     f[0][:id])
        assert_equal((j*10 + i).to_s,                        f[1])
      end
    end
  end

  def test_get_next_n_by_group
    FileUtils.rm_r '/tmp/asdfasdf'
    c = Conveyor::Channel.new '/tmp/asdfasdf'
    100.times {|i| c.post i.to_s}

    10.times do |j|
      r = c.get_next_n_by_group 10, 'foo'
      r.each_with_index do |f, i|
        assert_equal Digest::MD5.hexdigest((j*10 + i).to_s), f[0][:hash]
        assert_equal((j*10 + i)+1,                           f[0][:id])
        assert_equal((j*10 + i).to_s,                        f[1])
      end
    end

    assert_equal [], c.get_next_n_by_group(10, 'foo')

    10.times do |j|
      r = c.get_next_n_by_group 10, 'bar'
      r.each_with_index do |f, i|
        assert_equal Digest::MD5.hexdigest((j*10 + i).to_s), f[0][:hash]
        assert_equal((j*10 + i)+1,                           f[0][:id])
        assert_equal((j*10 + i).to_s,                        f[1])
      end
    end
    assert_equal [], c.get_next_n_by_group(10, 'bar')
  end
  
end