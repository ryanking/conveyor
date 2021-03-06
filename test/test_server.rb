require "test/unit"
require "conveyor/server"
require 'net/http'
require 'conveyor/client'
require 'rack'

class TestConveyorServer < Test::Unit::TestCase
  include Conveyor

  FileUtils.rm_r('/tmp/asdf') rescue nil
  FileUtils.mkdir('/tmp/asdf')

  Thread.start do
    app = Rack::Builder.new do
      map '/channels' do
        run Conveyor::App.new('/tmp/asdf', :unsafe_mode => true)
      end
    end

    Rack::Handler::Mongrel.run(app, :Host => '0.0.0.0', :Port => 8011)
   end
   sleep 1

  def test_channels
    Net::HTTP.start("localhost", 8011) do |h|
      req = h.get('/channels')
      assert_equal Net::HTTPOK, req.class
    end
  end

  def test_create_channel
    chan = 'test_create_channel'
    Net::HTTP.start('localhost', 8011) do |h|
      req = h.put("/channels/#{chan}", '', {'Content-Type' => 'application/octet-stream'})
      assert_equal Net::HTTPCreated, req.class

      req = h.post("/channels/#{chan}", 'foo', {'Content-Type' => 'application/octet-stream', 'Date' => Time.now.to_s})
      assert_equal Net::HTTPAccepted, req.class
    end
  end

  def test_post
    chan = 'test_post'
    Net::HTTP.start('localhost', 8011) do |h|
      req = h.put("/channels/#{chan}", '', {'Content-Type' => 'application/octet-stream'})
      assert_equal Net::HTTPCreated, req.class

      data =
      ["ZqZyDN2SouQCYEHYS0LuM1XeqsF0MKIbFEBE6xQ972VqEcjs21wJSosvZMWEH1lq5ukTq4Ze", 
        "5sgCbNpumntlHC2jl6uXcW3Wz1RGTc5lqeGhpH2ZaCtAOc61TLBmLPUzPWVeJzkfr6qEQqTkuPK5kCE54u6iiTVFArMPSUy3oo", 
        "6qPDKL09AquFvgj9Zv9CFY2dn0x", "84ReeNklrWJeIu2USbBtwoUnkDwUgU8lNjJ", 
        "WUSYY2dCBdDdZEiGWtyfC5yGKVMgDhzBhyNLwcefxa49fED1Sf05f8MlgXOBx6n5I6Ae2Wy3Mds", 
        "uAlUDvngWqDl3PaRVl1i9RcwDIvJlNp6yMy9RQgVsucwNvKaSOQlJMarWItKy8zT2ON08ElKkZ2aQJlb45Z8FwfE0xh8sA", 
        "NxWmEBmJp0uiNRhyxa26frQjfFaNERmZbConrytNQKnHfilFsZWAo0Qy8eVKgq", "ajq3i5ksiBovQYfvj", 
        "yY3vhjeq","2IDeF0ccG8tRZIZSekz6fUii29"
      ]
        
      data.each do |d|
        req = h.post("/channels/#{chan}", d, {'Content-Type' => 'application/octet-stream', 'Date' => Time.now.to_s})
        assert_equal Net::HTTPAccepted, req.class
      end

      data.each_with_index do |d, i|
        req = h.get("/channels/#{chan}/#{i+1}")
        assert_equal d, req.body
      end
    end
  end

  def test_invalid_channel
    Net::HTTP.start('localhost', 8011) do |h|
      req = h.put('/channels/|', '', {'Content-Type' => 'application/octet-stream'})
      assert_equal Net::HTTPNotAcceptable, req.class
    end
    
  end

  def test_get_next
    Net::HTTP.start('localhost', 8011) do |h|
      req = h.put('/channels/bar', '', {'Content-Type' => 'application/octet-stream'})
      assert_equal Net::HTTPCreated, req.class

      data =
      ["ZqZyDN2SouQCYEHYS0LuM1XeqsF0MKIbFEBE6xQ972VqEcjs21wJSosvZMWEH1lq5ukTq4Ze", 
        "5sgCbNpumntlHC2jl6uXcW3Wz1RGTc5lqeGhpH2ZaCtAOc61TLBmLPUzPWVeJzkfr6qEQqTkuPK5kCE54u6iiTVFArMPSUy3oo", 
        "6qPDKL09AquFvgj9Zv9CFY2dn0x", "84ReeNklrWJeIu2USbBtwoUnkDwUgU8lNjJ", 
        "WUSYY2dCBdDdZEiGWtyfC5yGKVMgDhzBhyNLwcefxa49fED1Sf05f8MlgXOBx6n5I6Ae2Wy3Mds", 
        "uAlUDvngWqDl3PaRVl1i9RcwDIvJlNp6yMy9RQgVsucwNvKaSOQlJMarWItKy8zT2ON08ElKkZ2aQJlb45Z8FwfE0xh8sA", 
        "NxWmEBmJp0uiNRhyxa26frQjfFaNERmZbConrytNQKnHfilFsZWAo0Qy8eVKgq", "ajq3i5ksiBovQYfvj", 
        "yY3vhjeq","2IDeF0ccG8tRZIZSekz6fUii29"]
        
      data.each do |d|
        req = h.post('/channels/bar', d, {'Content-Type' => 'application/octet-stream', 'Date' => Time.now.to_s})
        assert_equal Net::HTTPAccepted, req.class
      end

      data.each_with_index do |d, i|
        req = h.get("/channels/bar?next")
        assert_equal d, req.body
      end
    end
  end

  def test_status
    chan = 'test_status'
    Net::HTTP.start('localhost', 8011) do |h|
      req = h.put("/channels/#{chan}", '', {'Content-Type' => 'application/octet-stream'})
      assert_equal Net::HTTPCreated, req.class

      data = ["ZqZyDN2SouQCYEHYS0LuM1XeqsF0MKIbFEBE6xQ972VqEcjs21wJSosvZMWEH1lq5ukTq4Ze"]

      data.each do |d|
        req = h.post("/channels/#{chan}", d, {'Content-Type' => 'application/octet-stream', 'Date' => Time.now.to_s})
        assert_equal Net::HTTPAccepted, req.class
      end

      req = h.get("/channels/#{chan}")
      assert_kind_of Net::HTTPOK, req
      json = {
        "iterator_groups" => {},
        "blocks" => 1,
        "directory"=>"/tmp/asdf/#{chan}",
        "data_files"=>[{"path"=>"/tmp/asdf/#{chan}/0","bytes"=>122}],
        "iterator"=>1,
        "last_id" => 1,
        "block_cache_keys" => []
      }
      assert_equal json, JSON::parse(req.body)
      
    end
  end

  def test_rewinding
    chan = 'test_rewinding'
    Net::HTTP.start('localhost', 8011) do |h|
      req = h.put("/channels/#{chan}", '', {'Content-Type' => 'application/octet-stream'})
      assert_equal Net::HTTPCreated, req.class

      data =
      ["ZqZyDN2SouQCYEHYS0LuM1XeqsF0MKIbFEBE6xQ972VqEcjs21wJSosvZMWEH1lq5ukTq4Ze"]
        
      data.each do |d|
        req = h.post("/channels/#{chan}", d, {'Content-Type' => 'application/octet-stream', 'Date' => Time.now.to_s})
        assert_equal Net::HTTPAccepted, req.class
      end

      req = h.get("/channels/#{chan}?next")

      assert_kind_of Net::HTTPOK, req
      assert_equal data[0], req.body
      
      req = h.get("/channels/#{chan}?next")
      
      assert_kind_of Net::HTTPNotFound, req
      
      req = h.post("/channels/#{chan}?rewind_id=1", nil, {'Content-Type' => 'application/octet-stream'})
      assert_kind_of Net::HTTPOK, req
      
      req = h.get("/channels/#{chan}?next")
      
      assert_kind_of Net::HTTPOK, req
      assert_equal data[0], req.body
      
      req = h.get("/channels/#{chan}?next")
      assert_kind_of Net::HTTPNotFound, req
    end
  end

  def test_group_rewind
    chan = 'test_group_rewind'
    c = Client.new 'localhost', chan
    c.create_channel
    c.post 'foo'
    
    assert_equal 'foo', c.get_next('bar')
    c.rewind(:id => 1, :group => 'bar')
    assert_equal 'foo', c.get_next('bar')
    c.rewind(:id => 1, :group => 'bar')
  end

  def test_get_next_by_group
    chan = 'test_get_next_by_group'
    c = Conveyor::Client.new 'localhost', chan
    c.create_channel
    c.post 'foo'
    c.post 'bar'
    c.post 'bam'

    group = 'bam'

    assert_equal 'foo', c.get_next(group)
    assert_equal 'bar', c.get_next(group)
    assert_equal 'bam', c.get_next(group)
    assert_equal '',   c.get_next(group)

    group = 'bar'
    assert_equal 'foo', c.get_next(group)
    assert_equal 'bar', c.get_next(group)
    assert_equal 'bam', c.get_next(group)
    assert_equal '',    c.get_next(group)
  end

  def test_get_next_n
    chan = 'test_get_next_n'
    c = Client.new 'localhost', chan
    c.create_channel
    100.times {|i| c.post i.to_s}

    10.times do |j|
      r = c.get_next_n 10
      r.each_with_index do |f, i|
        assert_equal((j*10 + i)+1,                           f["id"])
        assert_equal(Digest::MD5.hexdigest((j*10 + i).to_s), f["hash"])
        assert_equal((j*10 + i).to_s,                        f["data"])
      end
    end
    
    100.times {|i| c.post i.to_s}

    10.times do |j|
      r = c.get_next_n 10
      r.each_with_index do |f, i|
        assert_equal(Digest::MD5.hexdigest((j*10 + i).to_s), f["hash"])
        assert_equal((100 + j*10 + i)+1,                     f["id"])
        assert_equal((j*10 + i).to_s,                        f["data"])
      end
    end
  end

  def test_get_next_n_by_group
    chan = 'test_get_next_n_by_group'
    c = Client.new 'localhost', chan
    c.create_channel
    100.times {|i| c.post i.to_s}

    10.times do |j|
      r = c.get_next_n 10, 'foo'
      r.each_with_index do |f, i|
        assert_equal(Digest::MD5.hexdigest((j*10 + i).to_s), f[0]["hash"])
        assert_equal((j*10 + i)+1,                           f[0]["id"])
        assert_equal((j*10 + i).to_s,                        f[1])
      end
    end

    assert_equal [], c.get_next_n(10, 'foo')

    10.times do |j|
      r = c.get_next_n 10, 'bar'
      r.each_with_index do |f, i|
        assert_equal Digest::MD5.hexdigest((j*10 + i).to_s), f[0]["hash"]
        assert_equal((j*10 + i)+1,                           f[0]["id"])
        assert_equal((j*10 + i).to_s,                        f[1])
      end
    end
    assert_equal [], c.get_next_n(10, 'bar')
  end

  def test_delete
    chan = "test_delete"
    Net::HTTP.start("localhost", 8011) do |h|
      r = h.put("/channels/#{chan}", "")
      assert_kind_of Net::HTTPCreated, r
      10.times {|i| h.post("/channels/#{chan}", i.to_s, {'Content-Type' => 'application/octet-stream', 'Date' => Time.now.gmtime.to_s})}
      10.times {|i| assert_equal(i.to_s, h.get("/channels/#{chan}?next").body)}
      10.times {|i| assert_equal(i.to_s, h.get("/channels/#{chan}/#{i+1}").body)}
      r = h.delete("/channels/#{chan}")
      assert_kind_of Net::HTTPOK, r
      
      r = h.put("/channels/#{chan}", "")
      assert_kind_of Net::HTTPCreated, r
      10.times {|i| h.post("/channels/#{chan}", i.to_s, {'Content-Type' => 'application/octet-stream', 'Date' => Time.now.gmtime.to_s})}
      10.times {|i| assert_equal(i.to_s, h.get("/channels/#{chan}?next").body)}
      10.times {|i| assert_equal(i.to_s, h.get("/channels/#{chan}/#{i+1}").body)}
    end
  end

  def test_autocreate_channel
    chan = "test_autocreate_channel"
    c = Client.new 'localhost', chan
    c.post 'foo'
    assert_equal 'foo', c.get_next
  end

  def test_get_by_timestamp
    chan = 'test_get_by_timestamp'
    c = Client.new('localhost', chan)

    10.times{|i| c.post(i.to_s)}
    assert_equal '0', c.get_nearest_after_timestamp(0)
    assert_equal '', c.get_nearest_after_timestamp(2**32)

    t0 = Time.now.to_i
    sleep 1
    10.times{|i| c.post((10 + i).to_s)}
    assert_equal '10', c.get_nearest_after_timestamp(t0)
  end

  def test_rewind_to_timestamp
    chan = 'test_rewind_to_timestamp'
    c = Client.new('localhost', chan)

    10.times{|i| c.post(i.to_s)}
    10.times{|i| assert_equal i.to_s, c.get_next}

    c.rewind :time => 0
    10.times{|i| assert_equal i.to_s, c.get_next}

    t0 = Time.now.to_i + 1
    c.rewind :time => t0
    assert_equal '', c.get_next
  end
  
  def test_rewind_group_to_timestamp
    chan = 'test_rewind_group_to_timestamp'
    group = 'foo'
    c = Client.new('localhost', chan)

    10.times{|i| c.post(i.to_s)}
    10.times{|i| assert_equal i.to_s, c.get_next(group)}

    c.rewind :time => 0, :group => group
    10.times{|i| assert_equal i.to_s, c.get_next(group)}

    t0 = Time.now.to_i + 1
    c.rewind :time => t0, :group => group
    assert_equal '', c.get_next(group)
  end
end

