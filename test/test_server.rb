require "test/unit"
require "conveyor/server"
require 'net/http'

class TestConveyorServer < Test::Unit::TestCase
  def setup
    FileUtils.rm_r('/tmp/asdf') rescue nil
    FileUtils.mkdir('/tmp/asdf')
    @server = Conveyor::Server.new("127.0.0.1", 8888, '/tmp/asdf')
    @server.run
  end
  
  def teardown
    @server.stop
  end
  
  def test_channels
    Net::HTTP.start("localhost", 8888) do |h|
      req = h.get('/channels')
      assert_equal Net::HTTPOK, req.class
    end
  end
  
  def test_create_channel
    Net::HTTP.start('localhost', 8888) do |h|
      req = h.put('/channels/foo', '', {'Content-Type' => 'application/octet-stream'})
      assert_equal Net::HTTPCreated, req.class

      req = h.post('/channels/foo', 'foo', {'Content-Type' => 'application/octet-stream', 'Date' => Time.now.to_s})
      assert_equal Net::HTTPAccepted, req.class
    end
  end
  
  def test_post
    Net::HTTP.start('localhost', 8888) do |h|
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
        req = h.get("/channels/bar/#{i+1}")
        assert_equal d, req.body
      end
    end
  end
  
  def test_invalid_channel
    Net::HTTP.start('localhost', 8888) do |h|
      req = h.put('/channels/|', '', {'Content-Type' => 'application/octet-stream'})
      assert_equal Net::HTTPNotAcceptable, req.class
    end
    
  end

  def test_get_next
    Net::HTTP.start('localhost', 8888) do |h|
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
end