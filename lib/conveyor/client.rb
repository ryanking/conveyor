require 'net/http'

module Conveyor
  class Client
    def initialize host, port = 8011
      @host = host
      @port = port
      connect!
    end
    
    def connect!
      @conn = Net::HTTP.start(@host, @port)
    end

    def create_channel channel_name
      @conn.put("/channels/#{channel_name}", nil, {'Content-Type' => 'application/octet-stream'})
    end

    def post channel_name, content
      @conn.post("/channels/#{channel_name}", content, {'Content-Type' => 'application/octet-stream', 'Date' => Time.now.gmtime.to_s})
    end
    
    def get channel_name, id
      @conn.get("/channels/#{channel_name}/#{id}").body
    end

    def get_next channel_name, group=nil
      if group
        @conn.get("/channels/#{channel_name}?next&group=#{group}").body
      else
        @conn.get("/channels/#{channel_name}?next").body
      end
    end

    def channel_status channel_name
      JSON::parse(@conn.get("/channels/#{channel_name}").body)
    end

    def get_next_n channel_name, n = 10, group = nil
      if group
        JSON.parse(@conn.get("/channels/#{channel_name}?next&n=#{n}&group=#{group}").body)
      else
        JSON.parse(@conn.get("/channels/#{channel_name}?next&n=#{n}").body)
      end
    end
    
    def rewind channel_name, id, group=nil
      if group
        @conn.post("/channels/#{channel_name}?rewind_id=#{id}&group=#{group}", nil)
      else
        @conn.post("/channels/#{channel_name}?rewind_id=#{id}", nil)
      end
    end
  end
end