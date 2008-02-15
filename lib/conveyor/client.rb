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
      @conn.post("/channels/#{channel_name}", content, {'Content-Type' => 'application/octet-stream', 'Date' => Time.now.to_s})
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
  end
end