require 'net/http'

module Conveyor
  class Client
    def initialize host, channel, port = 8011
      @host    = host
      @port    = port
      @channel = channel
      connect!
    end

    def connect!
      @conn = Net::HTTP.start(@host, @port)
    end

    def create_channel
      @conn.put("/channels/#{@channel}", nil, {'Content-Type' => 'application/octet-stream'})
    end

    def post content
      @conn.post("/channels/#{@channel}", content, {'Content-Type' => 'application/octet-stream', 'Date' => Time.now.gmtime.to_s})
    end
    
    def get id
      @conn.get("/channels/#{@channel}/#{id}").body
    end

    def get_next group=nil
      if group
        @conn.get("/channels/#{@channel}?next&group=#{group}").body
      else
        @conn.get("/channels/#{@channel}?next").body
      end
    end

    def status
      JSON::parse(@conn.get("/channels/#{@channel}").body)
    end

    def get_next_n n = 10, group = nil
      if group
        JSON.parse(@conn.get("/channels/#{@channel}?next&n=#{n}&group=#{group}").body)
      else
        JSON.parse(@conn.get("/channels/#{@channel}?next&n=#{n}").body)
      end
    end
    
    def rewind *opts
      opts = opts.inject{|h,m| m.merge(h)}
      if opts.key?(:id) && opts.key?(:group)
        @conn.post("/channels/#{@channel}?rewind_id=#{opts[:id]}&group=#{opts[:group]}", nil)
      elsif opts.key?(:id)
        @conn.post("/channels/#{@channel}?rewind_id=#{opts[:id]}", nil)
      elsif opts.key?(:time)
        @conn.post("/channels/#{@channel}?rewind_time=#{opts[:time].to_i}", nil)
      end
    end

    def get_nearest_after_timestamp timestamp
      @conn.get("/channels/#{@channel}?after=#{timestamp.to_i}").body
    end
  end
end