require 'rubygems'
require 'mongrel'
require 'feeder-ng/channel'

module FeederNG
  class Server < Mongrel::HttpServer

    class ChannelsHandler < Mongrel::HttpHandler

      def initialize data_directory
        @data_directory = data_directory
        @channels = {}
        # TODO read channels in from data directory
      end

      def create_new_channel channel_name
        # TODO validate channel name
        @channels[channel_name] = FeederNG::Channel.new(File.join(@data_directory, channel_name))
      end
      
      def process request, response
        if request.params["REQUEST_METHOD"] == "PUT" && request.params["REQUEST_PATH"].match(%r{/channels/(.*)})
          create_new_channel $1
          response.start(201) do |head, out|
            out.write("created channel #{$1}")
          end
        elsif request.params["REQUEST_METHOD"] == "POST" && request.params["REQUEST_PATH"].match(%r{/channels/(.*)}) &&
          @channels.keys.include?($1)
          id = @channels[$1].post(request.body.read)
          response.start(202) do |head, out|
            head["Location"] = "/channels/#{$1}/#{id}"
          end
        elsif request.params["REQUEST_METHOD"] == "GET" && request.params["REQUEST_PATH"].match(%r{/channels/(.*)/(\d+)}) &&
          @channels.keys.include?($1)
          
          headers, content = @channels[$1].get($2.to_i)
          if headers && content
            response.start(200) do |head, out|
              out.write content
            end
          end
        else
          response.start(200) do |head, out|
            out.write("fake!")
          end
        end
      end
    end

    def initialize(host, port, data_directory)
      super(host, port)
      ch = ChannelsHandler.new(data_directory)
      register("/channels", ch)
    end
  end
end