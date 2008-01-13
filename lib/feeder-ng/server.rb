require 'rubygems'
require 'mongrel'
require 'feeder-ng/channel'
require 'fileutils'

module FeederNG
  class Server < Mongrel::HttpServer

    class ChannelsHandler < Mongrel::HttpHandler

      def initialize data_directory
        @data_directory = data_directory
        @channels = {}
        Dir.entries(@data_directory).each do |e|
          if !['.', '..'].include?(e) && File.directory?(File.join(@data_directory, e))
            @channels[e] = Channel.new(File.join(@data_directory, e))
          end
        end
      end

      def create_new_channel channel_name
        @channels[channel_name] = FeederNG::Channel.new(File.join(@data_directory, channel_name))
      end
      
      def valid_channel_name? name
        !!name.match(%r{\A[a-zA-Z\-0-9]+\Z})
      end
      
      def process request, response
        if request.params["REQUEST_METHOD"] == "PUT" && request.params["REQUEST_PATH"].match(%r{/channels/(.*)})
          if Channel.valid_channel_name?($1)
            create_new_channel $1
            response.start(201) do |head, out|
              out.write("created channel #{$1}")
            end
          else
            response.start(406) do |head, out|
              out.write("invalid channel name. must match #{Channel::NAME_PATTERN}")
            end
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