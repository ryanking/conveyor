require 'rubygems'
require 'mongrel'
require 'feeder-ng/channel'
require 'fileutils'

class Mongrel::HttpRequest
  def put?
    params["REQUEST_METHOD"] == "PUT"
  end
  
  def post?
    params["REQUEST_METHOD"] == "POST"
  end
  
  def get?
    params["REQUEST_METHOD"] == "GET"
  end

  def path_match pattern
    params["REQUEST_PATH"].match(pattern)
  end
end

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

      def process request, response
        if request.put? && m = request.path_match(%r{/channels/(.*)})
          if Channel.valid_channel_name?(m.captures[0])
            create_new_channel m.captures[0]
            response.start(201) do |head, out|
              out.write("created channel #{m.captures[0]}")
            end
          else
            response.start(406) do |head, out|
              out.write("invalid channel name. must match #{Channel::NAME_PATTERN}")
            end
          end
        elsif request.post? && m = request.path_match(%r{/channels/(.*)})
          if @channels.keys.include?(m.captures[0])
            id = @channels[m.captures[0]].post(request.body.read)
            response.start(202) do |head, out|
              head["Location"] = "/channels/#{m.captures[0]}/#{id}"
            end
          end
        elsif request.get? && m = request.path_match(%r{/channels/(.*)/(\d+)})
          if @channels.keys.include?(m.captures[0])
            headers, content = @channels[m.captures[0]].get(m.captures[1].to_i)
            if headers && content
              response.start(200) do |head, out|
                out.write content
              end
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