require 'rubygems'
require 'mongrel'
require 'conveyor/channel'
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

module Conveyor
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
        @channels[channel_name] = Conveyor::Channel.new(File.join(@data_directory, channel_name))
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
          if @channels.key?(m.captures[0])
            if request.params.include?('HTTP_DATE') && d = Time.parse(request.params['HTTP_DATE'])
              id = @channels[m.captures[0]].post(request.body.read)
              response.start(202) do |head, out|
                head["Location"] = "/channels/#{m.captures[0]}/#{id}"
              end
            else
              response.start(400) do |head, out|
                out.write "A valid Date header is required for all POSTs."
              end
            end
          end

        elsif request.get?
          headers = content = nil
          if m = request.path_match(%r{/channels/(.*)/(\d+)})
            if @channels.key?(m.captures[0])
              headers, content = @channels[m.captures[0]].get(m.captures[1].to_i)
            end
          elsif m = request.path_match(%r{/channels/(.*)})
            if @channels.key?(m.captures[0])
              params = Mongrel::HttpRequest.query_parse(request.params['QUERY_STRING'])
              if params.key? 'next'
                if params.key? 'group'
                  headers, content = @channels[m.captures[0]].get_next_by_group(params['group'])
                else
                  headers, content = @channels[m.captures[0]].get_next
                end
              end
            end
          else
            response.start(200) do |head, out|
              out.write("fake!")
            end
          end

          if headers && content
            response.start(200) do |head, out|
              head['Content-Location'] = "/channels/#{m.captures[0]}/#{headers[:id]}"
              head['Content-MD5']      = headers[:hash]
              head['Content-Type']     = 'application/octet-stream'
              head['Last-Modified']    = Time.parse(headers[:time]).gmtime.to_s
              out.write content
            end
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