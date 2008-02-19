require 'rubygems'
require 'mongrel'
require 'conveyor/channel'
require 'fileutils'
require 'json'
require 'logger'

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
  
  # An HTTP server for Conveyor.
  class Server < Mongrel::HttpServer

    # A Mongrel handler for multiple Conveyor Channels.
    class ChannelsHandler < Mongrel::HttpHandler

      def initialize data_directory, log_directory=nil
        @data_directory = data_directory
        if log_directory
          @logger = Logger.new File.join(log_directory, 'conveyor.log')
        else
          @logger = Logger.new '/dev/null'
        end

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

      def i str
        @logger.info str
      end

      def process request, response
        if request.put? && m = request.path_match(%r{/channels/(.*)})
          if Channel.valid_channel_name?(m.captures[0])
            if !@channels.key?(m.captures[0])
              create_new_channel m.captures[0]
              response.start(201) do |head, out|
                out.write("created channel #{m.captures[0]}")
              end
              i "#{request.params["REMOTE_ADDR"]} PUT #{request.params["REQUEST_PATH"]} 201"
            else
              response.start(202) do |head, out|
                out.write("channel already exists. didn't do anything")
              end
              i "#{request.params["REMOTE_ADDR"]} PUT #{request.params["REQUEST_PATH"]}   "
            end
          else
            response.start(406) do |head, out|
              out.write("invalid channel name. must match #{Channel::NAME_PATTERN}")
              i "#{request.params["REMOTE_ADDR"]} GET #{request.params["REQUEST_PATH"]} 406"
            end
          end
        elsif request.post? && m = request.path_match(%r{/channels/(.*)})
          if @channels.key?(m.captures[0])
            params = Mongrel::HttpRequest.query_parse(request.params['QUERY_STRING'])
            if params.key?('rewind_id')
              if params['group']
                @channels[m.captures[0]].rewind(:id => params['rewind_id'], :group => params['group']).to_i # TODO make sure this is an integer
                response.start(200) do |head, out|
                  out.write "iterator rewound to #{params['rewind_id']}"
                end
              else
                @channels[m.captures[0]].rewind(:id => params['rewind_id']).to_i # TODO make sure this is an integer
                response.start(200) do |head, out|
                  out.write "iterator rewound to #{params['rewind_id']}"
                end
              end
            else
              if request.params.include?('HTTP_DATE') && d = Time.parse(request.params['HTTP_DATE'])
                id = @channels[m.captures[0]].post(request.body.read)
                response.start(202) do |head, out|
                  head["Location"] = "/channels/#{m.captures[0]}/#{id}"
                end
                i "#{request.params["REMOTE_ADDR"]} POST #{request.params["REQUEST_PATH"]} 202"
              else
                response.start(400) do |head, out|
                  out.write "A valid Date header is required for all POSTs."
                end
                i "#{request.params["REMOTE_ADDR"]} POST #{request.params["REQUEST_PATH"]} 400"
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
                  if params.key? 'n'
                    list = @channels[m.captures[0]].get_next_n_by_group(params['n'].to_i, params['group'])
                  else
                    headers, content = @channels[m.captures[0]].get_next_by_group(params['group'])
                  end
                else
                  if params.key? 'n'
                    list = @channels[m.captures[0]].get_next_n(params['n'].to_i)
                    list = list.map do |i|
                      {:data => i[1], :hash => i[0][:hash], :id => i[0][:id]}
                    end
                  else
                    headers, content = @channels[m.captures[0]].get_next
                  end
                end
              else
                response.start(200) do |head, out|
                  out.write @channels[m.captures[0]].status.to_json
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
            i "#{request.params["REMOTE_ADDR"]} GET #{request.params["REQUEST_PATH"]} 200 #{headers[:id]} #{headers[:length]} #{headers[:hash]}"
          elsif list
            response.start(200) do |head, out|
              out.write list.to_json
            end
          end
          
        end
      end
    end

    # +host+ and +port+ are passed along to Mongrel::HttpServer for TCP binding. +data_directory+ is used to store
    # all channel data and should be created before intializing a Server.
    def initialize(host, port, data_directory, log_directory = nil)
      super(host, port)
      ch = ChannelsHandler.new(data_directory, log_directory)
      register("/channels", ch)
    end
  end
end