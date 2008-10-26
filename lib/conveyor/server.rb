require 'rubygems'
require 'conveyor/channel'
require 'fileutils'
require 'json'
require 'logger'
require 'pp'

module Conveyor
  class App
    def initialize(data_directory, *options)
      options = options.inject(){|(k, v), m| m[k] = v; m}
      @data_directory = data_directory
      @log_directory  = options[:log_directory]
      @unsafe_mode    = options[:unsafe_mode] # allows deleting of channels. REALLY UNSAFE!
      @verbose        = options[:verbose]

      if @log_directory
        @logger = Logger.new File.join(@log_directory, 'conveyor.log')
      else
        @logger = Logger.new '/dev/null'
      end

      t0 = Time.now
      i "reading data"

      @channels = {}
      Dir.entries(@data_directory).each do |e|
        if !['.', '..'].include?(e) && File.directory?(File.join(@data_directory, e)) && Channel.valid_channel_name?(e)
          i "initializing channel '#{e}'"
          @channels[e] = Channel.new(File.join(@data_directory, e))
        end
      end

      i "done reading data (took #{Time.now - t0} sec.)"

      @requests = 0
    end

    def path_match env, pattern
      env["REQUEST_PATH"].match(pattern)
    end

    def create_new_channel channel_name
      @channels[channel_name] = Conveyor::Channel.new(File.join(@data_directory, channel_name))
    end

    def i msg
      @logger.info msg
    end

    def put request, channel
      if Channel.valid_channel_name?(channel)
        if !@channels.key?(channel)
          create_new_channel channel
          i "#{request.env["REMOTE_ADDR"]} PUT #{request.fullpath} 201"
          [201, {}, "created channel #{channel}"]
        else
          i "#{request.env["REMOTE_ADDR"]} PUT #{request.fullpath} 202"
          [202, {}, "channel already exists. didn't do anything"]
        end
      else
        i "#{request.env["REMOTE_ADDR"]} GET #{request.fullpath} 406"
        [406, {}, "invalid channel name (#{channel}). must match #{Channel::NAME_PATTERN}"]
      end
    end

    def post request, channel
      if @channels.key?(channel)
        if request.params.key?('rewind_id')
          if request.params['group']
            @channels[channel].rewind(:id => request.params['rewind_id'], :group => request.params['group']).to_i # TODO make sure this is an integer
            [200, {}, "iterator rewound to #{request.params['rewind_id']}"]
          else
            @channels[channel].rewind(:id => request.params['rewind_id']).to_i # TODO make sure this is an integer
            [200, {}, "iterator rewound to #{request.params['rewind_id']}"]
          end
        elsif request.params.key?('rewind_time')
          if request.params['group']
            @channels[channel].rewind(:time => request.params['rewind_time'].to_i, :group => request.params['group']).to_i # TODO make sure this is an integer
            [200, {}, "iterator rewound to #{request.params['rewind_id']}"]
          else
            @channels[channel].rewind(:time => request.params['rewind_time'].to_i) # TODO make sure this is an integer
            [200, {}, "iterator rewound to #{request.params['rewind_time']}"]
          end
        else
          if request.env.key?('HTTP_DATE') && d = Time.parse(request.env['HTTP_DATE'])
            id = @channels[channel].post(request.env['rack.input'].read)
            i "#{request.env["REMOTE_ADDR"]} POST #{request.env["REQUEST_PATH"]} 202"
            [202, {"Location" => "/channels/#{channel}/#{id}"}, ""]
          else
            i "#{request.env["REMOTE_ADDR"]} POST #{request.env["REQUEST_PATH"]} 400"
            [400, {}, "A valid Date header is required for all POSTs."]
          end
        end
      elsif Channel.valid_channel_name?(channel)
        create_new_channel(channel)
        post(request, channel)
      else
        i "#{env["REMOTE_ADDR"]} #{env["REQUEST_METHOD"]} #{env["REQUEST_PATH"]} 404"
        [404, {}, '']
      end
    end

    def get request
      headers = content = nil
      if m = request.path_info.match(%r{/(.*)/(\d+)})
        if @channels.key?(m.captures[0])
          headers, content = @channels[m.captures[0]].get(m.captures[1].to_i, true)
        end
      elsif m = request.path_info.match(%r{/(.*)})
        if @channels.key?(m.captures[0])
          if request.params.key? 'next'
            if request.params.key? 'group'
              if request.params.key? 'n'
                list = @channels[m.captures[0]].get_next_n_by_group(request.params['n'].to_i, request.params['group'])
              else
                headers, content = @channels[m.captures[0]].get_next_by_group(request.params['group'])
              end
            else
              if request.params.key? 'n'
                list = @channels[m.captures[0]].get_next_n(request.params['n'].to_i).map do |i|
                  {:data => i[1], :hash => i[0][:hash], :id => i[0][:id]}
                end
              else
                headers, content = @channels[m.captures[0]].get_next
              end
            end
          elsif request.params.key? 'after'
            headers, content = @channels[m.captures[0]].get_nearest_after_timestamp(request.params['after'].to_i)
          else
            return [200, {}, @channels[m.captures[0]].status.to_json]
          end
        end
      else
        return [200, {}, "fake!"]
      end

      if headers && content
        i "#{request.env["REMOTE_ADDR"]} GET #{request.env["REQUEST_PATH"]} 200 #{headers[:id]} #{headers[:length]} #{headers[:hash]}"
        return [
          200,
          {
            'Content-Location' => "/channels/#{m.captures.first}/#{headers[:id]}",
            'Content-MD5'      => headers[:hash],
            'Content-Type'     => 'application/octet-stream',
            'Last-Modified'    => Time.at(headers[:time]).gmtime.to_s,
          },
          content
        ]
      elsif list
        return [200, {}, list.to_json]
      end

      i "#{request.env["REMOTE_ADDR"]} #{request.env["REQUEST_METHOD"]} #{request.fullpath} 404"
      return [404, {}, '']
    end

    def delete request, channel
      if @channels.key?(channel)
        @channels[channel].delete!
        @channels.delete(channel)
        [200, {}, "Channel '#{channel}' deleted."]
      end
    end

    def call(env)
      if @verbose
        pp env
      end
      request = Rack::Request.new(env)
      @requests += 1
      if request.put?  && m = request.path_info.match(%r{/(.*)})
        put(request, m.captures[0])
      elsif request.post? && m = request.path_info.match(%r{/(.*)})
        post(request, m.captures[0])
      elsif @unsafe_mode && request.delete? && m = request.path_info.match(%r{/(.*)})
        delete(request, m.captures[0])
      elsif request.get?
        get(request)
      else
        i "#{env["REMOTE_ADDR"]} #{env["REQUEST_METHOD"]} #{env["REQUEST_PATH"]} 404"
        [404, {}, '']
      end
    end
  end
end