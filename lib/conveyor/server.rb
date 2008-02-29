require 'rubygems'
require 'mongrel'
require 'conveyor/channel'
require 'fileutils'
require 'json'
require 'logger'

module Conveyor
  class App
    def initialize(data_directory, *options)
      options = options.inject(){|(k, v), m| m[k] = v; m}
      @data_directory = data_directory
      @log_directory  = options[:log_directory]
      @unsafe_mode    = options[:unsafe_mode] # allows deleting of channels. REALLY UNSAFE!

      if @log_directory
        @logger = Logger.new File.join(@log_directory, 'conveyor.log')
      else
        @logger = Logger.new '/dev/null'
      end

      @channels = {}
      Dir.entries(@data_directory).each do |e|
        if !['.', '..'].include?(e) && File.directory?(File.join(@data_directory, e)) && Channel.valid_channel_name?(e)
          @channels[e] = Channel.new(File.join(@data_directory, e))
        end
      end

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

    def put env, m
      if Channel.valid_channel_name?(m.captures[0])
        if !@channels.key?(m.captures[0])
          create_new_channel m.captures[0]
          i "#{env["REMOTE_ADDR"]} PUT #{env["REQUEST_PATH"]} 201"
          [201, {}, "created channel #{m.captures[0]}"]
        else
          i "#{env["REMOTE_ADDR"]} PUT #{env["REQUEST_PATH"]}"
          [202, {}, "channel already exists. didn't do anything"]
        end
      else
        i "#{env["REMOTE_ADDR"]} GET #{env["REQUEST_PATH"]} 406"
        [406, {}, "invalid channel name. must match #{Channel::NAME_PATTERN}"]
      end
    end

    def post env, m
      if @channels.key?(m.captures[0])
        params = Mongrel::HttpRequest.query_parse(env['QUERY_STRING'])
        if params.key?('rewind_id')
          if params['group']
            @channels[m.captures[0]].rewind(:id => params['rewind_id'], :group => params['group']).to_i # TODO make sure this is an integer
            [200, {}, "iterator rewound to #{params['rewind_id']}"]
          else
            @channels[m.captures[0]].rewind(:id => params['rewind_id']).to_i # TODO make sure this is an integer
            [200, {}, "iterator rewound to #{params['rewind_id']}"]
          end
        elsif params.key?('rewind_time')
          @channels[m.captures[0]].rewind(:time => params['rewind_time'].to_i) # TODO make sure this is an integer
          [200, {}, "iterator rewound to #{params['rewind_time']}"]
        else
          if env.key?('HTTP_DATE') && d = Time.parse(env['HTTP_DATE'])
            id = @channels[m.captures[0]].post(env['rack.input'].read)
            i "#{env["REMOTE_ADDR"]} POST #{env["REQUEST_PATH"]} 202"
            [202, {"Location" => "/channels/#{m.captures[0]}/#{id}"}, ""]
          else
            i "#{env["REMOTE_ADDR"]} POST #{env["REQUEST_PATH"]} 400"
            [400, {}, "A valid Date header is required for all POSTs."]
          end
        end
      elsif Channel.valid_channel_name?(m.captures[0])
        create_new_channel(m.captures[0])
        post(env, m)
      else
        [404, {}, '']
      end
    end

    def get env
      headers = content = nil
      if m = path_match(env, %r{/channels/(.*)/(\d+)})
        if @channels.key?(m.captures[0])
          headers, content = @channels[m.captures[0]].get(m.captures[1].to_i)
        end
      elsif m = path_match(env, %r{/channels/(.*)})
        if @channels.key?(m.captures[0])
          params = Mongrel::HttpRequest.query_parse(env['QUERY_STRING'])
          if params.key? 'next'
            if params.key? 'group'
              if params.key? 'n'
                list = @channels[m.captures[0]].get_next_n_by_group(params['n'].to_i, params['group'])
              else
                headers, content = @channels[m.captures[0]].get_next_by_group(params['group'])
              end
            else
              if params.key? 'n'
                list = @channels[m.captures[0]].get_next_n(params['n'].to_i).map do |i|
                  {:data => i[1], :hash => i[0][:hash], :id => i[0][:id]}
                end
              else
                headers, content = @channels[m.captures[0]].get_next
              end
            end
          elsif params.key? 'after'
            headers, content = @channels[m.captures[0]].get_nearest_after_timestamp(params['after'].to_i)
          else
            return [200, {}, @channels[m.captures[0]].status.to_json]
          end
        end
      else
        return [200, {}, "fake!"]
      end

      if headers && content
        i "#{env["REMOTE_ADDR"]} GET #{env["REQUEST_PATH"]} 200 #{headers[:id]} #{headers[:length]} #{headers[:hash]}"
        return [
          200,
          {
            'Content-Location' => "/channels/#{m.captures[0]}/#{headers[:id]}",
            'Content-MD5'      => headers[:hash],
            'Content-Type'     => 'application/octet-stream',
            'Last-Modified'    => Time.at(headers[:time]).gmtime.to_s,
          },
            content
        ]
      elsif list
        return [200, {}, list.to_json]
      end

      return [404, {}, '']
    end

    def delete env, m
      if @channels.key?(m.captures[0])
        @channels[m.captures[0]].delete!
        @channels.delete(m.captures[0])
        [200, {}, "Channel deleted."]
      end
    end

    def call(env)
      @requests += 1
      if env['REQUEST_METHOD']    == 'PUT'  && m = path_match(env, %r{/channels/(.*)})
        put(env, m)
      elsif env['REQUEST_METHOD'] == 'POST' && m = path_match(env, %r{/channels/(.*)})
        post(env, m)
      elsif @unsafe_mode && env['REQUEST_METHOD'] == 'DELETE' && m = path_match(env, %r{/channels/(.*)})
        delete(env, m)
      elsif env['REQUEST_METHOD'] == 'GET'
        get(env)
      else
        [404, {}, '']
      end
    end
  end
end