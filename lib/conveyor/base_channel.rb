require 'digest/md5'
require 'rubygems'
require 'active_support/core_ext/date/conversions'
require 'priority_queue'

module Conveyor
  class BaseChannel

    NAME_PATTERN = %r{\A[a-zA-Z\-0-9]+\Z}
    BUCKET_SIZE = 100_000

    def initialize directory
      @directory               = directory
      @data_files              = []
      @index = []
      @iterator = 0

      if File.exists?(@directory)
        if !File.directory?(@directory)
          raise "#{@directory} is not a directory"
        end
      else
        Dir.mkdir(@directory)
      end

      index_path = File.join(@directory, 'index')

      if File.exists?(index_path) && File.size(index_path) > 0
        @index_file = File.open(index_path, 'r+')

        @index_file.each_line do |line|
          @index << parse_headers(line.strip, true)
          @last_id = @index.last[:id]
        end
        @index_file.seek(0, IO::SEEK_END)
      else
        @index_file = File.open(index_path, 'a')
        @last_id = 0
      end
      @index_file.sync = true

    end
    
    def inspect
      "<#{self.class} dir:'#{@directory.to_s}' last_id:#{@last_id}>"
    end
    
    def pick_bucket i
      (i / BUCKET_SIZE).to_i
    end

    def bucket_file i
      unless @data_files[i]
        @data_files[i] = File.open(File.join(@directory, i.to_s), 'a+')
        @data_files[i].sync = true
      end
      yield @data_files[i]
    end

    def commit data, time = nil
      Thread.exclusive do
        i = @last_id + 1
        t = time || Time.now
        l = data.length
        h = Digest::MD5.hexdigest(data)
        b = pick_bucket(i)
        header, o = nil
        bucket_file(b) do |f|
          f.seek(0, IO::SEEK_END)
          o = f.pos
          header = "#{i} #{t.xmlschema} #{o} #{l} #{h}"
          f.write("#{header}\n" + data + "\n")
        end

        @last_id = i
        @index_file.write "#{header} #{b}\n"
        @index << {:id => i, :time => t, :offset => o, :length => l, :hash => h, :file => b}
        i
      end
    end

    def get id
      return nil unless id <= @last_id
      i = @index.find{|e| e[:id] == id}
      header, content = nil
      Thread.exclusive do
        bucket_file(i[:file]) do |f|
          f.seek i[:offset]
          header  = f.readline.strip
          content = f.read(i[:length])
        end
      end
      [parse_headers(header), content]
    end

    def parse_headers str, index_file=false
      pattern =  '\A(\d+) (\d{4}\-\d{2}\-\d{2}T\d{2}\:\d{2}\:\d{2}[+\-]\d{2}\:\d{2}) (\d+) (\d+) ([a-f0-9]+)'
      pattern += ' (\d+)' if index_file
      pattern += '\Z'
      m = str.match(Regexp.new(pattern))
      {
        :id => m.captures[0].to_i,
        :time => m.captures[1],
        :offset => m.captures[2].to_i,
        :length => m.captures[3].to_i,
        :hash => m.captures[4], 
        :file => (index_file ? m.captures[5].to_i : nil)
      }.reject {|k,v| v == nil}
    end

    def self.valid_channel_name? name
      !!name.match(NAME_PATTERN)
    end
  end
end