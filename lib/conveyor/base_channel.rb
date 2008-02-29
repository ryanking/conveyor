require 'digest/md5'
require 'rubygems'
require 'time'
require 'stringio'
require 'zlib'
require 'json'

module Conveyor
  # BaseChannel
  #
  # Base implementation for channels. Not useful to instantiate directly.
  class BaseChannel

    NAME_PATTERN = %r{\A[a-zA-Z\-0-9\_]+\Z}
    BUCKET_SIZE = 100_000
    FORMAT_VERSION = 1

    module Flags
      GZIP = 1
    end

    def initialize directory
      @directory    = directory
      @data_files   = []
      @file_mutexes = []
      @index        = []
      @iterator     = 1
      @id_lock      = Mutex.new

      if File.exists?(@directory)
        if !File.directory?(@directory)
          raise "#{@directory} is not a directory"
        end
        load_channel
      else
        Dir.mkdir(@directory)
        setup_channel
      end

      @index_file.sync    = true
    end

    def inspect
      "<#{self.class} dir:'#{@directory.to_s}' last_id:#{@last_id} iterator:#{@iterator}>"
    end

    def pick_bucket i
      (i / BUCKET_SIZE).to_i
    end

    def bucket_file i
      unless @data_files[i]
        @data_files[i] = File.open(File.join(@directory, i.to_s), 'a+')
        @data_files[i].sync = true
        @file_mutexes[i] = Mutex.new
      end
      @file_mutexes[i].synchronize do
        yield @data_files[i]
      end
    end

    def id_lock
      @id_lock.synchronize do
        yield
      end
    end

    def commit data, time=nil
      l = nil
      gzip = data.length >= 256
      if gzip
        compressed_data = StringIO.new
        g = Zlib::GzipWriter.new(compressed_data)
        g << data
        g.finish
        compressed_data.rewind
        compressed_data = compressed_data.read
        l = compressed_data.length
      else
        l = data.length
      end

      h = Digest::MD5.hexdigest(data)

      id_lock do
        i = @last_id + 1
        t = time || Time.now
        b = pick_bucket(i)
        flags = 0
        flags = flags | Flags::GZIP if gzip
        header, o = nil
        bucket_file(b) do |f|
          f.seek(0, IO::SEEK_END)
          o = f.pos
          header = "#{i.to_s(36)} #{t.to_i.to_s(36)} #{o.to_s(36)} #{l.to_s(36)} #{h} #{flags.to_s(36)}"
          f.write("#{header}\n")
          f.write((gzip ? compressed_data : data))
          f.write("\n")
        end

        @last_id = i
        @index_file.write "#{header} #{b.to_s(36)}\n"
        @index << {:id => i, :time => t, :offset => o, :length => l, :hash => h, :file => b}
        i
      end
    end

    def get id, stream=false
      return nil unless id <= @last_id && id > 0
      i = @index[id-1]
      headers, content, compressed_content, g = nil
      bucket_file(i[:file]) do |f|
        f.seek i[:offset]
        headers  = parse_headers(f.readline.strip)
        compressed_content = f.read(i[:length])
      end
      io = StringIO.new(compressed_content)
      if (headers[:flags] & Flags::GZIP) != 0
        g  = Zlib::GzipReader.new(io)
      else
        g = io
      end
      if stream
        [headers, g]
      else
        [headers, g.read]
      end
    end

    def get_nearest_after_timestamp timestamp, stream=false
      # i = binary search to find nearest item at or after timestamp
      i = nearest_after(timestamp)
      get(i) if i
    end

    def self.parse_headers str, index_file=false
      pattern =  '\A([a-z\d]+) ([a-z\d]+) ([a-z\d]+) ([a-z\d]+) ([a-f0-9]+) ([a-z\d]+)'
      pattern += ' (\d+)' if index_file
      pattern += '\Z'
      m = str.match(Regexp.new(pattern))
      {
        :id     => m.captures[0].to_i(36),
        :time   => m.captures[1].to_i(36),
        :offset => m.captures[2].to_i(36),
        :length => m.captures[3].to_i(36),
        :hash   => m.captures[4],
        :flags  => m.captures[5].to_i(36),
        :file   => (index_file ? m.captures[6].to_i(36) : nil)
      }
    end

    def parse_headers str, index_file=false
      self.class.parse_headers str, index_file
    end
    
    def self.valid_channel_name? name
      !!name.match(NAME_PATTERN)
    end

    def delete!
      FileUtils.rm_r(@directory)
      @index = []
      @data_files =[]
      @last_id = 0
    end

    protected

    def setup_channel
      @index_file = File.open(index_path, 'a')
      @last_id = 0
      @version = FORMAT_VERSION
      File.open(version_path, 'w+'){|f| f.write(@version.to_s)}
    end

    def load_channel
      if File.exists?(version_path) && File.size(version_path) > 0
        @version = File.open(version_path).read.to_i
      else
        @version = 0
      end

      if @version != FORMAT_VERSION
        raise "Format versions don't match. Try upgrading."
      end

      @index_file = File.open(index_path, 'r+')

      @index_file.each_line do |line|
        @index << parse_headers(line.strip, true)
        @last_id = @index.last[:id]
      end
      @index_file.seek(0, IO::SEEK_END)
    end


    def index_path
      File.join(@directory, 'index')
    end

    def version_path
      File.join(@directory, 'version')
    end

    def nearest_after(timestamp)
      low = 0
      high = @index.length
      while low < high
        mid = (low + high) / 2
        if (@index[mid][:time].to_i > timestamp)
          high = mid - 1
        elsif (@index[mid][:time].to_i < timestamp)
          low = mid + 1
        else
          return mid
        end
      end
      if timestamp <= @index[mid][:time].to_i
        @index[mid][:id]
      else
        nil
      end
    end
  end
end