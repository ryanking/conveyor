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

    NAME_PATTERN   = %r{\A[a-zA-Z\-0-9\_]+\Z}
    BUCKET_SIZE    = 100_000
    FORMAT_VERSION =       1
    BLOCK_SIZE     =    1000
    CACHE_SIZE     =     100

    module Flags
      GZIP = 1
    end

    def initialize directory
      @directory             = directory
      @data_files            = []
      @file_mutexes          = []
      @iterator              = 1 #TODO: move to Channel.rb
      @id_lock               = Mutex.new
      @index_file_lock       = Mutex.new
      @block_cache           = {}
      @block_last_used       = {}

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

    def block_num i
      ((i-1) / BLOCK_SIZE)
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

    def index_file_lock
      @index_file_lock.synchronize do
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
        t = time || Time.now.to_i
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
        index_offset = nil
        index_file_lock do
          @index_file.seek(0, IO::SEEK_END)
          index_offset = @index_file.pos
          @index_file.write "#{header} #{b.to_s(36)}\n"
        end
        block_num = block_num(i)
        if !@blocks[block_num]
          @blocks << {:offset => index_offset}
        end
        if @block_cache.key?(block_num)
          @block_cache[block_num] << {:id => i, :time => t, :offset => o, :length => l, :hash => h, :file => b}
        end
        i
      end
    end

    def get id, stream=false
      return nil unless id <= @last_id && id > 0

      index_entry = search_index(id)

      headers, content, compressed_content, g = nil
      bucket_file(index_entry[:file]) do |f|
        f.seek index_entry[:offset]
        headers  = self.class.parse_headers(f.readline.strip)
        compressed_content = f.read(index_entry[:length])
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
      id, time, offset, length, hash, flags, file = str.split ' '
      {
        :id     => id.to_i(36),
        :time   => time.to_i(36),
        :offset => offset.to_i(36),
        :length => length.to_i(36),
        :hash   => hash,
        :flags  => flags.to_i(36),
        :file   => (index_file ? file.to_i(36) : nil)
      }
    end

    def self.valid_channel_name? name
      !!name.match(NAME_PATTERN)
    end

    def delete!
      FileUtils.rm_r(@directory)
      @data_files =[]
      @last_id = 0
      @blocks = []
    end

    def rebuild_index
      Dir.glob(@directory + '/' + '[0-9]*').each do |f|
        File.open(f, 'r') do |file|
          b = f.split("/").last.to_i
          while line = file.gets
            headers = self.class.parse_headers(line.strip)
            content = file.read(headers[:length])
            file.read(1)
            index_offset = nil
            header = "#{headers[:id].to_s(36)} #{headers[:time].to_s(36)} #{headers[:offset].to_s(36)} #{headers[:length].to_s(36)} #{headers[:hash]} #{headers[:flags].to_s(36)}"
            index_file_lock do
              @index_file.seek(0, IO::SEEK_END)
              index_offset = @index_file.pos
              @index_file.write "#{header} #{b.to_s(36)}\n"
            end
          end
        end
      end
    end

    protected

    def setup_channel
      @index_file = File.open(index_path, 'a+')
      @last_id = 0
      @version = FORMAT_VERSION
      @blocks = []
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
      @blocks = []
      @last_id = 0
      index_offset = 0
      while line = @index_file.gets
        entry = self.class.parse_headers(line.strip, true)
        if entry[:id] % BLOCK_SIZE == 1
          @blocks << {:offset => index_offset}
        end
        @last_id = entry[:id]
        index_offset = @index_file.pos
      end
      @index_file.seek(0, IO::SEEK_END)
    end

    def index_path
      File.join(@directory, 'index')
    end

    def version_path
      File.join(@directory, 'version')
    end

    def cache_block block_num
      if @block_cache.length > CACHE_SIZE
        reject = @block_last_used.sort{|a,b| a[1] <=> b[1]}.last.first
        @block_cache.delete(reject)
        puts "rejected #{reject}"
      end
      a = []

      buf = ''
      block_start = @blocks[block_num][:offset]
      block_end   = @blocks[block_num + 1] ? @blocks[block_num + 1][:offset] : nil
      index_file_lock do
        @index_file.seek(block_start)
        if block_end
          buf = @index_file.read(block_end - block_start)
        else
          buf = @index_file.read
        end
        @index_file.seek(0, IO::SEEK_END)
      end
      buf.split(/\n/).each do |line|
        a << self.class.parse_headers(line.strip, true)
      end
      @block_cache[block_num] = a
    end

    def search_index id
      block_num = block_num(id)

      if !@block_cache.has_key?(block_num)
        cache_block(block_num)
      end
      @block_last_used[block_num] = Time.now.to_i
      entry = @block_cache[block_num][id - 1 - (block_num * BLOCK_SIZE)]
    end

    def nearest_after(timestamp)
      i = 0
      while (i < @blocks.length - 1) && timestamp < @blocks[i+1][:time]
        i += 1
      end
      cache_block(i) if !@block_cache.has_key?(i)
      @block_last_used[i] = Time.now.to_i
      @block_cache[i].each do |entry|
        if entry[:time] > timestamp
          return entry[:id]
        end
      end
      if @blocks[i+1]
        cache_block(i+1)
        @block_cache[i+1].first[:id]
      else
        nil
      end
    end
  end
end