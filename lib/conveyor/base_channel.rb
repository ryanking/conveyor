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
    INDEX_MODULO = 10

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
      @index_file_lock = Mutex.new

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
        index_offset = nil
        index_file_lock do
          @index_file.seek(0, IO::SEEK_END)
          index_offset = @index_file.pos
          @index_file.write "#{header} #{b.to_s(36)}\n"
        end
        if i % INDEX_MODULO == 1
          @index << {:id => i, :time => t, :offset => o, :length => l, :hash => h, :file => b, :index_offset => index_offset}
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
        headers  = parse_headers(f.readline.strip)
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
      pattern =  '\A([a-z\d]+) ([a-z\d]+) ([a-z\d]+) ([a-z\d]+) ([a-f0-9]+) ([a-z\d]+)'
      pattern += ' ([a-z\d]+)' if index_file
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

    def rebuild_index
      Dir.glob(@directory + '/' + '[0-9]*').each do |f|
        File.open(f, 'r') do |file|
          b = f.split("/").last.to_i
          while line = file.gets
            headers = parse_headers(line.strip)
            content = file.read(headers[:length])
            file.read(1)
            index_offset = nil
            header = "#{headers[:id].to_s(36)} #{headers[:time].to_i.to_s(36)} #{headers[:offset].to_s(36)} #{headers[:length].to_s(36)} #{headers[:hash]} #{headers[:flags].to_s(36)}"
            index_file_lock do
              @index_file.seek(0, IO::SEEK_END)
              index_offset = @index_file.pos
              @index_file.write "#{header} #{b.to_s(36)}\n"
            end
            if headers[:id] % INDEX_MODULO == 1
              @index << {:id => headers[:id], :time => headers[:time], :offset => headers[:offset], :length => headers[:length], 
                          :hash => headers[:hash], :file => b, :index_offset => index_offset}
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

      @last_id = 0
      while line = @index_file.gets
        index_offset = @index_file.pos
        entry = parse_headers(line.strip, true)
        if entry[:id] % INDEX_MODULO == 1
          entry[:index_offset] = index_offset
          @index << entry
        end
        @last_id = entry[:id]
      end
      @index_file.seek(0, IO::SEEK_END)
    end

    def index_path
      File.join(@directory, 'index')
    end

    def version_path
      File.join(@directory, 'version')
    end

    def search_index id
      block_id = ((id-1) / INDEX_MODULO).to_i * INDEX_MODULO + 1
      entry = (@index.find{|entry| entry[:id] == block_id})

      index_file_lock do
        @index_file.seek(entry[:index_offset])
        until entry[:id] == id
          entry = parse_headers(@index_file.gets.strip, true)
        end
        @index_file.seek(0, IO::SEEK_END)
      end
      entry
    end

    def nearest_after(timestamp)
      i = 0
      while (i < @index.length - 1) && timestamp < @index[i+1][:time].to_i
        i += 1
      end
      entry = @index[i]
      index_file_lock do
        @index_file.seek(entry[:index_offset])
        begin
          while entry[:time].to_i < timestamp && line = @index_file.readline
            if entry[:time].to_i < timestamp
              entry = parse_headers(line.strip, true)
            end
          end
          entry[:id]
        rescue EOFError => e
          nil
        ensure
          @index_file.seek(0, IO::SEEK_END)
        end
      end
    end
  end
end