require 'digest/md5'
require 'rubygems'
require 'active_support/core_ext/date/conversions'

module FeederNG
  class Channel

    NAME_PATTERN = %r{\A[a-zA-Z\-0-9]+\Z}
    BUCKET_SIZE = 100_000

    def initialize directory
      @directory = directory
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
        @index = []
        @index_file.each_line do |line|
          @index << parse_headers(line.strip, true)
          @last_id = @index.last[:id]
        end
        @index_file.seek(0, IO::SEEK_END)
      else
        @index_file = File.open(index_path, 'a')
        @index = []
        @last_id = 0
      end
      @index_file.sync = true

      @data_files = []

      iterator_path = File.join(@directory, 'iterator')

      if File.exists?(iterator_path) && File.size(iterator_path) > 0
        @iterator_file = File.open(iterator_path, 'r+')
        @iterator = 0
        @iterator_file.each_line do |line|
          @iterator = line.to_i
        end
        @iterator_file.seek(0, IO::SEEK_END)
      else
        @iterator_file = File.open(iterator_path, 'a')
        @iterator = 0
      end
      @iterator_file.sync = true

      @group_iterators = {}
      @group_iterators_files = {}
      Dir.glob(File.join(@directory, 'iterator-*')) do |i|
        g = i.split(%r{/}).last.match(%r{iterator-(.*)}).captures[0]
        @group_iterators_files[g] = File.open(i, 'r+')
        @group_iterators[g] = 0
        @group_iterators_files[g].each_line do |line|
          @group_iterators[g] = line.to_i
        end
        @group_iterators_files[g].seek(0, IO::SEEK_END)
      end
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

    def post data
      Thread.exclusive do
        i = @last_id + 1
        t = Time.now
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
    
    def get_next
      r = nil
      Thread.exclusive do
        @iterator += 1 # TODO make sure this is lower than @last_id
        r = get(@iterator)
        @iterator_file.write("#{@iterator}\n")
      end
      r
    end
    
    def group_iterators_file group
      unless @group_iterators_files[group]
        @group_iterators_files[group] = File.open(File.join(@directory, 'iterator-' + group), 'a+')
        @group_iterators_files[group].sync = true
      end
      yield @group_iterators_files[group]
    end
    
    def get_next_by_group group
      r = nil
      Thread.exclusive do
        @group_iterators[group] = 0 unless @group_iterators.key?(group)
        @group_iterators[group] += 1
        r = get(@group_iterators[group])
        group_iterators_file(group) do |f|
          f.write("#{@group_iterators[group]}\n")
        end
      end
      r
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