require 'digest/md5'
require 'rubygems'
require 'active_support/core_ext/date/conversions'

module FeederNG
  class Channel

    NAME_PATTERN = %r{\A[a-zA-Z\-0-9]+\Z}

    def initialize directory
      if File.exists?(directory)
        if !File.directory?(directory)
          raise "#{directory} is not a directory"
        end
      else
        Dir.mkdir(directory)
      end

      index_path = File.join(directory, 'index')

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

      @data_file  = File.open(File.join(directory, '1'), 'a+')
      @data_file.sync = true

      iterator_path = File.join(directory, 'iterator')

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
    end

    def post data
      Thread.exclusive do
        i = @last_id + 1
        t = Time.now
        l = data.length
        h = Digest::MD5.hexdigest(data)
        @data_file.seek(0, IO::SEEK_END)
        o = @data_file.pos

        header = "#{i} #{t.xmlschema} #{o} #{l} #{h}"
        @data_file.write("#{header}\n" + data + "\n")
        @last_id = i

        @index_file.write "#{header} 1\n"
        @index << {:id => i, :time => t, :offset => o, :length => l, :hash => h, :file => 1}
        i
      end
    end

    def get id
      return nil unless id <= @last_id
      i = @index.find{|e| e[:id] == id}
      header, content = nil
      Thread.exclusive do
        @data_file.seek i[:offset]
        header  = @data_file.readline.strip
        content = @data_file.read(i[:length])
      end
      [parse_headers(header), content]
    end
    
    def get_next
      Thread.exclusive do
        @iterator += 1 # TODO make sure this is lower than @last_id
        r = get(@iterator)
        @iterator_file.write("#{@iterator}\n")
        r
      end
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