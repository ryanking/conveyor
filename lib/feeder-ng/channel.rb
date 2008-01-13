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
    end

    def post data
      i = @last_id + 1
      t = Time.now
      l = data.length
      h = Digest::MD5.hexdigest(data)
      @data_file.seek(0, IO::SEEK_END)
      o = @data_file.pos

      @data_file.write("#{i} #{t.xmlschema} #{o} #{l} #{h}\n" + data + "\n")
      @last_id = @last_id + 1

      @index_file.write "#{i} #{t.xmlschema} #{o} #{l} #{h} 1\n"
      @index << {:id => i, :time => t, :offset => o, :length => l, :hash => h, :file => 1}
      i
    end

    def get id
      return nil unless id <= @last_id
      i = @index.find{|e| e[:id] == id}
      @data_file.seek i[:offset]
      header  = @data_file.readline.strip
      content = @data_file.read(i[:length])
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