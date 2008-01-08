require 'digest/md5'
require 'rubygems'
require 'active_support/core_ext/date/conversions'

module FeederNG
  class Index
    def initialize directory
      # TODO check to be sure directory exists

      @index_file = File.open(File.join(directory, 'index'), 'w+') # TODO change mode to 'a+'
      @index_file.sync = true
      @data_file  = File.open(File.join(directory, '1'), 'w+') # TODO change mode to 'a+'
      @data_file.sync = true
      @current_offset = 0 # HACK
      @last_id = 0 # HACK

      @index = []
      # TODO multiple data files + lazy loading
    end

    def post data
      i = @last_id + 1
      t = Time.now
      l = data.length
      h = Digest::MD5.hexdigest(data)
      o = @current_offset

      @data_file.write("#{i} #{t.xmlschema} #{o} #{l} #{h}\n")
      @data_file.write(data + "\n")
      @last_id += 1

      @index_file.write "#{i} #{t.xmlschema} #{o} #{l} #{h} 1\n"
      @index << {:id => i, :time => t, :offset => o, :length => l, :hash => h, :file => 1}
      i
    end
    
    def get id
      i = @index.find{|e| e[:id] == id}
      @data_file.seek i[:offset]
      header  = @data_file.readline
      content = @data_file.read(i[:length])
      {:headers => parse_headers(header), :data => content}
    end
    
    def parse_headers str
      m = str.match /\A(\d+) (\d{4}\-\d{2}\-\d{2}T\d{2}\:\d{2}\:\d{2}[+\-]\d{2}\:\d{2}) (\d+) (\d+) ([a-f0-9]+)\Z/
      {:id => m.captures[0].to_i, :time => m.captures[1], :offset => m.captures[2].to_i, :length => m.captures[3].to_i, :hash => m.captures[4]}
    end
  end
end