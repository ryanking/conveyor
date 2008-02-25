require 'conveyor/base_channel'

def String.random_alphanumeric(size=16)
  s = ""
  size.times { s << (i = Kernel.rand(62); i += ((i < 10) ? 48 : ((i < 36) ? 55 : 61 ))).chr }
  s
end


module Conveyor
class Upgrader

  def initialize directory
    @directory = directory
  end

  def version_path
    File.join(@directory, 'version')
  end

  def source_version
    if File.exists?(version_path) && File.size(version_path) > 0
      File.open(version_path).read.to_i
    else
      0
    end
  end

  def upgrade
    case source_version
    when 0
      from_0
    end
  end

  def parse_headers_0 str, index_file = false
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

  def from_0
    Dir.glob(@directory + "/*").each do |d|
      p d
      if File.directory?(d)
        # create tmp dir
        tmp_dir = create_tmp_dir!
        puts "writing to #{tmp_dir}"
        chan = Channel.new(tmp_dir)

        Dir.glob("#{d}/[0-9]*").each do |f|
          puts "upgrading #{f}"
          size = File.size(f)
          f    = File.open(f)

          while f.pos < size
            l       = f.readline.strip
            header  = parse_headers_0(l)
            content = f.read(header[:length])
            f.readline # newline chomp

            chan.commit(content, Time.parse(header[:time]))
          end
        end
        
        

        puts "backing up #{d} to #{d}.bak"
        FileUtils.mv d, d + ".bak"

        puts "copying from #{tmp_dir} to #{d}"
        FileUtils.cp_r tmp_dir, d

        puts "deleting temp data"
        FileUtils.rm_r tmp_dir
      end
    end
  end

  def create_tmp_dir!
    loop do
      tmp_dir = File.join('/tmp', String.random_alphanumeric)
      if !File.exists?(tmp_dir)
        return tmp_dir
      end
    end
  end
end
end