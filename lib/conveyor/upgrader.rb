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
            header  = BaseChannel.parse_headers(l)
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