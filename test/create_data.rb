require 'fileutils'

def String.random_alphanumeric(size=16)
  s = ""
  size.times { s << (i = Kernel.rand(62); i += ((i < 10) ? 48 : ((i < 36) ? 55 : 61 ))).chr }
  s
end

FileUtils.rm_r 'test/data'
FileUtils.mkdir 'test/data'
n = ARGV.first.to_i

n.times do |i|
  File.open(File.join('test/data', i.to_s), 'w') do |f|
    f.write String.random_alphanumeric(10_000 + rand(10_000))
  end
end