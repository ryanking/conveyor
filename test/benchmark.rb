$: << 'lib'
require 'benchmark'
require 'net/http'
require 'conveyor/client'

def std_dev(ary, mean)
  Math.sqrt( (ary.inject(0) { |dev, i| 
                dev += (i - mean) ** 2}/ary.length.to_f) )
end

limit = (ARGV.first && ARGV.first.to_i)

times = []
i = 0
t0 = Time.now
c = Conveyor::Client.new 'localhost'
m = Benchmark.measure do
  c.create_channel('foo')
  Dir.glob('test/data/*').each do |d|
    t1 = Time.now
    c.post('foo', File.open(d.to_s).read)
    times << (Time.now - t1)
    i += 1
    if limit && i >= limit
      break
    end
    print '.'
    $stdout.flush
  end
end
puts
puts
times.length.times do |i|
  c.get('foo', i+1)
  print '.'
end


total_time = Time.now - t0

items = Dir.glob('test/data/*').length

puts
puts 'benchmark'
p m

puts
puts 'total time'
p total_time

puts
puts 'average time'
p total_time / items

puts
puts 'standard deviation'
p std_dev(times, (total_time/items))

