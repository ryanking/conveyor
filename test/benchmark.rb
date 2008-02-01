require 'benchmark'
require 'net/http'

m = Benchmark.measure do
  h = Net::HTTP.start('localhost', 8011)
  h.put('/channels/foo','')
  Dir.glob('test/data/*').each do |d|
    h.post('/channels/foo', File.open(d.to_s).read)
  end
end

p m