$: << 'lib'
require 'feeder-ng/index'

def rand_str(len, domain)
  (1..len).inject("") { |s, x| s << domain[rand(domain.length)] }
end

alphanum = [ ('a'..'z').to_a, ('A'..'Z').to_a, ('0'..'9').to_a ].flatten
data = []

index = FeederNG::Index.new '/tmp/bar'

puts "writing random data"
1000.times{index.post(d=rand_str(rand(10000), alphanum)); data << d}

puts "reading data back"

1000.times do |i|
  headers, content = index.get(i + 1)
  
  unless content == data[i]
    puts "unmatched content"
    puts "ORIGINAL"
    puts "----"
    puts data[i]
    puts "----"
    
    puts "RETURNED"
    puts "----"
    puts content
    puts "----"
  end
  
end