require "test/unit"

require "priority_queue"

class TestPriorityQueue < Test::Unit::TestCase
  def test_simple
    
    pq = PriorityQueue.new

    pq << 1 << 2 << -1

    assert_equal(-1, pq.pop)
    assert_equal 1, pq.pop
    assert_equal 2, pq.pop
  end
end