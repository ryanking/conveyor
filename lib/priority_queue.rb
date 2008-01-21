# a naive (performance-wise) priority queue implementation
#
# modified from http://www.rubyquiz.com/quiz98.html
class PriorityQueue
  def initialize sort_proc = nil
    @list = []
    @sort_proc = sort_proc || proc{|x,y| x <=> y}
  end

  def add(item)
    @list << item
    @list.sort!(&@sort_proc)
    self
  end

  alias << add

  def front
    @list.first
  end
  
  def pop
    @list.shift
  end

  def empty?
    @list.empty?
  end

  def length
    @list.length
  end
end
