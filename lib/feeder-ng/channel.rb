require 'feeder-ng/base_channel'

module FeederNG
  class Channel < BaseChannel

    def initialize directory
      @group_iterators         = {}
      @group_iterators_files   = {}
      
      super(directory)
      
      iterator_path = File.join(@directory, 'iterator')

      if File.exists?(iterator_path) && File.size(iterator_path) > 0
        @iterator_file = File.open(iterator_path, 'r+')
        @iterator_file.each_line do |line|
          @iterator = line.to_i
        end
        @iterator_file.seek(0, IO::SEEK_END)
      else
        @iterator_file = File.open(iterator_path, 'a')
      end
      @iterator_file.sync = true

      Dir.glob(File.join(@directory, 'iterator-*')) do |i|
        g = i.split(%r{/}).last.match(%r{iterator-(.*)}).captures[0]
        @group_iterators_files[g] = File.open(i, 'r+')
        @group_iterators[g] = 0
        @group_iterators_files[g].each_line do |line|
          @group_iterators[g] = line.to_i
        end
        @group_iterators_files[g].seek(0, IO::SEEK_END)
      end
    end

    def post data
      commit data
    end
    
    def get_next
      r = nil
      Thread.exclusive do
        @iterator += 1 # TODO make sure this is lower than @last_id
        r = get(@iterator)
        @iterator_file.write("#{@iterator}\n")
      end
      r
    end

    def group_iterators_file group
      unless @group_iterators_files[group]
        @group_iterators_files[group] = File.open(File.join(@directory, 'iterator-' + group), 'a+')
        @group_iterators_files[group].sync = true
      end
      yield @group_iterators_files[group]
    end

    def get_next_by_group group
      r = nil
      Thread.exclusive do
        @group_iterators[group] = 0 unless @group_iterators.key?(group)
        @group_iterators[group] += 1
        r = get(@group_iterators[group])
        group_iterators_file(group) do |f|
          f.write("#{@group_iterators[group]}\n")
        end
      end
      r
    end
  end
end