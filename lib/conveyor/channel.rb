require 'conveyor/base_channel'

module Conveyor
  # Channel
  #
  # A basic channel.
  class Channel < BaseChannel

    # If +directory+ doesn't already exist, it will be created during initialization.
    def initialize directory
      @group_iterators       = {}
      @group_iterators_files = {}
      @iterator_lock         = Mutex.new
      @group_iterator_locks  = Hash.new{|k,v| Mutex.new }

      super(directory)

      @iterator_file.sync    = true
    end

    # Add data to the channel.
    def post data
      commit data
    end
    
    # Returns the next item from the global (non-group) iterator.
    def get_next
      r = nil
      iterator_lock do
        if @iterator <= @last_id
          r = get(@iterator)
          @iterator += 1
          @iterator_file.write("#{@iterator.to_s(36)}\n")
          r
        else
          nil
        end
      end
    end

    # Returns the next item for +group+. If +group+ hasn't been seen before, the first item is returned.
    def get_next_by_group group
      r = nil
      group_iterator_lock(group) do
        @group_iterators[group] = 1 unless @group_iterators.key?(group)
        if @group_iterators[group] <= @last_id
          r = get(@group_iterators[group])
          @group_iterators[group] += 1
          group_iterators_file(group) do |f|
            f.write("#{@group_iterators[group].to_s(36)}\n")
          end
        else
          nil
        end
      end
      r
    end

    def get_next_n n
      r = []
      iterator_lock do
        while r.length < n && @iterator <= @last_id
          r << get(@iterator)
          @iterator += 1
          @iterator_file.write("#{@iterator.to_s(36)}\n")
          r
        end
      end
      r
    end
    
    def get_next_n_by_group n, group
      r = []
      group_iterator_lock(group) do
        @group_iterators[group] = 1 unless @group_iterators.key?(group)
        while r.length < n && @group_iterators[group] < @last_id
          r << get(@group_iterators[group])
          @group_iterators[group] += 1
          group_iterators_file(group) do |f|
            f.write("#{@group_iterators[group].to_s(36)}\n")
          end
        end
      end
      r
    end

    def status
      {
        :directory => @directory,
        :blocks => @blocks.length,
        :data_files => @data_files.collect{|f| {:path => f.path, :bytes => File.size(f.path)}},
        :iterator => {:position => @iterator},
        :iterator_groups => @group_iterators.inject({}){|m,(k,v)| m[k] = v; m},
        :last_id => @last_id
      }
    end

    def rewind *opts
      opts = opts.inject{|h, m| m.merge(h)}
      if opts.key?(:id)
        if opts.key?(:group)
          group_iterator_lock(opts[:group]) do
            @group_iterators[opts[:group]] = opts[:id].to_i
            group_iterators_file(opts[:group]) do |f|
              f.write("#{@group_iterators[opts[:group]].to_s(36)}\n")
            end
          end
        else
          iterator_lock do
            @iterator = opts[:id].to_i
            @iterator_file.write("#{@iterator.to_s(36)}\n")
          end
        end
      elsif opts.key?(:time)
        if opts.key?(:group)
          rewind :id => nearest_after(opts[:time]), :group => opts[:group]
        else
          rewind :id => nearest_after(opts[:time])
        end
      end
    end


    private

    def iterator_lock
      @iterator_lock.synchronize do
        yield
      end
    end

    def group_iterator_lock group
      @group_iterator_locks[group].synchronize do
        yield
      end
    end

    def group_iterators_file group
      unless @group_iterators_files[group]
        @group_iterators_files[group] = File.open(File.join(@directory, 'iterator-' + group), 'a+')
        @group_iterators_files[group].sync = true
      end
      yield @group_iterators_files[group]
    end

    def load_channel
      super
      @iterator_file = File.open(iterator_path, 'r+')
      @iterator_file.each_line do |line|
        @iterator = line.to_i(36)
      end
      @iterator_file.seek(0, IO::SEEK_END)

      Dir.glob(File.join(@directory, 'iterator-*')) do |i|
        g = i.split(%r{/}).last.match(%r{iterator-(.*)}).captures[0]
        @group_iterators_files[g] = File.open(i, 'r+')
        @group_iterators[g] = 1
        @group_iterators_files[g].each_line do |line|
          @group_iterators[g] = line.to_i(36)
        end
        @group_iterators_files[g].seek(0, IO::SEEK_END)
      end
    end

    def setup_channel
      super
      @iterator_file = File.open(iterator_path, 'a')
    end

    def iterator_path
      File.join(@directory, 'iterator')
    end

    def version_path
      File.join(@directory, 'version')
    end
  end
end