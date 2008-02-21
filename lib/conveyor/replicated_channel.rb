require 'conveyor/base_channel'

module Conveyor
  class ReplicatedChannel < BaseChannel # :nodoc: not ready for primetime
    attr_accessor :peers
    attr_reader :commit_thread
    attr_reader :ping_thread
    
    def initialize directory
      @peers                   = []
      @clock                   = 0
      @commit_queue            = PriorityQueue.new
      @last_message_id_by_peer = {}
      @clock_lock              = Mutex.new
      @commit_queue_lock       = Mutex.new

      super(directory)

      start_threads
    end

    def start_threads
      @commit_thread = Thread.new do
        commit_loop
      end
      @commit_thread.abort_on_exception = true

      @peer_id = Process.pid # TODO makes this stable across restarts (UUID stored in data folder?)
      
      @ping_thread = Thread.new do
        ping_loop
      end
      @ping_thread.abort_on_exception
    end

    def commit_loop
      loop do
        begin
          if !@commit_queue.empty?
            @commit_queue_lock.synchronize do
              if !@commit_queue.empty? && @commit_queue.front[0] < @last_message_id_by_peer[@commit_queue.front[2]]
                clock, client_time, peer_id, server_time, data = @commit_queue.pop
                commit data, server_time
              end
            end
          else
            sleep 0.2
          end
        rescue => e
          puts "commit loop died with"
          puts e.class
          puts e.message
          puts e.backtrace.join("\n")
          p @commit_queue
          p @directory
        end
      end
    end

    def ping_loop
      loop do
        begin
          peers.each do |p|
            @clock_lock.synchronize do
              p.ping @clock, @peer_id
              @clock += 1
            end
          end
        rescue => e
          puts "ping loop died with"
          puts e.class
          puts e.message
          puts e.backtrace.join("\n")
          p @commit_queue
          p @directory
        end
        sleep 1
      end
    end

    def ping clock, peer_id
      @last_message_id_by_peer[peer_id] = clock
    end

    def replicate data, clock, client_time, peer_id, server_time
      @commit_queue << [clock, client_time, peer_id, server_time, data]
      @clock_lock.synchronize { @clock = [@clock + 1, clock].max }
      @last_message_id_by_peer[peer_id] = clock
    end

    def post data, client_time
      server_time = Time.now
      @commit_queue_lock.synchronize do
        @clock_lock.synchronize do
          peers.each do |p|
            p.replicate data, @clock, client_time, @peer_id, server_time
          end
          @commit_queue << [@clock, client_time, @peer_id, server_time, data]
          @last_message_id_by_peer[@peer_id] = @clock
          @clock += 1
        end
      end
    end
  end
end