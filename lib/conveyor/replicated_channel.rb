require 'conveyor/base_channel'

module Conveyor
  class ReplicatedChannel < BaseChannel
    attr_accessor :peers
    attr_reader :commit_thread
    attr_reader :ping_thread
    
    def initialize directory
      @peers                   = []
      @clock                   = 0
      @commit_queue            = PriorityQueue.new
      @last_message_id_by_peer = {}

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
            Thread.exclusive do
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
        end
      end
    end

    def ping_loop
      loop do
        peers.each do |p|
          Thread.exclusive do
            p.ping @clock, @peer_id
            @clock += 1
          end
        end
        sleep 1
      end
    end

    def ping clock, peer_id
      @last_message_id_by_peer[peer_id] = clock
    end

    def replicate data, clock, client_time, peer_id, server_time
      Thread.exclusive do
        @commit_queue << [clock, client_time, peer_id, server_time, data]
        @clock = [@clock + 1, clock].max
        @last_message_id_by_peer[peer_id] = clock
      end
    end

    def post data, client_time
      server_time = Time.now
      Thread.exclusive do
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