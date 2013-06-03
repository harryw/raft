require 'lib/raft'

require 'goliath'

module Raft
  class Goliath
    class HttpJsonRpcResponder < Goliath::API
      use Goliath::Rack::Render, 'json'
      use Goliath::Rack::Validation::RequestMethod, %w(POST)
      use Goliath::Rack::Params

      def initialize(node)
        @node = node
      end

      def response(env)
        case env['REQUEST_PATH']
        when '/request_vote'
          handle_errors {request_vote_response(env['params'])}
        when '/append_entries'
          handle_errors {append_entries_response(env['params'])}
        when '/command'
          handle_errors {command_response(env['params'])}
        else
          error_response(404, 'not found')
        end
      end

      def request_vote_response(params)
        request = Raft::RequestVoteRequest.new(
            params['term'],
            params['candidate_id'],
            params['last_log_index'],
            params['last_log_term'])
        response = @node.handle_request_vote(request)
        [200, {}, {'term' => response.term, 'vote_granted' => response.vote_granted}]
      end

      def append_entries_response(params)
        Raft::AppendEntriesRequest.new(
            params['term'],
            params['leader_id'],
            params['prev_log_index'],
            params['prev_log_term'],
            params['entries'],
            params['commit_index'])
        response = @node.handle_append_entries(request)
        [200, {}, {'term' => response.term, 'success' => response.success}]
      end

      def command_response(params)
        Raft::CommandRequest.new(params['command'])
        response = @node.handle_command(request)
        [response.success ? 200 : 409, {}, {'success' => response.success}]
      end

      def handle_errors
        yield
      rescue StandardError => se
        error_response(422, se.message)
      rescue Exception => e
        error_response(500, e.message)
      end

      def error_response(code, message)
        [code, {}, {'error' => message}]
      end
    end

    #TODO: implement HttpJsonRpcProvider!
    class HttpJsonRpcProvider < Raft::RpcProvider
      attr_reader :url_generator

      def initialize(url_generator)
        @url_generator = url_generator
      end

      def request_votes(request, cluster)
        #multi = EventMachine::Synchrony::Multi.new
        #cluster.node_ids.each do |node_id|
        #  multi.add node_id, EventMachine::HttpRequest.new(url_generator.call(node_id)).apost
        #end
      end

      def append_entries(request, cluster)
        raise "Your RpcProvider subclass must implement #append_entries"
      end

      def append_entries_to_follower(request, node_id)
        raise "Your RpcProvider subclass must implement #append_entries_to_follower"
      end

      def command(request, node_id)
        raise "Your RpcProvider subclass must implement #command"
      end
    end

    class EventMachineAsyncProvider < Raft::AsyncProvider
      def await
        until yield
          f = Fiber.current
          EventMachine::add_timer(0.1) do
            f.resume
          end
          Fiber.yield
        end
      end
    end

    def self.rpc_provider
      HttpJsonRpcProvider.new
    end

    def self.async_provider
      EventMachineAsyncProvider.new
    end

    def initialize(node)

    end

    attr_reader :update_fiber
    attr_reader :running

    def start
      @runner = Goliath::Runner.new(ARGV, nil)
      @runner.api = HttpJsonRpcResponder.new(node)
      @runner.app = Goliath::Rack::Builder.build(HttpJsonRpcResponder, runner.api)
      @runner.run

      @running = true

      runner = self
      @update_fiber = Fiber.new do
        while runner.running?
          EventMachine.add_timer node.config.update_interval, Proc.new do
            runner.update_fiber.resume if runner.running?
          end
          @node.update
          Fiber.yield
        end
      end
    end

    def stop
      @running = false
      f = Fiber.current
      while @update_fiber.alive?
        EventMachine.add_timer 0.1, proc { f.resume }
        Fiber.yield
      end
    end
  end
end


