require_relative '../raft'

require 'goliath'

module Raft
  class Goliath

    def self.log(message)
      STDOUT.write("\n\n")
      STDOUT.write(message)
      STDOUT.write("\n\n")
    end

    class HttpJsonRpcResponder < ::Goliath::API
      use ::Goliath::Rack::Render, 'json'
      use ::Goliath::Rack::Validation::RequestMethod, %w(POST)
      use ::Goliath::Rack::Params

      def initialize(node)
        @node = node
      end

      HEADERS = { 'Content-Type' => 'application/json' }

      def response(env)
        case env['REQUEST_PATH']
        when '/request_vote'
          handle_errors { request_vote_response(env['params']) }
        when '/append_entries'
          handle_errors { append_entries_response(env['params']) }
        when '/command'
          handle_errors { command_response(env['params']) }
        else
          error_response(404, 'not found')
        end
      end

      def request_vote_response(params)
        #STDOUT.write("\nnode #{@node.id} received request_vote from #{params['candidate_id']}, term #{params['term']}\n")
        request = Raft::RequestVoteRequest.new(
            params['term'],
            params['candidate_id'],
            params['last_log_index'],
            params['last_log_term'])
        response = @node.handle_request_vote(request)
        [200, HEADERS, { 'term' => response.term, 'vote_granted' => response.vote_granted }]
      end

      def append_entries_response(params)
        #STDOUT.write("\nnode #{@node.id} received append_entries from #{params['leader_id']}, term #{params['term']}\n")
        entries = params['entries'].map {|entry| Raft::LogEntry.new(entry['term'], entry['index'], entry['command'])}
        request = Raft::AppendEntriesRequest.new(
            params['term'],
            params['leader_id'],
            params['prev_log_index'],
            params['prev_log_term'],
            entries,
            params['commit_index'])
        #STDOUT.write("\nnode #{@node.id} received entries: #{request.entries.pretty_inspect}\n")
        response = @node.handle_append_entries(request)
        #STDOUT.write("\nnode #{@node.id} completed append_entries from #{params['leader_id']}, term #{params['term']} (#{response})\n")
        [200, HEADERS, { 'term' => response.term, 'success' => response.success }]
      end

      def command_response(params)
        request = Raft::CommandRequest.new(params['command'])
        response = @node.handle_command(request)
        [response.success ? 200 : 409, HEADERS, { 'success' => response.success }]
      end

      def handle_errors
        yield
      rescue StandardError => se
        error_response(422, se)
      rescue Exception => e
        error_response(500, e)
      end

      def error_message(exception)
        "#{exception.message}\n\t#{exception.backtrace.join("\n\t")}".tap {|m| STDOUT.write("\n\n\t#{m}\n\n")}
      end

      def error_response(code, exception)
        [code, HEADERS, { 'error' => error_message(exception) }]
      end
    end

    module HashMarshalling
      def self.hash_to_object(hash, klass)
        object = klass.new
        hash.each_pair do |k, v|
          object.send("#{k}=", v)
        end
        object
      end

      def self.object_to_hash(object, attrs)
        attrs.reduce({}) { |hash, attr|
          hash[attr] = object.send(attr); hash
        }
      end
    end

    class HttpJsonRpcProvider < Raft::RpcProvider
      attr_reader :uri_generator

      def initialize(uri_generator)
        @uri_generator = uri_generator
      end

      def request_votes(request, cluster, &block)
        sent_hash = HashMarshalling.object_to_hash(request, %w(term candidate_id last_log_index last_log_term))
        sent_json = MultiJson.dump(sent_hash)
        deferred_calls = []
        EM.synchrony do
          cluster.node_ids.each do |node_id|
            next if node_id == request.candidate_id
            http = EventMachine::HttpRequest.new(uri_generator.call(node_id, 'request_vote')).apost(
                :body => sent_json,
                :head => { 'Content-Type' => 'application/json' })
            http.callback do
              if http.response_header.status == 200
                received_hash = MultiJson.load(http.response)
                response = HashMarshalling.hash_to_object(received_hash, Raft::RequestVoteResponse)
                #STDOUT.write("\n\t#{node_id} responded #{response.vote_granted} to #{request.candidate_id}\n\n")
                yield node_id, request, response
              else
                Raft::Goliath.log("request_vote failed for node '#{node_id}' with code #{http.response_header.status}")
              end
            end
            deferred_calls << http
          end
        end
        deferred_calls.each do |http|
          EM::Synchrony.sync http
        end
      end

      def append_entries(request, cluster, &block)
        deferred_calls = []
        EM.synchrony do
          cluster.node_ids.each do |node_id|
            next if node_id == request.leader_id
            deferred_calls << create_append_entries_to_follower_request(request, node_id, &block)
          end
        end
        deferred_calls.each do |http|
          EM::Synchrony.sync http
        end
      end

      def append_entries_to_follower(request, node_id, &block)
#        EM.synchrony do
          create_append_entries_to_follower_request(request, node_id, &block)
#        end
      end

      def create_append_entries_to_follower_request(request, node_id, &block)
        sent_hash = HashMarshalling.object_to_hash(request, %w(term leader_id prev_log_index prev_log_term entries commit_index))
        sent_hash['entries'] = sent_hash['entries'].map {|obj| HashMarshalling.object_to_hash(obj, %w(term index command))}
        sent_json = MultiJson.dump(sent_hash)
        raise "replicating to self!" if request.leader_id == node_id
        #STDOUT.write("\nleader #{request.leader_id} replicating entries to #{node_id}: #{sent_hash.pretty_inspect}\n")#"\t#{caller[0..4].join("\n\t")}")

        http = EventMachine::HttpRequest.new(uri_generator.call(node_id, 'append_entries')).apost(
            :body => sent_json,
            :head => { 'Content-Type' => 'application/json' })
        http.callback do
          #STDOUT.write("\nleader #{request.leader_id} calling back to #{node_id} to append entries\n")
          if http.response_header.status == 200
            received_hash = MultiJson.load(http.response)
            response = HashMarshalling.hash_to_object(received_hash, Raft::AppendEntriesResponse)
            yield node_id, response
          else
            Raft::Goliath.log("append_entries failed for node '#{node_id}' with code #{http.response_header.status}")
          end
        end
        http
      end

      def command(request, node_id)
        sent_hash = HashMarshalling.object_to_hash(request, %w(command))
        sent_json = MultiJson.dump(sent_hash)
        http = EventMachine::HttpRequest.new(uri_generator.call(node_id, 'command')).apost(
            :body => sent_json,
            :head => { 'Content-Type' => 'application/json' })
        http = EM::Synchrony.sync(http)
        if http.response_header.status == 200
          received_hash = MultiJson.load(http.response)
          HashMarshalling.hash_to_object(received_hash, Raft::CommandResponse)
        else
          Raft::Goliath.log("command failed for node '#{node_id}' with code #{http.response_header.status}")
          CommandResponse.new(false)
        end
      end
    end

    class EventMachineAsyncProvider < Raft::AsyncProvider
      def await
        f = Fiber.current
        until yield
          EM.next_tick {f.resume}
          Fiber.yield
        end
      end
    end

    def self.rpc_provider(uri_generator)
      HttpJsonRpcProvider.new(uri_generator)
    end

    def self.async_provider
      EventMachineAsyncProvider.new
    end

    def initialize(node)
      @node = node
    end

    attr_reader :node
    attr_reader :update_fiber
    attr_reader :running

    def start(options = {})
      @runner = ::Goliath::Runner.new(ARGV, nil)
      @runner.api = HttpJsonRpcResponder.new(node)
      @runner.app = ::Goliath::Rack::Builder.build(HttpJsonRpcResponder, @runner.api)
      @runner.address = options[:address] if options[:address]
      @runner.port = options[:port] if options[:port]
      @runner.run
      @running = true

      update_proc = Proc.new do
        EM.synchrony do
          @node.update
        end
      end
      @update_timer = EventMachine.add_periodic_timer(node.config.update_interval, update_proc)
#      @node.update
    end

    def stop
      @update_timer.cancel
    end
  end
end


