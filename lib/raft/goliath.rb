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
        STDOUT.write("\n\n#{self.class} #{__method__}\n\n")
        @node = node
      end

      HEADERS = { 'Content-Type' => 'application/json' }

      def response(env)
        STDOUT.write("\n\n#{self.class} #{__method__} #{__LINE__}\n\n")
        STDOUT.write("\n\n#{env.pretty_inspect}\n\n")
        resp = case env['REQUEST_PATH']
        when '/request_vote'
          handle_errors { request_vote_response(env['params']) }
        when '/append_entries'
          handle_errors { append_entries_response(env['params']) }
        when '/command'
          handle_errors { command_response(env['params']) }
        else
          error_response(404, 'not found')
        end
        STDOUT.write("\n\n#{self.class} #{__method__} #{__LINE__}\n\n")
        Raft::Goliath.log(resp.pretty_inspect)
        resp
      end

      def request_vote_response(params)
        request = Raft::RequestVoteRequest.new(
            params['term'],
            params['candidate_id'],
            params['last_log_index'],
            params['last_log_term'])
        response = @node.handle_request_vote(request)
        [200, HEADERS, { 'term' => response.term, 'vote_granted' => response.vote_granted }]
      end

      def append_entries_response(params)
        request = Raft::AppendEntriesRequest.new(
            params['term'],
            params['leader_id'],
            params['prev_log_index'],
            params['prev_log_term'],
            params['entries'],
            params['commit_index'])
        response = @node.handle_append_entries(request)
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
        msg = "#{exception.message}\n\t#{exception.backtrace.join("\n\t")}"
        STDOUT.write("\n\nerror message: #{msg}\n\n")
        msg
      end

      def error_response(code, exception)
        STDOUT.write("\n\n#{self.class} #{__method__} #{__LINE__}\n\n")
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

    #TODO: implement HttpJsonRpcProvider!
    class HttpJsonRpcProvider < Raft::RpcProvider
      attr_reader :uri_generator

      def initialize(uri_generator)
        STDOUT.write("\n\n#{self.class} #{__method__}\n\n")
        @uri_generator = uri_generator
      end

      def request_votes(request, cluster, &block)
        sent_hash = HashMarshalling.object_to_hash(request, %w(term candidate_id last_log_index last_log_term))
        sent_json = MultiJson.dump(sent_hash)
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
                yield node_id, response
              else
                Raft::Goliath.log("request_vote failed for node '#{node_id}' with code #{http.response_header.code}")
              end
            end
          end
        end
      end

      def append_entries(request, cluster, &block)
        cluster.node_ids.each do |node_id|
          next if node_id == request.leader_id
          append_entries_to_follower(request, node_id, &block)
        end
      end

      def append_entries_to_follower(request, node_id, &block)
        sent_hash = HashMarshalling.object_to_hash(request, %w(term leader_id prev_log_index prev_log_term entries commit_index))
        sent_json = MultiJson.dump(sent_hash)
        STDOUT.write("sent_json: #{sent_json}")
        EM.synchrony do
          http = EventMachine::HttpRequest.new(uri_generator.call(node_id, 'append_entries')).apost(
              :body => sent_json,
              :head => { 'Content-Type' => 'application/json' })
          http.callback do
            if http.response_header.status == 200
              received_hash = MultiJson.load(http.response)
              STDOUT.write("received_hash: #{received_hash}")
              response = HashMarshalling.hash_to_object(received_hash, Raft::AppendEntriesResponse)
              yield node_id, response
            else
              Raft::Goliath.log("append_entries failed for node '#{node_id}' with code #{http.response_header.status}")
            end
          end
        end
      end

      def command(request, node_id)
        sent_hash = HashMarshalling.object_to_hash(request, %w(command))
        sent_json = MultiJson.dump(sent_hash)
        STDOUT.write("\nsent_json: #{sent_json}\n")
        http = EventMachine::HttpRequest.new(uri_generator.call(node_id, 'command')).apost(
            :body => sent_json,
            :head => { 'Content-Type' => 'application/json' })
        http = EM::Synchrony.sync(http)
              STDOUT.write("\nresponse status: #{http.response_header.status}\n")
              STDOUT.write("\nresponse: #{http.pretty_inspect}\n")
        if http.response_header.status == 200
          received_hash = MultiJson.load(http.response)
          STDOUT.write("received_hash: #{received_hash}")
          HashMarshalling.hash_to_object(received_hash, Raft::CommandResponse)
        else
          Raft::Goliath.log("command failed for node '#{node_id}' with code #{http.response_header.code}")
          CommandResponse.new(false)
        end
      end
    end

    class EventMachineAsyncProvider < Raft::AsyncProvider
      def await
        until yield
          EM::Synchrony.sleep(0.1)
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

      @update_timer = EventMachine.add_periodic_timer node.config.update_interval, Proc.new { STDOUT.write("\n\nUPDATE\n\n"); @node.update }
      @node.update
    end

    def stop
      @update_timer.cancel
    end
  end
end


