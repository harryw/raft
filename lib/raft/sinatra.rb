require_relative '../raft'
require_relative 'common'

require 'sinatra'

module Raft
  class Sinatra

    class HttpJsonRpcResponder < Sinatra::Base

      include Raft::JsonResponder

      def initialize(node)
        @node = node
      end

      HEADERS = { 'Content-Type' => 'application/json' }

      post '/request_vote' do
        handle_errors { request_vote_response(@node, env['params']) }
      end

      post '/append_entries' do
        handle_errors { append_entries_response(@node, env['params']) }
      end

      post '/command' do
        handle_errors { command_response(@node, env['params']) }
      end

      not_found do
        error_response(404, 'not found')
      end

    end

  end
end
