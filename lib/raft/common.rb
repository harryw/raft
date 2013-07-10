module Raft

  module JsonResponder

    HEADERS = { 'Content-Type' => 'application/json' }

    def request_vote_response(node, params)
      #STDOUT.write("\nnode #{node.id} received request_vote from #{params['candidate_id']}, term #{params['term']}\n")
      request = Raft::RequestVoteRequest.new(
          params['term'],
          params['candidate_id'],
          params['last_log_index'],
          params['last_log_term'])
      response = node.handle_request_vote(request)
      [200, HEADERS, { 'term' => response.term, 'vote_granted' => response.vote_granted }]
    end

    def append_entries_response(node, params)
      #STDOUT.write("\nnode #{node.id} received append_entries from #{params['leader_id']}, term #{params['term']}\n")
      entries = params['entries'].map { |entry| Raft::LogEntry.new(entry['term'], entry['index'], entry['command']) }
      request = Raft::AppendEntriesRequest.new(
          params['term'],
          params['leader_id'],
          params['prev_log_index'],
          params['prev_log_term'],
          entries,
          params['commit_index'])
      #STDOUT.write("\nnode #{node.id} received entries: #{request.entries.pretty_inspect}\n")
      response = node.handle_append_entries(request)
      #STDOUT.write("\nnode #{node.id} completed append_entries from #{params['leader_id']}, term #{params['term']} (#{response})\n")
      [200, HEADERS, { 'term' => response.term, 'success' => response.success }]
    end

    def command_response(node, params)
      request = Raft::CommandRequest.new(params['command'])
      response = node.handle_command(request)
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
      "#{exception.message}\n\t#{exception.backtrace.join("\n\t")}".tap { |m| STDOUT.write("\n\n\t#{m}\n\n") }
    end

    def error_response(code, exception)
      [code, HEADERS, { 'error' => error_message(exception) }]
    end

  end
end
