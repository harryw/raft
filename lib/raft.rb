module Raft
  Config = Struct.new(:rpc_provider, :async_provider, :election_timeout, :update_interval, :heartbeat_interval)

  class Cluster
    attr_accessible :node_ids

    def quorum
      @node_ids.size / 2 + 1 # integer division rounds down
    end
  end

  PersistentState = Struct.new(:current_term, :voted_for, :log)

  TemporaryState = Struct.new(:commit_index, :leader_id)

  class LeadershipState
    def followers
      @followers ||= {}
    end

    attr_reader :update_timer

    def initialize(update_interval)
      @update_timer = Timer.new(update_interval)
    end
  end

  FollowerState = Struct.new(:next_index, :succeeded)

  LogEntry = Struct.new(:term, :index, :command)

  RequestVoteRequest = Struct.new(:term, :candidate_id, :last_log_index, :last_log_term)

  RequestVoteResponse = Struct.new(:term, :vote_granted)

  AppendEntriesRequest = Struct.new(:term, :leader_id, :prev_log_index, :prev_log_term, :entries, :commit_index)

  AppendEntriesResponse = Struct.new(:term, :success)

  CommandRequest = Struct.new(:command)

  CommandResponse = Struct.new(:success)

  class RpcProvider
    def request_votes(request, cluster)
      raise "Your RpcProvider subclass must implement #request_votes"
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

  class AsyncProvider
    def await
      raise "Your AsyncProvider subclass must implement #await"
    end
  end

  class Timer
    def initialize(interval)
      @timeout = Time.now + timeout
      reset!
    end

    def reset!
      @start = Time.now
    end

    def timeout
      @start + @interval
    end

    def timed_out?
      Time.now > timeout
    end
  end

  class Node
    attr_reader :id
    attr_reader :role
    attr_reader :config
    attr_reader :cluster
    attr_reader :persistent_state
    attr_reader :temporary_state
    attr_reader :election_timer

    FOLLOWER_ROLE = 0
    CANDIDATE_ROLE = 1
    LEADER_ROLE = 2

    def initialize(id, config, cluster)
      @id = id
      @role = FOLLOWER_ROLE
      @config = config
      @cluster = cluster
      @persistent_state = PersistentState.new(0, nil, [])
      @temporary_state = TemporaryState.new(nil, nil)
      @election_timer = Timer.new(config.election_timeout)
    end

    def update
      case @role
      when FOLLOWER_ROLE
        follower_update
      when CANDIDATE_ROLE
        candidate_update
      when LEADER_ROLE
        leader_update
      end
    end

    def follower_update
      if @election_timer.timed_out?
        @role = CANDIDATE_ROLE
        candidate_update
      end
    end
    protected :follower_update

    def candidate_update
      if @election_timer.timed_out?
        @persistent_state.current_term += 1
        @persistent_state.voted_for = @id
        reset_election_timeout
        request = RequestVoteRequest.new(@persistent_state.current_term, @id, @persistent_state.log.last.index, @persistent_state.log.last.term)
        votes_for = 1 # candidate always votes for self
        votes_against = 0
        quorum = @cluster.quorum
        elected = @config.rpc_provider.request_votes(request, @cluster) do |_, response|
          if response.term > @persistent_state.current_term
            @role = FOLLOWER_ROLE
            return false
          elsif response.vote_granted
            votes_for += 1
            return false if votes_for >= quorum
          else
            votes_against += 1
            return false if votes_against >= quorum
          end
          nil # no majority result yet
        end
        if elected
          @role = LEADER_ROLE
          establish_leadership
        end
      end
    end
    protected :candidate_update

    def leader_update
      if @leadership_state.update_timer.timed_out?
        @leadership_state.update_timer.reset!
        send_heartbeats
      end
      @temporary_state.commit_index = @leadership_state.followers.values.
          select {|follower_state| follower_state.succeeded}.
          map {|follower_state| follower_state.next_index}.
          sort[@cluster.quorum - 1]
    end
    protected :leader_update

    def establish_leadership
      @leadership_state = LeadershipState.new(@config.update_interval)
      @cluster.node_ids.each do |node_id|
        follower_state = (@leadership_state.followers[node_id] ||= FollowerState.new)
        follower_state.next_index = @persistent_state.log.size + 1
        follower_state.succeeded = false
      end
      send_heartbeats
    end
    protected :establish_leadership

    def send_heartbeats
      request = AppendEntriesRequest.new(
          @persistent_state.current_term,
          @id,
          @persistent_state.log.size - 1,
          @persistent_state.log.last.term,
          [],
          @temporary_state.commit_index)

      @config.rpc_provider.append_entries(request, @cluster) do |node_id, response|
        append_entries_to_follower(node_id, request, response)
      end
    end
    protected :send_heartbeats

    def append_entries_to_follower(node_id, request, response)
      if response.success
        @leadership_state.followers[node_id].next_index = request.log.last.index
        @leadership_state.followers[node_id].succeeded = true
      elsif response.term <= @persistent_state.current_term
        @config.rpc_provider.append_entries_to_follower(request, node_id) do |node_id, response|
          prev_log_index = request.prev_log_index > 1 ? request.prev_log_index - 1 : nil
          prev_log_term = nil
          entries = @persistent_state.log
          unless prev_log_index.nil?
            prev_log_term = @persistent_state.log[prev_log_index].term
            entries = @persistent_state.log.slice((prev_log_index + 1)..-1)
          end
          next_request = AppendEntriesRequest.new(
              @persistent_state.current_term,
              @id,
              prev_log_index,
              prev_log_term,
              entries,
              @temporary_state.commit_index)
          @config.rpc_provider.append_entries_to_follower(next_request, @temporary_state.leader_id) do |node_id, response|
            append_entries_to_follower(node_id, next_request, response)
          end
        end
      end
    end
    protected :append_entries_to_follower

    def handle_request_vote(request)
      response = RequestVoteResponse.new
      response.term = @persistent_state.current_term
      response.vote_granted = false

      return response if request.term < @persistent_state.current_term

      @temporary_state.leader_id = nil if request.term > @persistent_state.current_term

      step_down_if_new_term(request.term)

      if FOLLOWER_ROLE == @role
        if @persistent_state.voted_for == request.candidate_id
          response.vote_granted = success
        elsif @persistent_state.voted_for.nil?
          if request.last_log_term == @persistent_state.log.last.term &&
            request.last_log_index < @persistent_state.log.last.index
            # candidate's log is incomplete compared to this node
          elsif request.last_log_term < @persistent_state.log.last.term
            # candidate's log is incomplete compared to this node
            @persistent_state.voted_for = request.candidate_id
          else
            @persistent_state.voted_for = request.candidate_id
            response.vote_granted = true
          end
        end
        reset_election_timeout if response.vote_granted
      end

      response
    end

    def handle_append_entries(request)
      response = AppendEntriesResponse.new
      response.term = @persistent_state.current_term
      response.success = false

      return response if request.term < @persistent_state.current_term

      step_down_if_new_term(request.term)

      reset_election_timeout

      @temporary_state.leader_id = request.leader_id

      abs_log_index = abs_log_index_for(request.prev_log_index, request.prev_log_term)
      return response if abs_log_index.nil? && !request.prev_log_index.nil? && !request.prev_log_term.nil?

      raise "Cannot truncate committed logs" if abs_log_index < @temporary_state.commit_index

      truncate_and_update_log(abs_log_index, request.entries)

      return response unless update_commit_index(request.commit_index)

      response.success = true
      response
    end

    def handle_command(request)
      response = CommandResponse.new(false)
      case @role
      when FOLLOWER_ROLE
        response = @config.rpc_provider.command(request, @temporary_state.leader_id)
      when CANDIDATE_ROLE
        await_leader
        response = handle_command(request)
      when LEADER_ROLE
        log_entry = LogEntry.new(@persistent_state.current_term, @persistent_state.log.last.index + 1, request.command)
        @persistent_state.log << log_entry
        await_consensus(log_entry)
      end
      response
    end

    def await_consensus(log_entry)
      @config.async_provider.await do
        persisted_log_entry = @persistent_state.log[log_entry.index - 1]
        @temporary_state.commit_index >= log_entry.index &&
            persisted_log_entry.term == log_entry.term &&
            persisted_log_entry.command == log_entry.command
      end
    end
    protected :await_consensus

    def await_leader
      @config.async_provider.await do
        @role != CANDIDATE_ROLE && !@temporary_state.leader_id.nil?
      end
    end
    protected :await_leader

    def step_down_if_new_term(request_term)
      if request_term > @persistent_state.current_term
        @persistent_state.current_term = request_term
        @persistent_state.voted_for = nil
        @role = FOLLOWER_ROLE
      end
    end
    protected :step_down_if_new_term

    def reset_election_timeout
      @election_timer.reset!
    end
    protected :reset_election_timeout

    def abs_log_index_for(prev_log_index, prev_log_term)
      @persistent_state.log.rindex {|log_entry| log_entry.index == prev_log_index && log_entry.term == prev_log_term}
    end
    protected :log_index_contains_term

    def truncate_and_update_log(abs_log_index, entries)
      log = @persistent_state.log
      if abs_log_index.nil?
        log = []
      elsif log.length == abs_log_index + 1
        # no truncation required, past log is the same
      else
        log = log.slice(0..abs_log_index)
      end
      log = log.concat(entries) unless entries.empty?
      @persistent_state.log = log
    end
    protected :truncate_log_to_index_and_update

    def update_commit_index(commit_index)
      return false if @temporary_state.commit_index && @temporary_state.commit_index > commit_index
      @temporary_state.commit_index = commit_index
    end
    protected :update_commit_index
  end
end
