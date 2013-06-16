require 'delegate'

module Raft
  Config = Struct.new(:rpc_provider, :async_provider, :election_timeout, :update_interval, :heartbeat_interval)

  class Cluster
    attr_reader :node_ids

    def initialize
      @node_ids = []
    end

    def quorum
      @node_ids.count / 2 + 1 # integer division rounds down
    end
  end

  class LogEntry
    attr_reader :term, :index, :command

    def initialize(term, index, command)
      @term, @index, @command = term, index, command
    end

    def ==(other)
      [:term, :index, :command].all? do |attr|
        self.send(attr) == other.send(attr)
      end
    end

    def eql?(other)
      self == other
    end

    def hash
      [:term, :index, :command].reduce(0) do |h, attr|
        h ^= self.send(attr)
      end
    end
  end

  class Log < DelegateClass(Array)
    def last(*args)
      self.any? ? super(*args) : LogEntry.new(nil, nil, nil)
    end
  end

  class PersistentState #= Struct.new(:current_term, :voted_for, :log)
    attr_reader :current_term, :voted_for, :log

    def initialize
      @current_term = 0
      @voted_for = nil
      @log = Log.new([])
    end

    def current_term=(new_term)
      raise 'cannot restart an old term' unless @current_term < new_term
      @current_term = new_term
      @voted_for = nil
    end

    def voted_for=(new_votee)
      raise 'cannot change vote for this term' unless @voted_for.nil?
      @voted_for = new_votee
    end

    def log=(new_log)
      @log = Log.new(new_log)
    end
  end

  class TemporaryState
    attr_reader :commit_index
    attr_accessor :leader_id

    def initialize(commit_index, leader_id)
      @commit_index, @leader_id = commit_index, leader_id
    end

    def commit_index=(new_commit_index)
      raise 'cannot uncommit log entries' unless @commit_index.nil? || @commit_index <= new_commit_index
      @commit_index = new_commit_index
    end
  end

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

  RequestVoteRequest = Struct.new(:term, :candidate_id, :last_log_index, :last_log_term)

  #class RequestVoteRequest < Struct.new(:term, :candidate_id, :last_log_index, :last_log_term)
  #  def term; @term.to_i; end
  #  def last_log_index; @last_log_index.to_i; end
  #  def last_log_term; @last_log_term.to_i; end
  #end

  RequestVoteResponse = Struct.new(:term, :vote_granted)

  #class RequestVoteResponse < Struct.new(:term, :vote_granted)
  #  def term; @term.to_i; end
  #end

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
      @interval = interval
      @start = Time.now - interval
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

    def initialize(id, config, cluster, commit_handler=nil, &block)
      @id = id
      @role = FOLLOWER_ROLE
      @config = config
      @cluster = cluster
      @persistent_state = PersistentState.new
      @temporary_state = TemporaryState.new(nil, nil)
      @election_timer = Timer.new(config.election_timeout)
      @commit_handler = commit_handler || (block.to_proc if block_given?)
    end

    def update
      return if @updating
      @updating = true
      indent = "\t" * (@id.to_i % 3)
      #STDOUT.write("\n\n#{indent}update #{@id}, role #{@role}, log length #{@persistent_state.log.count}\n\n")
      case @role
      when FOLLOWER_ROLE
        follower_update
      when CANDIDATE_ROLE
        candidate_update
      when LEADER_ROLE
        leader_update
      end
      @updating = false
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
        last_log_entry = @persistent_state.log.last
        log_index = last_log_entry ? last_log_entry.index : nil
        log_term = last_log_entry ? last_log_entry.term : nil
        request = RequestVoteRequest.new(@persistent_state.current_term, @id, log_index, log_term)
        votes_for = 1 # candidate always votes for self
        votes_against = 0
        quorum = @cluster.quorum
        #STDOUT.write("\n\t\t#{@id} requests votes for term #{@persistent_state.current_term}\n\n")
        @config.rpc_provider.request_votes(request, @cluster) do |_, request, response|
          #STDOUT.write("\n\t\t#{@id} receives vote #{response.vote_granted}\n\n")
          elected = nil # no majority result yet
          if request.term != @persistent_state.current_term
            # this is a response to an out-of-date request, just ignore it
          elsif response.term > @persistent_state.current_term
            @role = FOLLOWER_ROLE
            elected = false
          elsif response.vote_granted
            votes_for += 1
            elected = true if votes_for >= quorum
          else
            votes_against += 1
            elected = false if votes_against >= quorum
          end
          #STDOUT.write("\n\t\t#{@id} receives vote #{response.vote_granted}, elected is #{elected.inspect}\n\n")
          elected
        end
        if votes_for >= quorum
          #STDOUT.write("\n#{@id} becomes leader for term #{@persistent_state.current_term}\n\n")
          @role = LEADER_ROLE
          establish_leadership
        else
          #STDOUT.write("\n\t\t#{@id} not elected leader (for #{votes_for}, against #{votes_against})\n\n")
        end
      end
    end

    protected :candidate_update

    def leader_update
      #STDOUT.write("\nLEADER UPDATE BEGINS\n")
      if @leadership_state.update_timer.timed_out?
        @leadership_state.update_timer.reset!
        send_heartbeats
      end
      if @leadership_state.followers.any?
        new_commit_index = @leadership_state.followers.values.
            select { |follower_state| follower_state.succeeded }.
            map { |follower_state| follower_state.next_index - 1 }.
            sort[@cluster.quorum - 1]
      else
        new_commit_index = @persistent_state.log.size - 1
      end
      handle_commits(new_commit_index)
      #STDOUT.write("\nLEADER UPDATE ENDS\n")
    end

    protected :leader_update

    def handle_commits(new_commit_index)
      #STDOUT.write("\nnode #{@id} handle_commits(new_commit_index = #{new_commit_index}) (@temporary_state.commit_index = #{@temporary_state.commit_index}\n")
      return if new_commit_index == @temporary_state.commit_index
      next_commit = @temporary_state.commit_index.nil? ? 0 : @temporary_state.commit_index + 1
      while next_commit <= new_commit_index
        @commit_handler.call(@persistent_state.log[next_commit].command) if @commit_handler
        @temporary_state.commit_index = next_commit
        next_commit += 1
        #STDOUT.write("\n\tnode #{@id} handle_commits(new_commit_index = #{new_commit_index}) (new @temporary_state.commit_index = #{@temporary_state.commit_index}\n")
      end
    end

    protected :handle_commits

    def establish_leadership
      @leadership_state = LeadershipState.new(@config.update_interval)
      @temporary_state.leader_id = @id
      @cluster.node_ids.each do |node_id|
        next if node_id == @id
        follower_state = (@leadership_state.followers[node_id] ||= FollowerState.new)
        follower_state.next_index = @persistent_state.log.size
        follower_state.succeeded = false
      end
      send_heartbeats
    end

    protected :establish_leadership

    def send_heartbeats
      #STDOUT.write("\nsending heartbeats\n")
      last_log_entry = @persistent_state.log.last
      log_index = last_log_entry ? last_log_entry.index : nil
      log_term = last_log_entry ? last_log_entry.term : nil
      request = AppendEntriesRequest.new(
          @persistent_state.current_term,
          @id,
          log_index,
          log_term,
          [],
          @temporary_state.commit_index)

      @config.rpc_provider.append_entries(request, @cluster) do |node_id, response|
        append_entries_to_follower(node_id, request, response)
      end
    end

    protected :send_heartbeats

    def append_entries_to_follower(node_id, request, response)
      if @role != LEADER_ROLE
        # we lost the leadership
      elsif response.success
        #STDOUT.write("\nappend_entries_to_follower #{node_id} request #{request.pretty_inspect} succeeded\n")
        @leadership_state.followers[node_id].next_index = (request.prev_log_index || -1) + request.entries.count + 1
        @leadership_state.followers[node_id].succeeded = true
      elsif response.term <= @persistent_state.current_term
        #STDOUT.write("\nappend_entries_to_follower #{node_id} request failed (#{request.pretty_inspect}) and responded with #{response.pretty_inspect}\n")
        @config.rpc_provider.append_entries_to_follower(request, node_id) do |node_id, response|
          if @role == LEADER_ROLE # make sure leadership wasn't lost since the request
            #STDOUT.write("\nappend_entries_to_follower #{node_id} callback...\n")
            prev_log_index = (request.prev_log_index.nil? || request.prev_log_index <= 0) ? nil : request.prev_log_index - 1
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
            #STDOUT.write("\nappend_entries_to_follower #{node_id} request #{request.pretty_inspect} failed...\n")
            #STDOUT.write("sending updated request #{next_request.pretty_inspect}\n")
            @config.rpc_provider.append_entries_to_follower(next_request, node_id) do |node_id, response|
              append_entries_to_follower(node_id, next_request, response)
            end
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
          if @persistent_state.log.empty?
            # this node has no log so it can't be ahead
            @persistent_state.voted_for = request.candidate_id
            response.vote_granted = true
          elsif request.last_log_term == @persistent_state.log.last.term &&
              request.last_log_index && request.last_log_index < @persistent_state.log.last.index
            # candidate's log is incomplete compared to this node
          elsif request.last_log_term && request.last_log_term < @persistent_state.log.last.term
            # candidate's log is incomplete compared to this node
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
      #STDOUT.write("\n\nnode #{@id} handle_append_entries: #{request.entries.pretty_inspect}\n\n") if request.prev_log_index.nil?
      response = AppendEntriesResponse.new
      response.term = @persistent_state.current_term
      response.success = false

      return response if request.term < @persistent_state.current_term
      #STDOUT.write("\n\nnode #{@id} handle_append_entries stage 2\n") if request.prev_log_index.nil?

      step_down_if_new_term(request.term)

      reset_election_timeout

      @temporary_state.leader_id = request.leader_id

      abs_log_index = abs_log_index_for(request.prev_log_index, request.prev_log_term)
      return response if abs_log_index.nil? && !request.prev_log_index.nil? && !request.prev_log_term.nil?
      #STDOUT.write("\n\nnode #{@id} handle_append_entries stage 3\n") if request.prev_log_index.nil?
      if @temporary_state.commit_index &&
          abs_log_index &&
          abs_log_index < @temporary_state.commit_index
        raise "Cannot truncate committed logs; @temporary_state.commit_index = #{@temporary_state.commit_index}; abs_log_index = #{abs_log_index}"
      end

      truncate_and_update_log(abs_log_index, request.entries)

      return response unless update_commit_index(request.commit_index)
      #STDOUT.write("\n\nnode #{@id} handle_append_entries stage 4\n") if request.prev_log_index.nil?

      response.success = true
      response
    end

    def handle_command(request)
      response = CommandResponse.new(false)
      case @role
      when FOLLOWER_ROLE
        await_leader
        if @role == LEADER_ROLE
          handle_command(request)
        else
          # forward the command to the leader
          response = @config.rpc_provider.command(request, @temporary_state.leader_id)
        end
      when CANDIDATE_ROLE
        await_leader
        response = handle_command(request)
      when LEADER_ROLE
        last_log = @persistent_state.log.last
        log_entry = LogEntry.new(@persistent_state.current_term, last_log.index ? last_log.index + 1 : 0, request.command)
        @persistent_state.log << log_entry
        await_consensus(log_entry)
        response = CommandResponse.new(true)
      end
      response
    end

    def await_consensus(log_entry)
      @config.async_provider.await do
        persisted_log_entry = @persistent_state.log[log_entry.index]
        !@temporary_state.commit_index.nil? &&
            @temporary_state.commit_index >= log_entry.index &&
            persisted_log_entry.term == log_entry.term &&
            persisted_log_entry.command == log_entry.command
      end
    end

    protected :await_consensus

    def await_leader
      if @temporary_state.leader_id.nil?
        @role = CANDIDATE_ROLE
      end
      @config.async_provider.await do
        @role != CANDIDATE_ROLE && !@temporary_state.leader_id.nil?
      end
    end

    protected :await_leader

    def step_down_if_new_term(request_term)
      if request_term > @persistent_state.current_term
        @persistent_state.current_term = request_term
        @role = FOLLOWER_ROLE
      end
    end

    protected :step_down_if_new_term

    def reset_election_timeout
      @election_timer.reset!
    end

    protected :reset_election_timeout

    def abs_log_index_for(prev_log_index, prev_log_term)
      @persistent_state.log.rindex { |log_entry| log_entry.index == prev_log_index && log_entry.term == prev_log_term }
    end

    protected :abs_log_index_for

    def truncate_and_update_log(abs_log_index, entries)
      log = @persistent_state.log
      if abs_log_index.nil?
        log = []
      elsif log.length == abs_log_index + 1
        # no truncation required, past log is the same
      else
        log = log.slice(0..abs_log_index)
      end
      #STDOUT.write("\n\nentries is: #{entries.pretty_inspect}\n\n")
      log = log.concat(entries) unless entries.empty?
      @persistent_state.log = log
    end

    protected :truncate_and_update_log

    def update_commit_index(new_commit_index)
      #STDOUT.write("\n\n%%%%%%%%%%%%%%%%%%%%% node #{@id} update_commit_index(new_commit_index = #{new_commit_index})\n")
      return false if @temporary_state.commit_index && @temporary_state.commit_index > new_commit_index
      handle_commits(new_commit_index)
      true
    end

    protected :update_commit_index
  end
end
