Before do
  @goliaths = {}
  rpc_provider =
  @config = Raft::Config.new(
      Raft::Goliath.rpc_provider(Proc.new {|node_id, message| URI("http://localhost:#{node_id}/#{message}")}),
      Raft::Goliath.async_provider,
      2, #election_timeout seconds
      0.1, #update_interval seconds
      0.5) #heartbeat_interval second
  @cluster = Raft::Cluster.new
end

After do
  puts "AFTER"
  @goliaths.values.each {|goliath| goliath.stop}
  EventMachine.stop
end

def create_node_on_port(port)
  @cluster.node_ids << port
  node = Raft::Node.new(port, @config, @cluster)
  @goliaths[port] = Raft::Goliath.new(node) {|command| puts "executing command #{command}"}
  @goliaths[port].start(port: port)
end

def state_code(state)
  case state
  when /leader/i
    Raft::Node::LEADER_ROLE
  when /follower/i
    Raft::Node::FOLLOWER_ROLE
  when /candidate/i
    Raft::Node::CANDIDATE_ROLE
  end
end

def clear_log_on_node(node)
  log = node.persistent_state.log
  log.clear if log.any?
end

def update_log_on_node(node, new_log)
  clear_log_on_node(node)
  log = node.persistent_state.log
  new_log.each {|log_entry| log << log_entry}
end

Given(/^there is a node on port (\d+)$/) do |port|
  create_node_on_port(port)
end

When(/^I send the command "(.*?)" to the node on port (\d+)$/) do |command, port|
  http = EventMachine::HttpRequest.new("http://localhost:#{port}/command").apost(
      :body => %Q({"command": "#{command}"}),
      :head => { 'Content-Type' => 'application/json' })
  http.timeout 4
  http = EM::Synchrony.sync(http)
  fail "request unfinished" unless http.finished?
end

Then(/^the node on port (\d+) should be in the "(.*?)" state$/) do |port, state|
  @goliaths[port].node.role.should == state_code(state)
end

Given(/^there are nodes on the following ports:$/) do |table|
  table.raw.each do |row|
    create_node_on_port(row[0])
  end
end

Then(/^just one of the nodes should be in the "(.*?)" state$/) do |state|
  @goliaths.values.select {|goliath| goliath.node.role == state_code(state)}
end

Given(/^all the nodes have empty logs$/) do
  @goliaths.values.each do |goliath|
    clear_log_on_node(goliath.node)
  end
end

Given(/^the nodes on port (\d+) has an empty log$/) do |port|
  clear_log_on_node(@goliaths[port].node)
end

Given(/^the node on port (\d+) has the following log:$/) do |port, table|
  log = table.hashes.map {|row| Raft::LogEntry.new(row['term'], row['index'], row['command'])}.to_a
  update_log_on_node(port, log)
end

Given(/^the node on port (\d+)'s current term is (\d+)$/) do |port, term|
  @goliaths[port].node.persistent_state.current_term = term.to_i
end

Then(/^a single node on one of the following ports should be in the "(.*?)" state:$/) do |state, table|
  table.raw.select {|port| @goliaths[port].node.state == state_code(state)}.should have(1).item
end

Then(/^the node on port (\d+) should have the following log:$/) do |port, table|
  log = table.hashes.map {|row| Raft::LogEntry.new(row['term'], row['index'], row['command'])}.to_a
  @goliaths[port].node.persistent_state.log.should == log
end

When(/^I await full replication$/) do
  EM::Synchrony.sleep(5)
end

Given(/^the node port port (\d+) has as commit index of (\d+)$/) do |port, commit_index|
  @goliaths[port].node.temporary_state.commit_index = commit_index.to_i
end
