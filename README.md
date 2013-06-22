# Raft

This is a Ruby implementation of the Raft algorithm.

Raft is a distributed consensus algorithm designed to be easy to understand. The algorithm is the work of Diego Ongaro
and John Ousterhout at Stanford University.  The implementation here is based upon this paper:

https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf

Many thanks to the authors for their hard work!

## Technical Design

This gem provides a `Raft::Node` class that handles log replication across a cluster of peer nodes.  Design decisions
about the RPC protocol, concurrency mechanism, error handling and data persistence are left to the client.

For convenience and testing, an example implementation is provided based on HTTP and EventMachine, with in-memory
data persistence.  Contributions of further examples are very welcome!

## Usage

Raft replicates commands across a cluster of nodes.  Each node in the cluster is aware of every other node in the
cluster.  Let's create a new cluster and define its configuration:


```ruby
@cluster = Raft::Cluster.new('alpha', 'beta', 'gamma')

@config = Raft::Config.new(
  rpc_provider,       # see Raft::RpcProvider
  async_provider,     # see Raft::AsyncProvider
  election_timeout,   # in seconds
  election_splay,     # in seconds
  update_interval,    # in seconds
  heartbeat_interval) # in seconds
```

Now we can create Raft nodes for each node defined in the cluster:

```ruby
@nodes = @cluster.node_ids.map do |node_id|
  Raft::Node.new(node_id, @config, @cluster)
end
```

Since the concurrency mechanism is left to the client, you must call `Raft::Node#update` regularly to allow the
node to participate in the cluster:

```ruby
# Threaded example:
@update_threads = @nodes.map do |node|
 Thread.new do
   while true
     node.update
     sleep(node.config.update_interval)
   end
 end
end
```
```ruby
# Evented example
@update_timers =  @nodes.map do |node|
  EventMachine.add_periodic_timer(node.config.update_interval) do
    EM.synchrony do
      node.update
    end
  end
end
```

We can send commands (which are strings) to the cluster and they will be appended to the command log, which will be
replicated across the cluster.

```ruby
command = 'example'
request = Raft::CommandRequest.new(command)
node = @nodes.sample
response = node.handle_command(request) # response is a Raft::CommandResponse
```

`Raft::Node#handle_command` will not return success until the command has been replicated to a majority of nodes,
so that it is considered /committed/ and is safe to execute.

If you would like to execute commands as they are committed, you can assign a commit handler for each node:

```ruby
@nodes.each do |node|
  node.commit_handler = Proc.new do |command|
    puts "Node #{node.id} executing command #{command}!"
  end
end
```

