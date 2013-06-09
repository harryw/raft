require 'raft'
require 'raft/goliath'

require 'rspec'
require 'em-synchrony/em-http'

Around do |_, block|
  EM.synchrony {block.call}
end

