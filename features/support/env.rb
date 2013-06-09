require 'raft'
require 'raft/goliath'

require 'rspec'
require 'em-synchrony/em-http'

Around do |_, block|
  STDOUT.write("\n\n#{self.class} #{__method__} #{__FILE__} #{__LINE__}\n\n")
  EM.synchrony {block.call}
  STDOUT.write("\n\n#{self.class} #{__method__} #{__FILE__} #{__LINE__}\n\n")
end

