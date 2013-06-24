Gem::Specification.new do |s|
  s.name        = 'raft'
  s.version     = '0.1.3'
  s.date        = '2013-06-23'
  s.summary     = "A simple Raft distributed consensus implementation"
  s.description = s.summary
  s.authors     = ["Harry Wilkinson"]
  s.email       = 'hwilkinson@mdsol.com'
  s.files       = ["lib/raft.rb", "lib/raft/goliath.rb"]
  s.homepage    = 'http://github.com/harryw/raft'
  s.license     = 'MIT'

  s.add_dependency 'goliath', '~> 1.0'
  s.add_dependency 'multi_json', '~> 1.3'

  s.add_development_dependency 'cucumber', '~> 1.0'
  s.add_development_dependency 'em-http-request', '~> 1.0'
  s.add_development_dependency 'rspec', '~> 2.0'

end
