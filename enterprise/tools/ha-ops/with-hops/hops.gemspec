# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "hops/version"

Gem::Specification.new do |s|
  s.name        = "hops"
  s.version     = Hops::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Andreas Kollegger"]
  s.email       = ["andreas.kollegger@neotechnology.com"]
  s.homepage    = "http://neo4j.org"
  s.summary     = %q{Neo4j operations tool}
  s.description = %q{Helps manage a Neo4j installation}

  s.rubyforge_project = "hops"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]
end
