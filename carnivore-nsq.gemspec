$LOAD_PATH.unshift File.expand_path(File.dirname(__FILE__)) + '/lib/'
require 'carnivore-nsq/version'
Gem::Specification.new do |s|
  s.name = 'carnivore-nsq'
  s.version = Carnivore::Nsq::VERSION.version
  s.summary = 'Message processing helper'
  s.author = 'Chris Roberts'
  s.email = 'chrisroberts.code@gmail.com'
  s.homepage = 'https://github.com/carnivore-rb/carnivore-nsq'
  s.description = 'Carnivore NSQ source'
  s.require_path = 'lib'
  s.license = 'Apache 2.0'
  s.add_dependency 'carnivore', '>= 0.1.8'
  s.add_dependency 'krakow', '>= 0.3.6'
  s.files = Dir['lib/**/*'] + %w(carnivore-nsq.gemspec README.md CHANGELOG.md CONTRIBUTING.md LICENSE)
end
