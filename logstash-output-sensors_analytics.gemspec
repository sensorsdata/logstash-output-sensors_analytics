Gem::Specification.new do |s|
  s.name          = 'logstash-output-sensors_analytics'
  s.version       = '0.1.3'
  s.licenses      = ['Apache License (2.0)']
  s.summary       = 'Output plugin for Sensors Analytics'
  s.description   = 'This gem is a logstash plugin required to be installed on top of the Logstash core pipeline using $LS_HOME/bin/logstash-plugin install gemname. This gem is not a stand-alone program.'
  s.homepage      = 'https://manual.sensorsdata.cn/'
  s.authors       = ['Feng Jiajie']
  s.email         = 'fengjiajie@sensorsdata.cn'
  s.require_paths = ['lib']

  # Files
  s.files = Dir['lib/**/*','spec/**/*','vendor/**/*','*.gemspec','*.md','CONTRIBUTORS','Gemfile','LICENSE','NOTICE.TXT']
   # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "output" }

  # Gem dependencies
  s.add_runtime_dependency "logstash-core-plugin-api", ">= 1.60", "<= 2.99"
  s.add_runtime_dependency "logstash-codec-plain"
  s.add_runtime_dependency 'logstash-mixin-http_client', ">= 6.0.0", "< 8.0.0"
  s.add_runtime_dependency 'stud', "~> 0.0.23"

  s.add_development_dependency "logstash-devutils"
  s.add_development_dependency 'sinatra'
  s.add_development_dependency 'webrick'
end
