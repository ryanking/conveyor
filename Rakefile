# -*- ruby -*-

require 'rubygems'
require 'hoe'
require './lib/conveyor'

Hoe.new('conveyor', Conveyor::VERSION) do |p|
  p.rubyforge_name = 'conveyor'
  p.author = 'Ryan King'
  p.email = 'ryan@theryanking.com'
  p.remote_rdoc_dir = ''
  p.extra_deps << ['mongrel']
  p.extra_deps << ['json']
  p.extra_deps << ['daemons']
  p.extra_deps << ['rack']
end

# vim: syntax=Ruby