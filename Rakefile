# -*- ruby -*-

require 'rubygems'
require 'hoe'
require './lib/conveyor'

Hoe.new('conveyor', Conveyor::VERSION) do |p|
  p.rubyforge_name = 'conveyor'
  p.author = 'Ryan King'
  p.email = 'ryan@theryanking.com'
  p.summary = 'Like TiVo for your data.'
  p.description = p.paragraphs_of('README.txt', 2..5).join("\n\n")
  p.url = p.paragraphs_of('README.txt', 0).first.split(/\n/)[1..-1]
  p.changes = p.paragraphs_of('History.txt', 0..1).join("\n\n")
  p.extra_deps << ['mongrel']
  p.extra_deps << ['activesupport']
  p.extra_deps << ['json']
end

# vim: syntax=Ruby
