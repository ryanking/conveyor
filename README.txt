= Conveyor

by Ryan King (http://theryanking.com)

== Description

* Like TiVo for your data
* A distributed rewindable multi-queue

== Overview

A Conveyor server provides an HTTP interface that allows for POSTing and GETing items in streams called Channels.

POSTing is simple: you add an item to the channel and it gets persisted and assigned a sequence number.

Consuming items from a Channel is more flexible, you can:

* consume by id number ("GET /channels/foo/1337")
* consume by from a global queue ("GET /channels/foo?next")
* consume from a queue group ("GET /channels/foo?next&group=bar")
  * this allows multiple groups of consumers to each have what appears to them to be a queue.

The payload for all of these is a stream of bytes. Conveyor will stream it back exactly as it was given.

== REQUIREMENTS:

* Ruby
* Mongrel
* active_support

== INSTALL:

* gem install conveyor


== LICENSE:

(The MIT License)

Copyright (c) 2008 Ryan King

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
'Software'), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
