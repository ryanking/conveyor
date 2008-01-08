feeder-ng
    by Ryan King
    http://theryanking.com/

== DESCRIPTION:
  
A feeder.

== FEATURES/PROBLEMS:
  
* Rewindable broadcast of data.

== SYNOPSIS:

  You put stuff in and in it comes back out!

== REQUIREMENTS:

* Ruby

== INSTALL:

* gem install feeder-ng

== FILE FORMATS
===  DATA FILES

id time offset length hash
content
...

contrived example:

1213124 2008-01-05T13:35:32 1234 11 asdfasdfasdfasdfasdfasdfasdfa
foo bar bam

* space separated line of metadata followed by content
* delimiter might be useful for sanity checking, but the hash could probably suffice for ensuring that the offset was calculated and persisted properly. We should look at what ARC does here.
* offset is to beginning of metadata line
* length doesn't include a trailing \n that separates the content from the next bit of metadata (this might not be necessary)

=== INDEX FILES

id time offset length hash file

contrived example:

1213124 2008-01-05T13:35:32 1234 11 asdfasdfasdfasdfasdfasdfasdfa 1

notes:
* 1 is the filename
* assuming a lucene-style directory of datafiles + ToC/index
* given that the files are written sequentially we can avoid writing every entry to the index file (as long as you write the first and last entry to the index). At most this means you have to read n entries, where n is the gap between index entries. Given that most clients will have persistent connections and be reading sequentially, we can do some clever things on the server side to make this really efficient (basically meaning you'll only have to pay that penalty on the first item you read).


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
