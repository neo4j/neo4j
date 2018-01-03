###
Copyright (c) 2002-2018 "Neo Technology,"
Network Engine for Objects in Lund AB [http://neotechnology.com]

This file is part of Neo4j.

Neo4j is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
###

define(
  ["./Queue", 
   "./Search",], 
  (Queue, Search) ->

    class QueuedSearch extends Search

      constructor : (server) ->
        super(server)
        @queue = new Queue
        @queue.bind("item:pushed", @jobAdded)
        @isSearching = false

      exec : (statement) =>
        promise = new neo4j.Promise
        @queue.push {statement : statement, promise : promise}
        return promise

      jobAdded : () =>
        if not @isSearching
          @executeNextJob()

      jobDone : () =>
        @isSearching = false
        if @queue.hasMoreItems()
          @executeNextJob()

      executeNextJob : () =>
        job = @queue.pull()
        @isSearching = true

        jobCompletionCallback = (result) =>
          @jobDone()
          job.promise.fulfill(result)
  
        QueuedSearch.__super__.exec.call(this, job.statement).then(jobCompletionCallback, jobCompletionCallback)

)

