(function() {
  /*
  Copyright (c) 2002-2011 "Neo Technology,"
  Network Engine for Objects in Lund AB [http://neotechnology.com]

  This file is part of Neo4j.

  Neo4j is free software: you can redistribute it and/or modify
  it under the terms of the GNU Affero General Public License as
  published by the Free Software Foundation, either version 3 of the
  License, or (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU Affero General Public License for more details.

  You should have received a copy of the GNU Affero General Public License
  along with this program. If not, see <http://www.gnu.org/licenses/>.
  */  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; }, __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  };
  define(["./Queue", "./Search"], function(Queue, Search) {
    var QueuedSearch;
    return QueuedSearch = (function() {
      __extends(QueuedSearch, Search);
      function QueuedSearch(server) {
        this.pickSearcher = __bind(this.pickSearcher, this);;
        this.executeNextJob = __bind(this.executeNextJob, this);;
        this.jobDone = __bind(this.jobDone, this);;
        this.jobAdded = __bind(this.jobAdded, this);;
        this.exec = __bind(this.exec, this);;        QueuedSearch.__super__.constructor.call(this, server);
        this.queue = new Queue;
        this.queue.bind("item:pushed", this.jobAdded);
        this.isSearching = false;
      }
      QueuedSearch.prototype.exec = function(statement) {
        var promise;
        promise = new neo4j.Promise;
        this.queue.push({
          statement: statement,
          promise: promise
        });
        return promise;
      };
      QueuedSearch.prototype.jobAdded = function() {
        if (!this.isSearching) {
          return this.executeNextJob();
        }
      };
      QueuedSearch.prototype.jobDone = function() {
        this.isSearching = false;
        if (this.queue.hasMoreItems()) {
          return this.executeNextJob();
        }
      };
      QueuedSearch.prototype.executeNextJob = function() {
        var job, jobDone;
        job = this.queue.pull();
        this.isSearching = true;
        jobDone = __bind(function(result) {
          job.promise.fulfill(result);
          return setTimeout(this.jobDone, 0);
        }, this);
        return QueuedSearch.__super__.exec.call(this, job.statement).then(jobDone, jobDone);
      };
      QueuedSearch.prototype.pickSearcher = function(statement) {
        var searcher, _i, _len, _ref;
        _ref = this.searchers;
        for (_i = 0, _len = _ref.length; _i < _len; _i++) {
          searcher = _ref[_i];
          if (searcher.match(statement)) {
            return searcher;
          }
        }
      };
      return QueuedSearch;
    })();
  });
}).call(this);
