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
  */  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; };
  define(["./UrlSearcher", "./NodeSearcher", "./RelationshipSearcher"], function(UrlSearcher, NodeSearcher, RelationshipSearcher) {
    var Search;
    return Search = (function() {
      function Search(server) {
        this.pickSearcher = __bind(this.pickSearcher, this);;
        this.exec = __bind(this.exec, this);;        this.searchers = [new UrlSearcher(server), new NodeSearcher(server), new RelationshipSearcher(server)];
      }
      Search.prototype.exec = function(statement) {
        var searcher;
        searcher = this.pickSearcher(statement);
        if (searcher != null) {
          return searcher.exec(statement);
        } else {
          return neo4j.Promise.fulfilled(null);
        }
      };
      Search.prototype.pickSearcher = function(statement) {
        var searcher, _i, _len, _ref;
        _ref = this.searchers;
        for (_i = 0, _len = _ref.length; _i < _len; _i++) {
          searcher = _ref[_i];
          if (searcher.match(statement)) {
            return searcher;
          }
        }
      };
      return Search;
    })();
  });
}).call(this);
