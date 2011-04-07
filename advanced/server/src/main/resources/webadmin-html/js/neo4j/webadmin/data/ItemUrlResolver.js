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
  define(["lib/backbone"], function() {
    var ItemUrlResolver;
    return ItemUrlResolver = (function() {
      function ItemUrlResolver(server) {
        this.extractLastUrlSegment = __bind(this.extractLastUrlSegment, this);;
        this.extractRelationshipId = __bind(this.extractRelationshipId, this);;
        this.extractNodeId = __bind(this.extractNodeId, this);;
        this.getRelationshipUrl = __bind(this.getRelationshipUrl, this);;
        this.getNodeUrl = __bind(this.getNodeUrl, this);;        this.server = server;
      }
      ItemUrlResolver.prototype.getNodeUrl = function(id) {
        return this.server.url + "/db/data/node/" + id;
      };
      ItemUrlResolver.prototype.getRelationshipUrl = function(id) {
        return this.server.url + "/db/data/relationship/" + id;
      };
      ItemUrlResolver.prototype.extractNodeId = function(url) {
        return this.extractLastUrlSegment(url);
      };
      ItemUrlResolver.prototype.extractRelationshipId = function(url) {
        return this.extractLastUrlSegment(url);
      };
      ItemUrlResolver.prototype.extractLastUrlSegment = function(url) {
        if (url.substr(-1) === "/") {
          url = url.substr(0, url.length - 1);
        }
        return url.substr(url.lastIndexOf("/") + 1);
      };
      return ItemUrlResolver;
    })();
  });
}).call(this);
