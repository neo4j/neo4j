/*
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
(function() {
  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; };
  define(["lib/backbone"], function() {
    var NodeSearcher;
    return NodeSearcher = (function() {
      function NodeSearcher() {
        this.exec = __bind(this.exec, this);;
        this.match = __bind(this.match, this);;        this.pattern = /^(node:)?([0-9]+)$/i;
      }
      NodeSearcher.prototype.match = function(statement) {
        return this.pattern.test(statement);
      };
      NodeSearcher.prototype.exec = function(statement) {
        return location.hash = "#/data/node/" + this.getNodeId(statement);
      };
      NodeSearcher.prototype.getNodeId = function(statement) {
        var match;
        match = this.pattern.exec(statement);
        return match[2];
      };
      return NodeSearcher;
    })();
  });
}).call(this);
