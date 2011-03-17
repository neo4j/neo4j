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
  define(['neo4j/webadmin/data/ItemUrlResolver', './RelationshipProxy', 'lib/backbone'], function(ItemUrlResolver, RelationshipProxy) {
    var RelationshipList;
    return RelationshipList = (function() {
      function RelationshipList() {
        this.getRawRelationships = __bind(this.getRawRelationships, this);;
        this.getRelationships = __bind(this.getRelationships, this);;
        this.getPropertyKeys = __bind(this.getPropertyKeys, this);;
        this.setRawRelationships = __bind(this.setRawRelationships, this);;
        this.initialize = __bind(this.initialize, this);;        RelationshipList.__super__.constructor.apply(this, arguments);
      }
      __extends(RelationshipList, Backbone.Model);
      RelationshipList.prototype.initialize = function(relationships) {
        return this.setRawRelationships(relationships || []);
      };
      RelationshipList.prototype.setRawRelationships = function(relationships) {
        var key, propertyKeyMap, propertyKeys, rel, rels, value, _i, _len, _ref;
        this.set({
          "rawRelationships": relationships
        });
        rels = [];
        propertyKeyMap = {};
        for (_i = 0, _len = relationships.length; _i < _len; _i++) {
          rel = relationships[_i];
          _ref = rel.getProperties();
          for (key in _ref) {
            value = _ref[key];
            propertyKeyMap[key] = true;
          }
          rels.push(new RelationshipProxy(rel));
        }
        propertyKeys = (function() {
          var _results;
          _results = [];
          for (key in propertyKeyMap) {
            value = propertyKeyMap[key];
            _results.push(key);
          }
          return _results;
        })();
        this.set({
          "propertyKeys": propertyKeys
        });
        return this.set({
          "relationships": rels
        });
      };
      RelationshipList.prototype.getPropertyKeys = function() {
        return this.get("propertyKeys");
      };
      RelationshipList.prototype.getRelationships = function() {
        return this.get("relationships");
      };
      RelationshipList.prototype.getRawRelationships = function() {
        return this.get("rawRelationships");
      };
      return RelationshipList;
    })();
  });
}).call(this);
