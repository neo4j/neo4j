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
  */  var __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  };
  define(['lib/backbone', 'lib/has'], function() {
    var InMemoryStoringStrategy, LocalStorageStoringStrategy, LocallyStoredModel;
    LocalStorageStoringStrategy = (function() {
      function LocalStorageStoringStrategy() {}
      LocalStorageStoringStrategy.prototype.store = function(key, obj) {
        return localStorage.setItem(key, JSON.stringify(obj));
      };
      LocalStorageStoringStrategy.prototype.fetch = function(key, defaults) {
        var stored;
        if (defaults == null) {
          defaults = {};
        }
        stored = localStorage.getItem(key);
        if (stored !== null) {
          return JSON.parse(stored);
        } else {
          return defaults;
        }
      };
      return LocalStorageStoringStrategy;
    })();
    InMemoryStoringStrategy = (function() {
      function InMemoryStoringStrategy() {
        this.storage = {};
      }
      InMemoryStoringStrategy.prototype.store = function(key, obj) {
        return this.storage[key] = obj;
      };
      InMemoryStoringStrategy.prototype.fetch = function(key, defaults) {
        if (defaults == null) {
          defaults = {};
        }
        if (this.storage[key] != null) {
          return this.storage[key];
        } else {
          return this.defaults;
        }
      };
      return InMemoryStoringStrategy;
    })();
    return LocallyStoredModel = (function() {
      function LocallyStoredModel() {
        LocallyStoredModel.__super__.constructor.apply(this, arguments);
      }
      __extends(LocallyStoredModel, Backbone.Model);
      LocallyStoredModel.prototype.initialize = function() {
        if (has("native-localstorage")) {
          return this.storingStrategy = new LocalStorageStoringStrategy();
        } else {
          return this.storingStrategy = new InMemoryStoringStrategy();
        }
      };
      LocallyStoredModel.prototype.getStorageKey = function() {
        return "default-locally-stored-model";
      };
      LocallyStoredModel.prototype.fetch = function() {
        this.clear({
          silent: true
        });
        return this.set(this.storingStrategy.fetch(this.getStorageKey(), this.defaults));
      };
      LocallyStoredModel.prototype.save = function() {
        return this.storingStrategy.store(this.getStorageKey(), this.toJSON());
      };
      return LocallyStoredModel;
    })();
  });
}).call(this);
