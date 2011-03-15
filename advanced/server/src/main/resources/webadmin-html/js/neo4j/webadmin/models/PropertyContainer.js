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
  define(['./Property', 'lib/backbone'], function(Property) {
    var ID_COUNTER, PropertyContainer;
    ID_COUNTER = 0;
    return PropertyContainer = (function() {
      function PropertyContainer() {
        this.generatePropertyId = __bind(this.generatePropertyId, this);;
        this.isValidArrayValue = __bind(this.isValidArrayValue, this);;
        this.isMap = __bind(this.isMap, this);;
        this.cleanPropertyValue = __bind(this.cleanPropertyValue, this);;
        this.noErrors = __bind(this.noErrors, this);;
        this.setSaveState = __bind(this.setSaveState, this);;
        this.getSaveState = __bind(this.getSaveState, this);;
        this.isNotSaved = __bind(this.isNotSaved, this);;
        this.isSaved = __bind(this.isSaved, this);;
        this.setNotSaved = __bind(this.setNotSaved, this);;
        this.setSaved = __bind(this.setSaved, this);;
        this.saveFailed = __bind(this.saveFailed, this);;
        this.save = __bind(this.save, this);;
        this.updatePropertyList = __bind(this.updatePropertyList, this);;
        this.hasKey = __bind(this.hasKey, this);;
        this.getProperty = __bind(this.getProperty, this);;
        this.addProperty = __bind(this.addProperty, this);;
        this.deleteProperty = __bind(this.deleteProperty, this);;
        this.setValue = __bind(this.setValue, this);;
        this.setKey = __bind(this.setKey, this);;
        this.getSelf = __bind(this.getSelf, this);;
        this.getItem = __bind(this.getItem, this);;
        this.initialize = __bind(this.initialize, this);;        PropertyContainer.__super__.constructor.apply(this, arguments);
      }
      __extends(PropertyContainer, Backbone.Model);
      PropertyContainer.prototype.defaults = {
        status: "saved"
      };
      PropertyContainer.prototype.initialize = function(item, opts) {
        var key, value, _ref;
        this.properties = {};
        this.item = item;
        this.properties = {};
        _ref = this.getItem().getProperties();
        for (key in _ref) {
          value = _ref[key];
          this.addProperty(key, value, {
            silent: true
          });
        }
        this.setSaved();
        return this.updatePropertyList();
      };
      PropertyContainer.prototype.getItem = function() {
        return this.item;
      };
      PropertyContainer.prototype.getSelf = function() {
        return this.getItem().getSelf();
      };
      PropertyContainer.prototype.setKey = function(id, key, opts) {
        var duplicate, oldKey, property;
        if (opts == null) {
          opts = {};
        }
        duplicate = this.hasKey(key, id);
        property = this.getProperty(id);
        oldKey = property.key;
        property.set({
          "key": key
        });
        this.setNotSaved();
        if (duplicate) {
          property.setKeyError("This key is already used, please choose a different one.");
        } else {
          property.setKeyError(false);
          this.getItem().removeProperty(oldKey);
          this.getItem().setProperty(key, property.getValue());
        }
        return this.updatePropertyList(opts);
      };
      PropertyContainer.prototype.setValue = function(id, value, opts) {
        var cleanedValue, property;
        if (opts == null) {
          opts = {};
        }
        property = this.getProperty(id);
        cleanedValue = this.cleanPropertyValue(value);
        this.setNotSaved();
        if (cleanedValue.value != null) {
          property.set({
            "valueError": false
          });
          property.set({
            "value": cleanedValue.value
          });
          this.getItem().setProperty(property.getKey(), cleanedValue.value);
        } else {
          property.set({
            "value": value
          });
          property.set({
            "valueError": cleanedValue.error
          });
        }
        return this.updatePropertyList(opts);
      };
      PropertyContainer.prototype.deleteProperty = function(id, opts) {
        var property;
        if (opts == null) {
          opts = {};
        }
        if (this.noErrors({
          ignore: id
        })) {
          this.setNotSaved();
          property = this.getProperty(id);
          delete this.properties[id];
          this.getItem().removeProperty(property.getKey());
          return this.updatePropertyList(opts);
        }
      };
      PropertyContainer.prototype.addProperty = function(key, value, opts) {
        var id;
        if (key == null) {
          key = "";
        }
        if (value == null) {
          value = "";
        }
        if (opts == null) {
          opts = {};
        }
        id = this.generatePropertyId();
        this.properties[id] = new Property({
          key: key,
          value: value,
          localId: id
        });
        return this.updatePropertyList(opts);
      };
      PropertyContainer.prototype.getProperty = function(id) {
        return this.properties[id];
      };
      PropertyContainer.prototype.hasKey = function(search, ignoreId) {
        var id, property, _ref;
        if (ignoreId == null) {
          ignoreId = null;
        }
        _ref = this.properties;
        for (id in _ref) {
          property = _ref[id];
          if (property.getKey() === search && id !== ignoreId) {
            return true;
          }
        }
        return false;
      };
      PropertyContainer.prototype.updatePropertyList = function(opts) {
        var flatProperties, key, property, silent, _ref;
        if (opts == null) {
          opts = {};
        }
        flatProperties = [];
        _ref = this.properties;
        for (key in _ref) {
          property = _ref[key];
          flatProperties.push(property);
        }
        silent = (opts.silent != null) && opts.silent === true;
        opts.silent = true;
        this.set({
          propertyList: flatProperties
        }, opts);
        if (!silent) {
          return this.trigger("change:propertyList");
        }
      };
      PropertyContainer.prototype.save = function() {
        if (this.noErrors()) {
          this.setSaveState("saving");
          return this.getItem().save().then(this.setSaved, this.saveFailed);
        }
      };
      PropertyContainer.prototype.saveFailed = function(ev) {
        return this.setNotSaved();
      };
      PropertyContainer.prototype.setSaved = function() {
        return this.setSaveState("saved");
      };
      PropertyContainer.prototype.setNotSaved = function() {
        return this.setSaveState("notSaved");
      };
      PropertyContainer.prototype.isSaved = function() {
        return this.getSaveState() === "saved";
      };
      PropertyContainer.prototype.isNotSaved = function() {
        return this.getSaveState() === "notSaved";
      };
      PropertyContainer.prototype.getSaveState = function() {
        return this.get("status");
      };
      PropertyContainer.prototype.setSaveState = function(state, opts) {
        if (opts == null) {
          opts = {};
        }
        return this.set({
          status: state
        });
      };
      PropertyContainer.prototype.noErrors = function(opts) {
        var id, property, _ref;
        if (opts == null) {
          opts = {};
        }
        _ref = this.properties;
        for (id in _ref) {
          property = _ref[id];
          if (!(opts.ignore != null) || opts.ignore !== id) {
            if (property.hasKeyError() || property.hasValueError()) {
              return false;
            }
          }
        }
        return true;
      };
      PropertyContainer.prototype.cleanPropertyValue = function(rawVal) {
        var val;
        try {
          val = JSON.parse(rawVal);
          if (val === null) {
            return {
              error: "Null values are not allowed."
            };
          } else if (this.isMap(val)) {
            return {
              error: "Maps are not supported property values."
            };
          } else if (_(val).isArray() && !this.isValidArrayValue(val)) {
            return {
              error: "Only arrays with one type of values, and only primitive types, is allowed."
            };
          } else {
            return {
              value: val
            };
          }
        } catch (e) {
          return {
            error: "This does not appear to be a valid JSON value."
          };
        }
      };
      PropertyContainer.prototype.isMap = function(val) {
        return JSON.stringify(val).indexOf("{") === 0;
      };
      PropertyContainer.prototype.isValidArrayValue = function(val) {
        var firstValue, validType, value, _i, _len;
        if (val.length === 0) {
          return true;
        }
        firstValue = val[0];
        if (_.isString(firstValue)) {
          validType = _.isString;
        } else if (_.isNumber(firstValue)) {
          validType = _.isNumber;
        } else if (_.isBoolean(firstValue)) {
          validType = _.isBoolean;
        } else {
          return false;
        }
        for (_i = 0, _len = val.length; _i < _len; _i++) {
          value = val[_i];
          if (!validType(value)) {
            return false;
          }
        }
        return true;
      };
      PropertyContainer.prototype.generatePropertyId = function() {
        return ID_COUNTER++;
      };
      return PropertyContainer;
    })();
  });
}).call(this);
