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
        this.validate = __bind(this.validate, this);;
        this.setSaveState = __bind(this.setSaveState, this);;
        this.getSaveState = __bind(this.getSaveState, this);;
        this.isNotSaved = __bind(this.isNotSaved, this);;
        this.isCantSave = __bind(this.isCantSave, this);;
        this.isSaved = __bind(this.isSaved, this);;
        this.setNotSaved = __bind(this.setNotSaved, this);;
        this.setCantSave = __bind(this.setCantSave, this);;
        this.setSaved = __bind(this.setSaved, this);;
        this.saveFailed = __bind(this.saveFailed, this);;
        this.save = __bind(this.save, this);;
        this.hasDuplicates = __bind(this.hasDuplicates, this);;
        this.getPropertyList = __bind(this.getPropertyList, this);;
        this.updatePropertyList = __bind(this.updatePropertyList, this);;
        this.hasKey = __bind(this.hasKey, this);;
        this.getProperty = __bind(this.getProperty, this);;
        this.addProperty = __bind(this.addProperty, this);;
        this.deleteProperty = __bind(this.deleteProperty, this);;
        this.setValue = __bind(this.setValue, this);;
        this.setKey = __bind(this.setKey, this);;
        this.getSelf = __bind(this.getSelf, this);;
        this.getItem = __bind(this.getItem, this);;
        this.setDataModel = __bind(this.setDataModel, this);;
        this.initialize = __bind(this.initialize, this);;        PropertyContainer.__super__.constructor.apply(this, arguments);
      }
      __extends(PropertyContainer, Backbone.Model);
      PropertyContainer.prototype.defaults = {
        saveState: "saved"
      };
      PropertyContainer.prototype.initialize = function(opts) {
        return this.properties = {};
      };
      PropertyContainer.prototype.setDataModel = function(dataModel) {
        var key, value, _ref;
        this.dataModel = dataModel;
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
        return this.dataModel.get("data");
      };
      PropertyContainer.prototype.getSelf = function() {
        return this.dataModel.get("data").getSelf();
      };
      PropertyContainer.prototype.setKey = function(id, key) {
        var duplicate, oldKey, property;
        duplicate = this.hasKey(key, id);
        property = this.getProperty(id);
        oldKey = property.key;
        property.set({
          "key": key
        });
        if (!this.isCantSave()) {
          this.setNotSaved();
        }
        if (duplicate) {
          property.set({
            "isDuplicate": true
          });
          this.setCantSave();
        } else {
          property.set({
            "isDuplicate": false
          });
          if (!this.hasDuplicates()) {
            this.setNotSaved();
          }
          this.getItem().removeProperty(oldKey);
          this.getItem().setProperty(key, property.getValue());
        }
        return this.updatePropertyList();
      };
      PropertyContainer.prototype.setValue = function(id, value) {
        var cleanedValue, property;
        property = this.getProperty(id);
        cleanedValue = this.cleanPropertyValue(value);
        if (!this.isCantSave()) {
          this.setNotSaved();
        }
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
            "value": null
          });
          property.set({
            "valueError": cleanedValue.error
          });
        }
        return this.updatePropertyList();
      };
      PropertyContainer.prototype.deleteProperty = function(id, opts) {
        var property;
        if (opts == null) {
          opts = {};
        }
        if (!this.isCantSave()) {
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
        if (opts == null) {
          opts = {
            silent: true
          };
        }
        return this.set({
          propertyList: this.getPropertyList()
        }, opts);
      };
      PropertyContainer.prototype.getPropertyList = function() {
        var arrayed, key, property, _ref;
        arrayed = [];
        _ref = this.properties;
        for (key in _ref) {
          property = _ref[key];
          arrayed.push(property);
        }
        return arrayed;
      };
      PropertyContainer.prototype.hasDuplicates = function() {
        var key, property, _ref;
        _ref = this.properties;
        for (key in _ref) {
          property = _ref[key];
          if (property.isDuplicate()) {
            return true;
          }
        }
        return false;
      };
      PropertyContainer.prototype.save = function() {
        this.setSaveState("saving");
        return this.getItem().save().then(this.setSaved, this.saveFailed);
      };
      PropertyContainer.prototype.saveFailed = function(ev) {
        return this.setNotSaved();
      };
      PropertyContainer.prototype.setSaved = function() {
        return this.setSaveState("saved");
      };
      PropertyContainer.prototype.setCantSave = function() {
        return this.setSaveState("cantSave");
      };
      PropertyContainer.prototype.setNotSaved = function() {
        return this.setSaveState("notSaved");
      };
      PropertyContainer.prototype.isSaved = function() {
        return this.getSaveState() === "saved";
      };
      PropertyContainer.prototype.isCantSave = function() {
        return this.getSaveState() === "cantSave";
      };
      PropertyContainer.prototype.isNotSaved = function() {
        return this.getSaveState() === "notSaved" || this.isCantSave();
      };
      PropertyContainer.prototype.getSaveState = function() {
        return this.get("saveState");
      };
      PropertyContainer.prototype.setSaveState = function(state, opts) {
        if (opts == null) {
          opts = {};
        }
        return this.set({
          saveState: state
        }, opts);
      };
      PropertyContainer.prototype.validate = function() {
        var property, _i, _len, _ref;
        _ref = this.properties;
        for (_i = 0, _len = _ref.length; _i < _len; _i++) {
          property = _ref[_i];
          if (property.hasKeyError() || property.hasValueError()) {
            return false;
          }
        }
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
