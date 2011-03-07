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
  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; }, __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  };
  define(['lib/backbone'], function() {
    var ID_COUNTER, PropertyContainer;
    ID_COUNTER = 0;
    return PropertyContainer = (function() {
      function PropertyContainer() {
        this.generatePropertyId = __bind(this.generatePropertyId, this);;
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
          this.addProperty(key, value, false, {
            saved: true
          }, {
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
        property.key = key;
        if (!this.isCantSave()) {
          this.setNotSaved();
        }
        property.saved = false;
        if (duplicate) {
          property.isDuplicate = true;
          this.setCantSave();
        } else {
          property.isDuplicate = false;
          if (!this.hasDuplicates()) {
            this.setNotSaved();
          }
          this.getItem().removeProperty(oldKey);
          this.getItem().setProperty(key, property.value);
        }
        return this.updatePropertyList();
      };
      PropertyContainer.prototype.setValue = function(id, value) {
        if (!this.isCantSave()) {
          return this.setNotSaved();
        }
      };
      PropertyContainer.prototype.deleteProperty = function(id) {
        if (!isCantSave()) {
          return this.setNotSaved();
        }
      };
      PropertyContainer.prototype.addProperty = function(key, value, updatePropertyList, propertyMeta, opts) {
        var id, isDuplicate, saved;
        if (key == null) {
          key = "";
        }
        if (value == null) {
          value = "";
        }
        if (updatePropertyList == null) {
          updatePropertyList = true;
        }
        if (propertyMeta == null) {
          propertyMeta = {};
        }
        if (opts == null) {
          opts = {};
        }
        id = this.generatePropertyId();
        isDuplicate = propertyMeta.isDuplicate != null ? true : false;
        saved = propertyMeta.saved != null ? true : false;
        this.properties[id] = {
          key: key,
          value: value,
          id: id,
          isDuplicate: isDuplicate,
          saved: saved
        };
        if (updatePropertyList) {
          return this.updatePropertyList(opts);
        }
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
          if (property.key === search && id !== ignoreId) {
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
        this.set({
          propertyList: this.getPropertyList()
        }, opts);
        return this.change();
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
          if (property.isDuplicate) {
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
        return this.getSaveState() === "notSaved" || isCantSave();
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
      PropertyContainer.prototype.generatePropertyId = function() {
        return ID_COUNTER++;
      };
      return PropertyContainer;
    })();
  });
}).call(this);
