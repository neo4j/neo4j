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
        this.saveFailed = __bind(this.saveFailed, this);;
        this.saveSucceeded = __bind(this.saveSucceeded, this);;
        this.save = __bind(this.save, this);;
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
          this.addProperty(key, value, false, false);
        }
        this.set({
          saveState: "saved"
        }, {
          silent: true
        });
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
        if (duplicate) {
          property.isDuplicate = true;
        } else {
          property.isDuplicate = false;
          this.getItem().removeProperty(oldKey);
          this.getItem().setProperty(key, property.value);
          this.save();
        }
        return this.updatePropertyList();
      };
      PropertyContainer.prototype.setValue = function(id, value) {
        return console.log(id, value);
      };
      PropertyContainer.prototype.deleteProperty = function(id) {
        return console.log(id);
      };
      PropertyContainer.prototype.addProperty = function(key, value, isDuplicate, updatePropertyList) {
        var id;
        if (key == null) {
          key = "";
        }
        if (value == null) {
          value = "";
        }
        if (isDuplicate == null) {
          isDuplicate = false;
        }
        if (updatePropertyList == null) {
          updatePropertyList = true;
        }
        id = this.generatePropertyId();
        this.properties[id] = {
          key: key,
          value: value,
          id: id,
          isDuplicate: isDuplicate
        };
        if (updatePropertyList) {
          return this.updatePropertyList();
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
      PropertyContainer.prototype.updatePropertyList = function() {
        this.set({
          propertyList: this.getPropertyList()
        }, {
          silent: true
        });
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
      PropertyContainer.prototype.save = function() {
        this.set({
          saveState: "saving"
        });
        return this.getItem().save().then(this.saveSucceeded, this.saveFailed);
      };
      PropertyContainer.prototype.saveSucceeded = function() {
        return this.set({
          saveState: "saved"
        });
      };
      PropertyContainer.prototype.saveFailed = function(ev) {
        return this.set({
          saveState: "saveFailed"
        });
      };
      PropertyContainer.prototype.generatePropertyId = function() {
        return ID_COUNTER++;
      };
      return PropertyContainer;
    })();
  });
}).call(this);
