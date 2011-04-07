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
  define(['neo4j/webadmin/models/PropertyContainer', 'neo4j/webadmin/views/View', 'neo4j/webadmin/templates/databrowser/propertyEditor', 'lib/backbone'], function(PropertyContainer, View, propertyEditorTemplate) {
    var PropertyContainerView;
    return PropertyContainerView = (function() {
      function PropertyContainerView() {
        this.shouldBeConvertedToString = __bind(this.shouldBeConvertedToString, this);;
        this.unbind = __bind(this.unbind, this);;
        this.remove = __bind(this.remove, this);;
        this.renderProperties = __bind(this.renderProperties, this);;
        this.render = __bind(this.render, this);;
        this.setDataModel = __bind(this.setDataModel, this);;
        this.getPropertyIdForElement = __bind(this.getPropertyIdForElement, this);;
        this.setSaveState = __bind(this.setSaveState, this);;
        this.updateSaveState = __bind(this.updateSaveState, this);;
        this.updateErrorMessages = __bind(this.updateErrorMessages, this);;
        this.deleteItem = __bind(this.deleteItem, this);;
        this.saveChanges = __bind(this.saveChanges, this);;
        this.addProperty = __bind(this.addProperty, this);;
        this.deleteProperty = __bind(this.deleteProperty, this);;
        this.valueChangeDone = __bind(this.valueChangeDone, this);;
        this.keyChangeDone = __bind(this.keyChangeDone, this);;
        this.valueChanged = __bind(this.valueChanged, this);;
        this.keyChanged = __bind(this.keyChanged, this);;
        this.initialize = __bind(this.initialize, this);;        PropertyContainerView.__super__.constructor.apply(this, arguments);
      }
      __extends(PropertyContainerView, View);
      PropertyContainerView.prototype.events = {
        "keyup input.property-key": "keyChanged",
        "keyup input.property-value": "valueChanged",
        "change input.property-key": "keyChangeDone",
        "change input.property-value": "valueChangeDone",
        "click button.delete-property": "deleteProperty",
        "click button.add-property": "addProperty",
        "click button.data-save-properties": "saveChanges",
        "click button.data-delete-item": "deleteItem"
      };
      PropertyContainerView.prototype.initialize = function(opts) {
        return this.template = opts.template;
      };
      PropertyContainerView.prototype.keyChanged = function(ev) {
        var id;
        id = this.getPropertyIdForElement(ev.target);
        if ($(ev.target).val() !== this.propertyContainer.getProperty(id).getKey()) {
          return this.propertyContainer.setNotSaved();
        }
      };
      PropertyContainerView.prototype.valueChanged = function(ev) {
        var id, prop;
        id = this.getPropertyIdForElement(ev.target);
        if ($(ev.target).val() !== this.propertyContainer.getProperty(id).getValueAsJSON()) {
          return prop = this.propertyContainer.setNotSaved();
        }
      };
      PropertyContainerView.prototype.keyChangeDone = function(ev) {
        var id;
        id = this.getPropertyIdForElement(ev.target);
        this.propertyContainer.setKey(id, $(ev.target).val());
        this.saveChanges();
        return this.updateErrorMessages();
      };
      PropertyContainerView.prototype.valueChangeDone = function(ev) {
        var el, id;
        id = this.getPropertyIdForElement(ev.target);
        el = $(ev.target);
        if (this.shouldBeConvertedToString(el.val())) {
          el.val('"' + el.val() + '"');
        }
        this.propertyContainer.setValue(id, el.val());
        this.saveChanges();
        return this.updateErrorMessages();
      };
      PropertyContainerView.prototype.deleteProperty = function(ev) {
        var id;
        id = this.getPropertyIdForElement(ev.target);
        this.propertyContainer.deleteProperty(id);
        return this.propertyContainer.save();
      };
      PropertyContainerView.prototype.addProperty = function(ev) {
        return this.propertyContainer.addProperty();
      };
      PropertyContainerView.prototype.saveChanges = function(ev) {
        return this.propertyContainer.save();
      };
      PropertyContainerView.prototype.deleteItem = function(ev) {
        if (confirm("Are you sure?")) {
          return this.propertyContainer.getItem().remove().then(function() {
            return window.location = "#/data/search/0";
          });
        }
      };
      PropertyContainerView.prototype.updateErrorMessages = function() {
        var id, keyError, prop, row, valueError, _i, _len, _ref, _results;
        _ref = $("ul.property-row", this.el);
        _results = [];
        for (_i = 0, _len = _ref.length; _i < _len; _i++) {
          row = _ref[_i];
          id = $(row).find("input.property-id").val();
          prop = this.propertyContainer.getProperty(id);
          _results.push(prop ? (keyError = prop.hasKeyError() ? prop.getKeyError() : "", valueError = prop.hasValueError() ? prop.getValueError() : "", $(row).find(".property-key-wrap .error").html(keyError), $(row).find(".property-value-wrap .error").html(valueError)) : void 0);
        }
        return _results;
      };
      PropertyContainerView.prototype.updateSaveState = function(ev) {
        var state;
        state = this.propertyContainer.getSaveState();
        switch (state) {
          case "saved":
            return this.setSaveState("Saved", true);
          case "notSaved":
            return this.setSaveState("Not saved", false);
          case "saving":
            return this.setSaveState("Saving..", true);
          case "cantSave":
            return this.setSaveState("Can't save", true);
        }
      };
      PropertyContainerView.prototype.setSaveState = function(text, disabled) {
        var button;
        button = $("button.data-save-properties", this.el);
        button.html(text);
        if (disabled) {
          return button.attr("disabled", "disabled");
        } else {
          return button.removeAttr("disabled");
        }
      };
      PropertyContainerView.prototype.getPropertyIdForElement = function(element) {
        return $(element).closest("ul").find("input.property-id").val();
      };
      PropertyContainerView.prototype.setDataModel = function(dataModel) {
        this.unbind();
        this.propertyContainer = dataModel.getData();
        this.propertyContainer.bind("remove:property", this.renderProperties);
        this.propertyContainer.bind("add:property", this.renderProperties);
        return this.propertyContainer.bind("change:status", this.updateSaveState);
      };
      PropertyContainerView.prototype.render = function() {
        $(this.el).html(this.template({
          item: this.propertyContainer
        }));
        this.renderProperties();
        return this;
      };
      PropertyContainerView.prototype.renderProperties = function() {
        $(".properties", this.el).html(propertyEditorTemplate({
          properties: this.propertyContainer.get("propertyList")
        }));
        return this;
      };
      PropertyContainerView.prototype.remove = function() {
        this.unbind();
        return PropertyContainerView.__super__.remove.call(this);
      };
      PropertyContainerView.prototype.unbind = function() {
        if (this.propertyContainer != null) {
          this.propertyContainer.unbind("remove:property", this.renderProperties);
          this.propertyContainer.unbind("add:property", this.renderProperties);
          return this.propertyContainer.unbind("change:status", this.updateSaveState);
        }
      };
      PropertyContainerView.prototype.shouldBeConvertedToString = function(val) {
        try {
          JSON.parse(val);
          return false;
        } catch (e) {
          return /^[a-z0-9-_\/\\\(\)#%\&!$]+$/i.test(val);
        }
      };
      return PropertyContainerView;
    })();
  });
}).call(this);
