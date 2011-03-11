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
  define(['neo4j/webadmin/models/PropertyContainer', 'neo4j/webadmin/templates/databrowser/propertyEditor', 'lib/backbone'], function(PropertyContainer, propertyEditorTemplate) {
    var PropertyContainerView;
    return PropertyContainerView = (function() {
      function PropertyContainerView() {
        this.getPropertyField = __bind(this.getPropertyField, this);;
        this.shouldBeConvertedToString = __bind(this.shouldBeConvertedToString, this);;
        this.renderProperties = __bind(this.renderProperties, this);;
        this.render = __bind(this.render, this);;
        this.setDataModel = __bind(this.setDataModel, this);;
        this.getPropertyIdForElement = __bind(this.getPropertyIdForElement, this);;
        this.setSaveState = __bind(this.setSaveState, this);;
        this.updateSaveState = __bind(this.updateSaveState, this);;
        this.focusedOnValueField = __bind(this.focusedOnValueField, this);;
        this.focusedOnKeyField = __bind(this.focusedOnKeyField, this);;
        this.saveChanges = __bind(this.saveChanges, this);;
        this.addProperty = __bind(this.addProperty, this);;
        this.deleteProperty = __bind(this.deleteProperty, this);;
        this.valueChangeDone = __bind(this.valueChangeDone, this);;
        this.keyChangeDone = __bind(this.keyChangeDone, this);;
        this.valueChanged = __bind(this.valueChanged, this);;
        this.keyChanged = __bind(this.keyChanged, this);;
        this.initialize = __bind(this.initialize, this);;        PropertyContainerView.__super__.constructor.apply(this, arguments);
      }
      __extends(PropertyContainerView, Backbone.View);
      PropertyContainerView.prototype.events = {
        "focus input.property-key": "focusedOnKeyField",
        "focus input.property-value": "focusedOnValueField",
        "keyup input.property-key": "keyChanged",
        "keyup input.property-value": "valueChanged",
        "change input.property-key": "keyChangeDone",
        "change input.property-value": "valueChangeDone",
        "click button.delete-property": "deleteProperty",
        "click button.add-property": "addProperty",
        "click button.data-save-properties": "saveChanges"
      };
      PropertyContainerView.prototype.initialize = function(opts) {
        this.template = opts.template;
        this.propertyContainer = new PropertyContainer();
        this.propertyContainer.bind("change:propertyList", this.renderProperties);
        return this.propertyContainer.bind("change:saveState", this.updateSaveState);
      };
      PropertyContainerView.prototype.keyChanged = function(ev) {
        var id;
        id = this.getPropertyIdForElement(ev.target);
        return this.propertyContainer.setKey(id, $(ev.target).val());
      };
      PropertyContainerView.prototype.valueChanged = function(ev) {
        var id;
        id = this.getPropertyIdForElement(ev.target);
        return this.propertyContainer.setValue(id, $(ev.target).val());
      };
      PropertyContainerView.prototype.keyChangeDone = function(ev) {
        var id;
        id = this.getPropertyIdForElement(ev.target);
        this.propertyContainer.setKey(id, $(ev.target).val());
        return this.propertyContainer.save();
      };
      PropertyContainerView.prototype.valueChangeDone = function(ev) {
        var el, id;
        id = this.getPropertyIdForElement(ev.target);
        el = $(ev.target);
        if (this.shouldBeConvertedToString(el.val())) {
          el.val("'" + (el.val()) + "'");
        }
        this.propertyContainer.setValue(id, el.val());
        return this.propertyContainer.save();
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
      PropertyContainerView.prototype.focusedOnKeyField = function(ev) {
        var id;
        id = this.getPropertyIdForElement(ev.target);
        return this.focusedField = {
          id: id,
          type: "key"
        };
      };
      PropertyContainerView.prototype.focusedOnValueField = function(ev) {
        var id;
        id = this.getPropertyIdForElement(ev.target);
        return this.focusedField = {
          id: id,
          type: "value"
        };
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
        this.dataModel = dataModel;
        return this.propertyContainer.setDataModel(this.dataModel);
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
        if (this.focusedField != null) {
          this.getPropertyField(this.focusedField.id, this.focusedField.type);
        }
        return this;
      };
      PropertyContainerView.prototype.shouldBeConvertedToString = function(val) {
        try {
          JSON.parse(val);
          return false;
        } catch (e) {
          return /^[a-z0-9-_\/\\\(\)#%\&!$]+$/i.test(val);
        }
      };
      PropertyContainerView.prototype.getPropertyField = function(id, type) {};
      return PropertyContainerView;
    })();
  });
}).call(this);
