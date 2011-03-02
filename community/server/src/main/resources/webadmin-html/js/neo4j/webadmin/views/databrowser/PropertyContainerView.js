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
  define(['neo4j/webadmin/models/PropertyContainer', 'neo4j/webadmin/templates/databrowser/propertyEditor', 'lib/backbone'], function(PropertyContainer, propertyEditorTemplate) {
    var PropertyContainerView;
    return PropertyContainerView = (function() {
      function PropertyContainerView() {
        this.getPropertyField = __bind(this.getPropertyField, this);;
        this.renderProperties = __bind(this.renderProperties, this);;
        this.render = __bind(this.render, this);;
        this.setDataModel = __bind(this.setDataModel, this);;
        this.getPropertyIdForElement = __bind(this.getPropertyIdForElement, this);;
        this.focusedOnValueField = __bind(this.focusedOnValueField, this);;
        this.focusedOnKeyField = __bind(this.focusedOnKeyField, this);;
        this.addProperty = __bind(this.addProperty, this);;
        this.deleteProperty = __bind(this.deleteProperty, this);;
        this.valueChanged = __bind(this.valueChanged, this);;
        this.keyChanged = __bind(this.keyChanged, this);;
        this.initialize = __bind(this.initialize, this);;        PropertyContainerView.__super__.constructor.apply(this, arguments);
      }
      __extends(PropertyContainerView, Backbone.View);
      PropertyContainerView.prototype.events = {
        "focus input.property-key": "focusedOnKeyField",
        "focus input.property-value": "focusedOnValueField",
        "change input.property-key": "keyChanged",
        "change input.property-value": "valueChanged",
        "click  button.delete-property": "deleteProperty",
        "click  button.add-property": "addProperty"
      };
      PropertyContainerView.prototype.initialize = function(opts) {
        this.template = opts.template;
        this.propertyContainer = new PropertyContainer();
        return this.propertyContainer.bind("change", this.renderProperties);
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
      PropertyContainerView.prototype.deleteProperty = function(ev) {
        var id;
        id = this.getPropertyIdForElement(ev.target);
        return this.propertyContainer.deleteProperty(id);
      };
      PropertyContainerView.prototype.addProperty = function(ev) {
        return this.propertyContainer.addProperty();
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
      PropertyContainerView.prototype.getPropertyIdForElement = function(element) {
        return $(element).closest("li").find("input.property-id").val();
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
      PropertyContainerView.prototype.getPropertyField = function(id, type) {};
      return PropertyContainerView;
    })();
  });
}).call(this);
