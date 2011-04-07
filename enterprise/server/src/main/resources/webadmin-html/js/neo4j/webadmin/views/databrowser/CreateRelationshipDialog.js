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
  define(['neo4j/webadmin/data/ItemUrlResolver', 'neo4j/webadmin/templates/databrowser/createRelationship', 'neo4j/webadmin/views/View', 'neo4j/webadmin/ui/FormHelper', 'lib/backbone'], function(ItemUrlResolver, template, View, FormHelper) {
    var CreateRelationshipDialog;
    return CreateRelationshipDialog = (function() {
      function CreateRelationshipDialog() {
        this.render = __bind(this.render, this);;
        this.position = __bind(this.position, this);;
        this.saveFailed = __bind(this.saveFailed, this);;
        this.saveSuccessful = __bind(this.saveSuccessful, this);;
        this.save = __bind(this.save, this);;
        this.pickedFromAvailableTypes = __bind(this.pickedFromAvailableTypes, this);;
        this.initialize = __bind(this.initialize, this);;        CreateRelationshipDialog.__super__.constructor.apply(this, arguments);
      }
      __extends(CreateRelationshipDialog, View);
      CreateRelationshipDialog.prototype.className = "popout";
      CreateRelationshipDialog.prototype.events = {
        "click #create-relationship": "save",
        "change #create-relationship-types": "pickedFromAvailableTypes"
      };
      CreateRelationshipDialog.prototype.initialize = function(opts) {
        $(this.el).hide();
        $("body").append(this.el);
        this.formHelper = new FormHelper(this.el);
        this.baseElement = opts.baseElement;
        this.server = opts.server;
        this.dataModel = opts.dataModel;
        this.closeCallback = opts.closeCallback;
        this.urlResolver = new ItemUrlResolver(this.server);
        this.type = "RELATED_TO";
        if (this.dataModel.dataIsSingleNode()) {
          this.from = this.dataModel.getData().getId();
        } else {
          this.from = "";
        }
        this.to = "";
        return this.server.getAvailableRelationshipTypes().then(__bind(function(types) {
          this.types = types;
          this.position();
          this.render();
          return $(this.el).show();
        }, this));
      };
      CreateRelationshipDialog.prototype.pickedFromAvailableTypes = function() {
        var type;
        type = $("#create-relationship-types").val();
        if (type !== "Types in use") {
          $("#create-relationship-type").val(type);
        }
        return $("#create-relationship-types").val("Types in use");
      };
      CreateRelationshipDialog.prototype.save = function() {
        var type;
        this.formHelper.removeAllErrors();
        type = $("#create-relationship-type").val();
        return this.server.rel(this.getFromUrl(), type, this.getToUrl()).then(this.saveSuccessful, this.saveFailed);
      };
      CreateRelationshipDialog.prototype.saveSuccessful = function(relationship) {
        var id;
        id = this.urlResolver.extractRelationshipId(relationship.getSelf());
        this.dataModel.setData(relationship, true, {
          silent: true
        });
        this.dataModel.setQuery("rel:" + id, true);
        return this.closeCallback();
      };
      CreateRelationshipDialog.prototype.saveFailed = function(error) {
        if (error instanceof neo4j.exceptions.NotFoundException) {
          if (error.url === this.getFromUrl()) {
            return this.formHelper.addErrorTo("#create-relationship-from", "This node cannot be found.");
          } else {
            return this.formHelper.addErrorTo("#create-relationship-to", "This node cannot be found.");
          }
        } else {
          return this.formHelper.addErrorTo("#create-relationship-from", "Unable to create relationship.");
        }
      };
      CreateRelationshipDialog.prototype.getFromUrl = function() {
        return this.urlResolver.getNodeUrl(this.urlResolver.extractNodeId($("#create-relationship-from").val()));
      };
      CreateRelationshipDialog.prototype.getToUrl = function() {
        return this.urlResolver.getNodeUrl(this.urlResolver.extractNodeId($("#create-relationship-to").val()));
      };
      CreateRelationshipDialog.prototype.position = function() {
        var basePos, left, top;
        basePos = $(this.baseElement).offset();
        top = basePos.top + $(this.baseElement).outerHeight();
        left = basePos.left - ($(this.el).outerWidth() - $(this.baseElement).outerWidth());
        return $(this.el).css({
          position: "absolute",
          top: top + "px",
          left: left + "px"
        });
      };
      CreateRelationshipDialog.prototype.render = function() {
        $(this.el).html(template({
          from: this.from,
          to: this.to,
          type: this.type,
          types: this.types
        }));
        return this;
      };
      return CreateRelationshipDialog;
    })();
  });
}).call(this);
