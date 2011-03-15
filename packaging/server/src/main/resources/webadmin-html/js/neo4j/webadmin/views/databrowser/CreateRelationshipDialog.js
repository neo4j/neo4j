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
  define(['neo4j/webadmin/templates/databrowser/createRelationship', 'lib/backbone'], function(template) {
    var CreateRelationshipDialog;
    return CreateRelationshipDialog = (function() {
      function CreateRelationshipDialog() {
        this.render = __bind(this.render, this);;
        this.position = __bind(this.position, this);;
        this.initialize = __bind(this.initialize, this);;        CreateRelationshipDialog.__super__.constructor.apply(this, arguments);
      }
      __extends(CreateRelationshipDialog, Backbone.View);
      CreateRelationshipDialog.prototype.className = "create-relationship-dialog";
      CreateRelationshipDialog.prototype.initialize = function(baseElement, from, type, types, to) {
        this.baseElement = baseElement;
        this.from = from != null ? from : "";
        this.type = type != null ? type : "RELATED_TO";
        this.types = types != null ? types : [];
        this.to = to != null ? to : "";
        $("body").append(this.el);
        this.render();
        return this.position();
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
