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
  define(['neo4j/webadmin/data/ItemUrlResolver', 'neo4j/webadmin/templates/databrowser/visualizationSettings', 'neo4j/webadmin/views/View', 'lib/backbone'], function(ItemUrlResolver, template, View) {
    var VisualizationSettingsDialog;
    return VisualizationSettingsDialog = (function() {
      function VisualizationSettingsDialog() {
        this.render = __bind(this.render, this);;
        this.position = __bind(this.position, this);;
        this.save = __bind(this.save, this);;
        this.initialize = __bind(this.initialize, this);;        VisualizationSettingsDialog.__super__.constructor.apply(this, arguments);
      }
      __extends(VisualizationSettingsDialog, View);
      VisualizationSettingsDialog.prototype.className = "popout";
      VisualizationSettingsDialog.prototype.events = {
        "click #save-visualization-settings": "save"
      };
      VisualizationSettingsDialog.prototype.initialize = function(opts) {
        $("body").append(this.el);
        this.baseElement = opts.baseElement;
        this.closeCallback = opts.closeCallback;
        this.settings = opts.appState.getVisualizationSettings();
        this.position();
        return this.render();
      };
      VisualizationSettingsDialog.prototype.save = function() {
        var keys;
        keys = $("#visualization-label-properties").val().split(",");
        this.settings.setLabelProperties(keys);
        this.settings.save();
        return this.closeCallback();
      };
      VisualizationSettingsDialog.prototype.position = function() {
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
      VisualizationSettingsDialog.prototype.render = function() {
        $(this.el).html(template({
          labels: this.settings.getLabelProperties().join(",")
        }));
        return this;
      };
      return VisualizationSettingsDialog;
    })();
  });
}).call(this);
