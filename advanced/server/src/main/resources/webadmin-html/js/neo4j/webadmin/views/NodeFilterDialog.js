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
  define(['neo4j/webadmin/data/ItemUrlResolver', 'neo4j/webadmin/views/Dialog', 'neo4j/webadmin/views/FilterList', 'neo4j/webadmin/templates/nodeFilterDialog', 'lib/backbone'], function(ItemUrlResolver, Dialog, FilterList, template) {
    var NodeFilterDialog;
    return NodeFilterDialog = (function() {
      function NodeFilterDialog() {
        this.complete = __bind(this.complete, this);;
        this.wrapperClicked = __bind(this.wrapperClicked, this);;        NodeFilterDialog.__super__.constructor.apply(this, arguments);
      }
      __extends(NodeFilterDialog, Dialog);
      NodeFilterDialog.prototype.events = {
        'click .complete': 'complete',
        'click .selectAll': 'selectAll',
        'click .cancel': 'cancel'
      };
      NodeFilterDialog.prototype.initialize = function(opts) {
        var filterableItems, id, label, labelProp, labelProperties, node;
        this.completeCallback = opts.completeCallback;
        this.urlResolver = new ItemUrlResolver();
        labelProperties = opts.labelProperties || [];
        this.nodes = opts.nodes;
        filterableItems = (function() {
          var _i, _j, _len, _len2, _ref, _results;
          _ref = this.nodes;
          _results = [];
          for (_i = 0, _len = _ref.length; _i < _len; _i++) {
            node = _ref[_i];
            id = this.urlResolver.extractNodeId(node.getSelf());
            label = id;
            for (_j = 0, _len2 = labelProperties.length; _j < _len2; _j++) {
              labelProp = labelProperties[_j];
              if (node.hasProperty(labelProp)) {
                label = ("" + id + ": ") + JSON.stringify(node.getProperty(labelProp));
              }
            }
            _results.push({
              node: node,
              key: node.getSelf(),
              label: label
            });
          }
          return _results;
        }).call(this);
        this.filterList = new FilterList(filterableItems);
        return NodeFilterDialog.__super__.initialize.call(this);
      };
      NodeFilterDialog.prototype.render = function() {
        var wrapHeight;
        $(this.el).html(template());
        this.filterList.attach($(".filter", this.el));
        this.filterList.render();
        wrapHeight = $(this.el).height();
        return this.filterList.height(wrapHeight - 80);
      };
      NodeFilterDialog.prototype.wrapperClicked = function(ev) {
        if (ev.originalTarget === ev.currentTarget) {
          return this.cancel();
        }
      };
      NodeFilterDialog.prototype.complete = function() {
        var item, nodes;
        nodes = (function() {
          var _i, _len, _ref, _results;
          _ref = this.filterList.getFilteredItems();
          _results = [];
          for (_i = 0, _len = _ref.length; _i < _len; _i++) {
            item = _ref[_i];
            _results.push(item.node);
          }
          return _results;
        }).call(this);
        return this.completeCallback(nodes, this);
      };
      NodeFilterDialog.prototype.selectAll = function() {
        return this.completeCallback(this.nodes, this);
      };
      NodeFilterDialog.prototype.cancel = function() {
        return this.completeCallback([], this);
      };
      return NodeFilterDialog;
    })();
  });
}).call(this);
