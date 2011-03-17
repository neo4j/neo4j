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
  define(['neo4j/webadmin/data/ItemUrlResolver', 'neo4j/webadmin/ui/LoadingSpinner', 'neo4j/webadmin/views/View', 'neo4j/webadmin/ui/Tooltip', 'neo4j/webadmin/security/HtmlEscaper', 'neo4j/webadmin/templates/databrowser/visualization', 'lib/raphael', 'lib/dracula.graffle', 'lib/dracula.graph', 'lib/backbone'], function(ItemUrlResolver, LoadingSpinner, View, Tooltip, HtmlEscaper, template) {
    var GROUP_IDS, VisualizedView;
    GROUP_IDS = 0;
    return VisualizedView = (function() {
      function VisualizedView() {
        this.attach = __bind(this.attach, this);;
        this.detach = __bind(this.detach, this);;
        this.remove = __bind(this.remove, this);;
        this.hideLoader = __bind(this.hideLoader, this);;
        this.showLoader = __bind(this.showLoader, this);;
        this.removeVisualNode = __bind(this.removeVisualNode, this);;
        this.loadRelationships = __bind(this.loadRelationships, this);;
        this.groupClicked = __bind(this.groupClicked, this);;
        this.mouseLeavingNode = __bind(this.mouseLeavingNode, this);;
        this.mouseOverNode = __bind(this.mouseOverNode, this);;
        this.nodeClicked = __bind(this.nodeClicked, this);;
        this.groupRenderer = __bind(this.groupRenderer, this);;
        this.unexploredNodeRenderer = __bind(this.unexploredNodeRenderer, this);;
        this.nodeRenderer = __bind(this.nodeRenderer, this);;
        this.addRelationships = __bind(this.addRelationships, this);;
        this.addGroup = __bind(this.addGroup, this);;
        this.addAndGroupRelationships = __bind(this.addAndGroupRelationships, this);;
        this.addRelationship = __bind(this.addRelationship, this);;
        this.addUnexploredNode = __bind(this.addUnexploredNode, this);;
        this.addNode = __bind(this.addNode, this);;
        this.hasNode = __bind(this.hasNode, this);;
        this.redrawVisualization = __bind(this.redrawVisualization, this);;
        this.render = __bind(this.render, this);;        VisualizedView.__super__.constructor.apply(this, arguments);
      }
      __extends(VisualizedView, View);
      VisualizedView.prototype.initialize = function(options) {
        this.server = options.server;
        this.urlResolver = new ItemUrlResolver(this.server);
        this.htmlEscaper = new HtmlEscaper;
        this.dataModel = options.dataModel;
        this.tooltip = new Tooltip;
        this.nodeMap = {};
        return this.groupMap = {};
      };
      VisualizedView.prototype.render = function() {
        var height, node, width;
        $(this.el).html(template());
        this.nodeMap = {};
        this.groupMap = {};
        width = $(document).width() - 40;
        height = $(document).height() - 120;
        this.g = new Graph();
        switch (this.dataModel.get("type")) {
          case "node":
            node = this.dataModel.getData().getItem();
            this.addNode(node);
            this.loadRelationships(node);
            break;
          case "relationship":
            this.addRelationship(this.dataModel.getData().getItem());
            break;
          case "relationshipList":
            this.addRelationships(this.dataModel.getData().getRawRelationships());
            break;
          default:
            return this;
        }
        this.layout = new Graph.Layout.Spring(this.g);
        this.renderer = new Graph.Renderer.Raphael('visualization', this.g, width, height);
        return this;
      };
      VisualizedView.prototype.redrawVisualization = function() {
        this.layout.layout();
        return this.renderer.draw();
      };
      VisualizedView.prototype.hasNode = function(nodeUrl) {
        if (this.nodeMap[nodeUrl]) {
          return true;
        } else {
          return false;
        }
      };
      VisualizedView.prototype.addNode = function(node) {
        return this.nodeMap[node.getSelf()] = {
          neoNode: node,
          visualNode: this.g.addNode(node.getSelf(), {
            node: node,
            url: node.getSelf(),
            explored: true,
            render: this.nodeRenderer
          })
        };
      };
      VisualizedView.prototype.addUnexploredNode = function(nodeUrl) {
        return this.nodeMap[nodeUrl] = {
          neoNode: null,
          visualNode: this.g.addNode(nodeUrl, {
            node: null,
            url: nodeUrl,
            explored: false,
            render: this.unexploredNodeRenderer
          })
        };
      };
      VisualizedView.prototype.addRelationship = function(rel) {
        return this.addRelationships([rel]);
      };
      VisualizedView.prototype.addAndGroupRelationships = function(rels, node) {
        var group, groups, rel, type, _i, _len;
        groups = {};
        for (_i = 0, _len = rels.length; _i < _len; _i++) {
          rel = rels[_i];
          if (!groups[rel.getType()]) {
            groups[rel.getType()] = {
              type: rel.getType(),
              size: 0,
              relationships: []
            };
          }
          groups[rel.getType()].size++;
          groups[rel.getType()].relationships.push(rel);
        }
        for (type in groups) {
          group = groups[type];
          if (group.size > 5) {
            this.addGroup(group, node);
          } else {
            this.addRelationships(group.relationships);
          }
        }
        return this.redrawVisualization();
      };
      VisualizedView.prototype.addGroup = function(group, node) {
        var id;
        id = "group-" + GROUP_IDS++;
        this.groupMap[id] = {
          group: group,
          visualNode: this.g.addNode(id, {
            id: id,
            size: group.size,
            render: this.groupRenderer
          })
        };
        return this.g.addEdge(node.getSelf(), id, {
          label: group.type
        });
      };
      VisualizedView.prototype.addRelationships = function(rels) {
        var rel, _i, _len, _results;
        _results = [];
        for (_i = 0, _len = rels.length; _i < _len; _i++) {
          rel = rels[_i];
          if (this.hasNode(rel.getEndNodeUrl()) === false) {
            this.addUnexploredNode(rel.getEndNodeUrl());
          }
          if (this.hasNode(rel.getStartNodeUrl()) === false) {
            this.addUnexploredNode(rel.getStartNodeUrl());
          }
          _results.push(this.g.addEdge(rel.getStartNodeUrl(), rel.getEndNodeUrl(), {
            label: rel.getType(),
            directed: true
          }));
        }
        return _results;
      };
      VisualizedView.prototype.nodeRenderer = function(r, node) {
        var circle, clickHandler, label, mouseOutHandler, mouseOverHandler, shape;
        circle = r.circle(0, 0, 10).attr({
          fill: "#ffffff",
          stroke: "#333333",
          "stroke-width": 2
        });
        if (node.node.hasProperty("name")) {
          label = r.text(0, 0, node.node.getProperty("name"));
        } else {
          label = r.text(0, 0, this.urlResolver.extractNodeId(node.url));
        }
        clickHandler = __bind(function(ev) {
          return this.nodeClicked(node, circle);
        }, this);
        mouseOverHandler = __bind(function(ev) {
          return this.mouseOverNode(ev, node, circle);
        }, this);
        mouseOutHandler = __bind(function(ev) {
          return this.mouseLeavingNode(ev, node, circle);
        }, this);
        circle.click(clickHandler);
        label.click(clickHandler);
        circle.hover(mouseOverHandler, mouseOutHandler);
        label.hover(mouseOverHandler, mouseOutHandler);
        shape = r.set().push(circle).push(label);
        return shape;
      };
      VisualizedView.prototype.unexploredNodeRenderer = function(r, node) {
        var circle, clickHandler, label, mouseOutHandler, mouseOverHandler, shape;
        circle = r.circle(0, 0, 10).attr({
          fill: "#ffffff",
          stroke: "#dddddd",
          "stroke-width": 2
        });
        label = r.text(0, 0, this.urlResolver.extractNodeId(node.url));
        clickHandler = __bind(function(ev) {
          return this.nodeClicked(node, circle);
        }, this);
        mouseOverHandler = __bind(function(ev) {
          return this.mouseOverNode(ev, node, circle);
        }, this);
        mouseOutHandler = __bind(function(ev) {
          return this.mouseLeavingNode(ev, node, circle);
        }, this);
        circle.click(clickHandler);
        label.click(clickHandler);
        circle.hover(mouseOverHandler, mouseOutHandler);
        label.hover(mouseOverHandler, mouseOutHandler);
        shape = r.set().push(circle).push(label);
        return shape;
      };
      VisualizedView.prototype.groupRenderer = function(r, group) {
        var circle, clickHandler, label, shape;
        circle = r.circle(0, 0, 6).attr({
          fill: "#eeeeee",
          stroke: "#dddddd",
          "stroke-width": 2
        });
        label = r.text(0, 0, group.size);
        clickHandler = __bind(function(ev) {
          return this.groupClicked(group, circle);
        }, this);
        circle.click(clickHandler);
        label.click(clickHandler);
        shape = r.set().push(circle).push(label);
        return shape;
      };
      VisualizedView.prototype.nodeClicked = function(nodeMeta, circle) {
        nodeMeta = this.nodeMap[nodeMeta.url].visualNode;
        if (nodeMeta.explored === false) {
          circle.attr({
            fill: "#ffffff",
            stroke: "#333333",
            "stroke-width": 2
          });
          return this.server.node(nodeMeta.url).then(__bind(function(node) {
            nodeMeta.explored = true;
            nodeMeta.node = node;
            this.addNode(node);
            return this.loadRelationships(node);
          }, this));
        }
      };
      VisualizedView.prototype.mouseOverNode = function(ev, nodeMeta, circle) {
        var html, key, node, propHtml, val;
        if (nodeMeta.explored) {
          node = nodeMeta.node;
          propHtml = (function() {
            var _ref, _results;
            _ref = node.getProperties();
            _results = [];
            for (key in _ref) {
              val = _ref[key];
              key = this.htmlEscaper.escape(key);
              val = this.htmlEscaper.escape(JSON.stringify(val));
              _results.push("<li><span class='key'>" + key + "</span>: <span class='value'>" + val + "</span></li>");
            }
            return _results;
          }).call(this);
          console.log(propHtml, node.getProperties(), node);
          propHtml = propHtml.join("\n");
          html = "<ul class='tiny-property-list'>" + propHtml + "</ul>";
        } else {
          html = "<p>Unexplored node</p>";
        }
        return this.tooltip.show(html, [ev.clientX, ev.clientY]);
      };
      VisualizedView.prototype.mouseLeavingNode = function(ev, nodeMeta, circle) {
        return this.tooltip.hide();
      };
      VisualizedView.prototype.groupClicked = function(groupMeta, circle) {
        var group, ungroup, visualNode;
        group = this.groupMap[groupMeta.id].group;
        visualNode = this.groupMap[groupMeta.id].visualNode;
        this.showLoader();
        ungroup = __bind(function() {
          this.removeVisualNode(visualNode);
          this.addExplodedGroup(group);
          this.redrawVisualization();
          return this.hideLoader();
        }, this);
        return setTimeout(ungroup, 1);
      };
      VisualizedView.prototype.loadRelationships = function(node) {
        this.showLoader();
        return node.getRelationships().then(__bind(function(rels) {
          this.addAndGroupRelationships(rels, node);
          return this.hideLoader();
        }, this));
      };
      VisualizedView.prototype.removeVisualNode = function(visualNode, id) {
        var edge, _i, _len, _ref;
        visualNode.hide();
        _ref = visualNode.edges;
        for (_i = 0, _len = _ref.length; _i < _len; _i++) {
          edge = _ref[_i];
          edge.connection.label.hide();
          edge.hide();
        }
        return this.g.removeNode(visualNode.id);
      };
      VisualizedView.prototype.showLoader = function() {
        this.hideLoader();
        this.loader = new LoadingSpinner($(".workarea"));
        return this.loader.show();
      };
      VisualizedView.prototype.hideLoader = function() {
        if (this.loader != null) {
          return this.loader.destroy();
        }
      };
      VisualizedView.prototype.remove = function() {
        this.dataModel.unbind("change", this.render);
        return VisualizedView.__super__.remove.call(this);
      };
      VisualizedView.prototype.detach = function() {
        this.dataModel.unbind("change", this.render);
        return VisualizedView.__super__.detach.call(this);
      };
      VisualizedView.prototype.attach = function(parent) {
        VisualizedView.__super__.attach.call(this, parent);
        return this.dataModel.bind("change", this.render);
      };
      return VisualizedView;
    })();
  });
}).call(this);
