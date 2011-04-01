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
  */  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; };
  define(['neo4j/webadmin/visualization/Renderer', 'order!lib/jquery', 'order!lib/arbor', 'order!lib/arbor-graphics', 'order!lib/arbor-tween'], function(Renderer) {
    var VisualGraph;
    return VisualGraph = (function() {
      function VisualGraph(server, width, height) {
        this.server = server;
        if (width == null) {
          width = 800;
        }
        if (height == null) {
          height = 400;
        }
        this.detach = __bind(this.detach, this);;
        this.attach = __bind(this.attach, this);;
        this.start = __bind(this.start, this);;
        this.stop = __bind(this.stop, this);;
        this.nodeClicked = __bind(this.nodeClicked, this);;
        this.addNodes = __bind(this.addNodes, this);;
        this.addNode = __bind(this.addNode, this);;
        this.setNodes = __bind(this.setNodes, this);;
        this.setNode = __bind(this.setNode, this);;
        this.getLabelFor = __bind(this.getLabelFor, this);;
        this.el = $("<canvas width='" + width + "' height='" + height + "'></canvas>");
        this.groupCount = 0;
        this.visualizedGraph = {
          nodes: {},
          edges: {}
        };
        this.sys = arbor.ParticleSystem(1000, 600, 0.5, true, 30, 0.03);
        this.stop();
        this.sys.renderer = new Renderer(this.el, this);
        this.sys.renderer.bind("node:click", this.nodeClicked);
        this.sys.screenPadding(20);
      }
      VisualGraph.prototype.getLabelFor = function(visualNode) {
        switch (visualNode.data.type) {
          case "explored-node":
            return visualNode.data.neoNode.getSelf();
          case "unexplored-node":
            return "Unexplored";
          default:
            return "Group";
        }
      };
      VisualGraph.prototype.setNode = function(node) {
        return this.setNodes([node]);
      };
      VisualGraph.prototype.setNodes = function(nodes) {
        this.visualizedGraph = {
          nodes: {},
          edges: {}
        };
        return this.addNodes(nodes);
      };
      VisualGraph.prototype.addNode = function(node) {
        return this.addNodes([node]);
      };
      VisualGraph.prototype.addNodes = function(nodes) {
        var fetchCountdown, node, nodeMap, relMap, _i, _len, _results;
        relMap = this.visualizedGraph.edges;
        nodeMap = this.visualizedGraph.nodes;
        fetchCountdown = nodes.length;
        _results = [];
        for (_i = 0, _len = nodes.length; _i < _len; _i++) {
          node = nodes[_i];
          nodeMap[node.getSelf()] = {
            neoNode: node,
            type: "explored-node"
          };
          _results.push(node.getRelationships().then(__bind(function(rels) {
            var rel, _base, _i, _len, _name, _name2, _name3, _name4, _ref, _ref2, _ref3, _ref4;
            for (_i = 0, _len = rels.length; _i < _len; _i++) {
              rel = rels[_i];
              (_ref = nodeMap[_name = rel.getStartNodeUrl()]) != null ? _ref : nodeMap[_name] = {
                neoUrl: rel.getStartNodeUrl(),
                type: "unexplored-node"
              };
              (_ref2 = nodeMap[_name2 = rel.getEndNodeUrl()]) != null ? _ref2 : nodeMap[_name2] = {
                neoUrl: rel.getEndNodeUrl(),
                type: "unexplored-node"
              };
              (_ref3 = relMap[_name3 = rel.getStartNodeUrl()]) != null ? _ref3 : relMap[_name3] = {};
              (_ref4 = (_base = relMap[rel.getStartNodeUrl()])[_name4 = rel.getEndNodeUrl()]) != null ? _ref4 : _base[_name4] = {
                relationships: []
              };
              relMap[rel.getStartNodeUrl()][rel.getEndNodeUrl()].relationships.push(rel);
            }
            if ((--fetchCountdown) === 0) {
              this.visualizedGraph = {
                nodes: nodeMap,
                edges: relMap
              };
              return this.sys.merge(this.visualizedGraph);
            }
          }, this)));
        }
        return _results;
      };
      VisualGraph.prototype.nodeClicked = function(visualNode) {
        if ((visualNode.data.type != null) && visualNode.data.type === "unexplored-node") {
          return this.server.node(visualNode.data.neoUrl).then(this.addNode);
        }
      };
      VisualGraph.prototype.stop = function() {
        return this.sys.stop();
      };
      VisualGraph.prototype.start = function() {
        return this.sys.start();
      };
      VisualGraph.prototype.attach = function(parent) {
        this.detach();
        $(parent).append(this.el);
        return this.start();
      };
      VisualGraph.prototype.detach = function() {
        this.stop();
        return this.el.detach();
      };
      return VisualGraph;
    })();
  });
}).call(this);
