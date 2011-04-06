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
  define(['neo4j/webadmin/visualization/Renderer', 'neo4j/webadmin/visualization/NodeStyler', 'neo4j/webadmin/visualization/RelationshipStyler', 'neo4j/webadmin/visualization/VisualDataModel', 'neo4j/webadmin/views/NodeFilterDialog', 'order!lib/jquery', 'order!lib/arbor', 'order!lib/arbor-graphics', 'order!lib/arbor-tween'], function(Renderer, NodeStyler, RelationshipStyler, VisualDataModel, NodeFilterDialog) {
    var VisualGraph;
    return VisualGraph = (function() {
      function VisualGraph(server, width, height, groupingThreshold) {
        this.server = server;
        if (width == null) {
          width = 800;
        }
        if (height == null) {
          height = 400;
        }
        this.groupingThreshold = groupingThreshold != null ? groupingThreshold : 10;
        this.detach = __bind(this.detach, this);;
        this.attach = __bind(this.attach, this);;
        this.start = __bind(this.start, this);;
        this.stop = __bind(this.stop, this);;
        this.floatNode = __bind(this.floatNode, this);;
        this.reflow = __bind(this.reflow, this);;
        this.getNodeStyler = __bind(this.getNodeStyler, this);;
        this.nodeClicked = __bind(this.nodeClicked, this);;
        this.addNodes = __bind(this.addNodes, this);;
        this.addNode = __bind(this.addNode, this);;
        this.setNodes = __bind(this.setNodes, this);;
        this.setNode = __bind(this.setNode, this);;
        this.steadyStateCheck = __bind(this.steadyStateCheck, this);;
        this.el = $("<canvas width='" + width + "' height='" + height + "'></canvas>");
        this.labelProperties = [];
        this.nodeStyler = new NodeStyler();
        this.relationshipStyler = new RelationshipStyler();
        this.dataModel = new VisualDataModel();
        this.sys = arbor.ParticleSystem();
        this.sys.parameters({
          repulsion: 10,
          stiffness: 100,
          friction: 0.5,
          gravity: true,
          fps: 30,
          dt: 0.015,
          precision: 0.5
        });
        this.stop();
        this.sys.renderer = new Renderer(this.el, this.nodeStyler, this.relationshipStyler);
        this.sys.renderer.bind("node:click", this.nodeClicked);
        this.sys.screenPadding(20);
        this.steadStateWorker = setInterval(this.steadyStateCheck, 1000);
      }
      VisualGraph.prototype.steadyStateCheck = function() {
        var energy, meanEnergy;
        energy = this.sys.energy();
        if (energy != null) {
          meanEnergy = energy.mean;
          if (meanEnergy < 0.01) {
            return this.sys.stop();
          }
        }
      };
      VisualGraph.prototype.setNode = function(node) {
        return this.setNodes([node]);
      };
      VisualGraph.prototype.setNodes = function(nodes) {
        this.dataModel.clear();
        return this.addNodes(nodes);
      };
      VisualGraph.prototype.addNode = function(node) {
        return this.addNodes([node]);
      };
      VisualGraph.prototype.addNodes = function(nodes) {
        var fetchCountdown, node, _i, _len, _results;
        fetchCountdown = nodes.length;
        this.sys.stop();
        _results = [];
        for (_i = 0, _len = nodes.length; _i < _len; _i++) {
          node = nodes[_i];
          _results.push(__bind(function(node) {
            var relPromise, relatedNodesPromise;
            relPromise = node.getRelationships();
            relatedNodesPromise = node.traverse({});
            return neo4j.Promise.join(relPromise, relatedNodesPromise).then(__bind(function(result) {
              var rels;
              rels = result[0], nodes = result[1];
              this.dataModel.addNode(node, rels, nodes);
              if ((--fetchCountdown) === 0) {
                this.sys.merge(this.dataModel.getVisualGraph());
                return this.sys.start();
              }
            }, this));
          }, this)(node));
        }
        return _results;
      };
      VisualGraph.prototype.nodeClicked = function(visualNode) {
        var completeCallback, dialog, groupedMeta, nodes, url;
        if (visualNode.data.type != null) {
          switch (visualNode.data.type) {
            case "unexplored":
              return this.addNode(visualNode.data.neoNode);
            case "explored":
              this.dataModel.unexplore(visualNode.data.neoNode);
              return this.sys.merge(this.dataModel.getVisualGraph());
            case "group":
              nodes = (function() {
                var _ref, _results;
                _ref = visualNode.data.group.grouped;
                _results = [];
                for (url in _ref) {
                  groupedMeta = _ref[url];
                  _results.push(groupedMeta.node);
                }
                return _results;
              })();
              completeCallback = __bind(function(filteredNodes, dialog) {
                dialog.remove();
                this.dataModel.ungroup(filteredNodes);
                return this.sys.merge(this.dataModel.getVisualGraph());
              }, this);
              dialog = new NodeFilterDialog({
                nodes: nodes,
                completeCallback: completeCallback,
                labelProperties: this.labelProperties
              });
              return dialog.show();
          }
        }
      };
      VisualGraph.prototype.setLabelProperties = function(labelProps) {
        this.nodeStyler.setLabelProperties(labelProps);
        return this.labelProperties = labelProps;
      };
      VisualGraph.prototype.getNodeStyler = function() {
        return this.nodeStyler;
      };
      VisualGraph.prototype.reflow = function() {
        this.sys.eachNode(this.floatNode);
        this.sys.parameters({
          gravity: true
        });
        return this.start();
      };
      VisualGraph.prototype.floatNode = function(node, pt) {
        return node.fixed = false;
      };
      VisualGraph.prototype.stop = function() {
        if (this.sys.renderer != null) {
          this.sys.renderer.stop();
        }
        this.sys.parameters({
          gravity: false
        });
        return this.sys.stop();
      };
      VisualGraph.prototype.start = function() {
        if (this.sys.renderer != null) {
          this.sys.renderer.start();
        }
        return this.sys.start();
      };
      VisualGraph.prototype.attach = function(parent) {
        this.detach();
        $(parent).prepend(this.el);
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
