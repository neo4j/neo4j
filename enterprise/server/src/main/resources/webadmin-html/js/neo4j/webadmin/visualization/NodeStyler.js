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
  */  define(['neo4j/webadmin/data/ItemUrlResolver'], function(ItemUrlResolver) {
    var NodeStyler;
    return NodeStyler = (function() {
      NodeStyler.prototype.defaultExploredStyle = {
        nodeStyle: {
          fill: "#000000",
          alpha: 0.6
        },
        labelStyle: {
          color: "white",
          font: "12px Helvetica"
        }
      };
      NodeStyler.prototype.defaultUnexploredStyle = {
        nodeStyle: {
          fill: "#000000",
          alpha: 0.2
        },
        labelStyle: {
          color: "rgba(255, 255, 255, 0.4)",
          font: "12px Helvetica"
        }
      };
      NodeStyler.prototype.defaultGroupStyle = {
        nodeStyle: {
          shape: "dot",
          fill: "#000000",
          alpha: 0.2
        },
        labelStyle: {
          color: "white",
          font: "12px Helvetica"
        }
      };
      function NodeStyler() {
        this.labelProperties = [];
        this.itemUrlUtils = new ItemUrlResolver();
      }
      NodeStyler.prototype.getStyleFor = function(visualNode) {
        var label, node, prop, _i, _len, _ref;
        switch (visualNode.data.type) {
          case "explored-node":
            node = visualNode.data.neoNode;
            _ref = this.labelProperties;
            for (_i = 0, _len = _ref.length; _i < _len; _i++) {
              prop = _ref[_i];
              if (node.hasProperty(prop)) {
                label = node.getProperty(prop);
                break;
              }
            }
            label != null ? label : label = this.itemUrlUtils.extractNodeId(node.getSelf());
            return {
              nodeStyle: this.defaultExploredStyle.nodeStyle,
              labelStyle: this.defaultExploredStyle.labelStyle,
              labelText: label
            };
          case "unexplored-node":
            label = this.itemUrlUtils.extractNodeId(visualNode.data.neoUrl);
            return {
              nodeStyle: this.defaultUnexploredStyle.nodeStyle,
              labelStyle: this.defaultUnexploredStyle.labelStyle,
              labelText: label
            };
          default:
            return {
              nodeStyle: this.defaultGroupStyle.nodeStyle,
              labelStyle: this.defaultGroupStyle.labelStyle,
              labelText: "Group"
            };
        }
      };
      NodeStyler.prototype.setLabelProperties = function(labelProperties) {
        return this.labelProperties = labelProperties;
      };
      return NodeStyler;
    })();
  });
}).call(this);
