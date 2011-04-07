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
  define([], function() {
    var VisualDataModel;
    return VisualDataModel = (function() {
      function VisualDataModel(groupingThreshold) {
        this.groupingThreshold = groupingThreshold != null ? groupingThreshold : 5;
        this.addNode = __bind(this.addNode, this);;
        this.clear();
      }
      VisualDataModel.prototype.clear = function() {
        this.groupCount = 0;
        this.visualGraph = {
          nodes: {},
          edges: {}
        };
        return this.data = {
          relationships: {},
          nodes: {},
          groups: {}
        };
      };
      VisualDataModel.prototype.getVisualGraph = function() {
        return this.visualGraph;
      };
      VisualDataModel.prototype.addNode = function(node, relationships, relatedNodes) {
        var dir, group, groups, otherUrl, potentialGroups, rel, relatedNode, type, _base, _base2, _base3, _i, _j, _len, _len2, _name, _name2, _name3, _ref, _ref2, _ref3, _results;
        this.ungroup([node]);
        this.ungroup(relatedNodes);
        (_ref = (_base = this.data.nodes)[_name = node.getSelf()]) != null ? _ref : _base[_name] = {
          node: node,
          groups: {}
        };
        this.visualGraph.nodes[node.getSelf()] = {
          neoNode: node,
          type: "explored"
        };
        for (_i = 0, _len = relatedNodes.length; _i < _len; _i++) {
          relatedNode = relatedNodes[_i];
          (_ref2 = (_base2 = this.data.nodes)[_name2 = relatedNode.getSelf()]) != null ? _ref2 : _base2[_name2] = {
            node: relatedNode,
            groups: {}
          };
        }
        potentialGroups = {
          incoming: {},
          outgoing: {}
        };
        for (_j = 0, _len2 = relationships.length; _j < _len2; _j++) {
          rel = relationships[_j];
          if (this.data.relationships[rel.getSelf()] != null) {
            continue;
          }
          this.data.relationships[rel.getSelf()] = rel;
          otherUrl = rel.getOtherNodeUrl(node.getSelf());
          dir = rel.isStartNode(node.getSelf()) ? "outgoing" : "incoming";
          if (!(this.visualGraph.nodes[otherUrl] != null)) {
            (_ref3 = (_base3 = potentialGroups[dir])[_name3 = rel.getType()]) != null ? _ref3 : _base3[_name3] = {
              relationships: []
            };
            potentialGroups[dir][rel.getType()].relationships.push(rel);
          } else {
            this._addRelationship(rel.getStartNodeUrl(), rel.getEndNodeUrl(), rel);
          }
        }
        _results = [];
        for (dir in potentialGroups) {
          groups = potentialGroups[dir];
          _results.push((function() {
            var _results;
            _results = [];
            for (type in groups) {
              group = groups[type];
              _results.push((function() {
                var _i, _len, _ref, _results;
                if (group.relationships.length >= this.groupingThreshold) {
                  return this._addGroup(node, group.relationships, dir);
                } else {
                  _ref = group.relationships;
                  _results = [];
                  for (_i = 0, _len = _ref.length; _i < _len; _i++) {
                    rel = _ref[_i];
                    _results.push(this._addUnexploredNode(node, rel));
                  }
                  return _results;
                }
              }).call(this));
            }
            return _results;
          }).call(this));
        }
        return _results;
      };
      VisualDataModel.prototype.ungroup = function(nodes) {
        var group, groupMeta, key, meta, node, nodeUrl, rel, _i, _len, _results;
        _results = [];
        for (_i = 0, _len = nodes.length; _i < _len; _i++) {
          node = nodes[_i];
          nodeUrl = node.getSelf();
          _results.push((function() {
            var _i, _len, _ref, _ref2, _results;
            if (this.data.nodes[nodeUrl] != null) {
              meta = this.data.nodes[nodeUrl];
              _ref = meta.groups;
              _results = [];
              for (key in _ref) {
                groupMeta = _ref[key];
                group = groupMeta.group;
                group.nodeCount--;
                delete group.grouped[nodeUrl];
                _ref2 = groupMeta.relationships;
                for (_i = 0, _len = _ref2.length; _i < _len; _i++) {
                  rel = _ref2[_i];
                  this._addUnexploredNode(group.baseNode, rel);
                }
                _results.push(group.nodeCount <= 0 ? this._removeGroup(group) : void 0);
              }
              return _results;
            }
          }).call(this));
        }
        return _results;
      };
      VisualDataModel.prototype.unexplore = function(node) {
        var nodeUrl, potentiallRemove, relatedNodeMeta, relatedNodeUrl, visualNode;
        nodeUrl = node.getSelf();
        if (this._isLastExploredNode(node)) {
          return;
        }
        if (this.visualGraph.nodes[nodeUrl] != null) {
          visualNode = this.visualGraph.nodes[nodeUrl];
          visualNode.type = "unexplored";
          node.fixed = false;
          potentiallRemove = this._getUnexploredNodesRelatedTo(nodeUrl);
          for (relatedNodeUrl in potentiallRemove) {
            relatedNodeMeta = potentiallRemove[relatedNodeUrl];
            if (!this._hasExploredRelationships(relatedNodeUrl, nodeUrl)) {
              this.removeNode(relatedNodeMeta.node);
            }
          }
          this._removeGroupsFor(node);
          if (!this._hasExploredRelationships(nodeUrl)) {
            return this.removeNode(node);
          }
        }
      };
      VisualDataModel.prototype.removeNode = function(node) {
        var nodeUrl;
        nodeUrl = node.getSelf();
        delete this.visualGraph.nodes[nodeUrl];
        delete this.data.nodes[nodeUrl];
        return this._removeRelationshipsFor(node);
      };
      VisualDataModel.prototype._isLastExploredNode = function(node) {
        var nodeUrl, url, visualNode, _ref;
        nodeUrl = node.getSelf();
        _ref = this.visualGraph.nodes;
        for (url in _ref) {
          visualNode = _ref[url];
          if (visualNode.type === "explored" && url !== nodeUrl) {
            return false;
          }
        }
        return true;
      };
      VisualDataModel.prototype._getUnexploredNodesRelatedTo = function(nodeUrl) {
        var found, fromUrl, relMeta, toMap, toUrl, _ref;
        found = [];
        _ref = this.visualGraph.edges;
        for (fromUrl in _ref) {
          toMap = _ref[fromUrl];
          for (toUrl in toMap) {
            relMeta = toMap[toUrl];
            if (fromUrl === nodeUrl) {
              if ((this.visualGraph.nodes[toUrl].type != null) && this.visualGraph.nodes[toUrl].type === "unexplored") {
                found[toUrl] = this.data.nodes[toUrl];
              }
            }
            if (toUrl === nodeUrl) {
              if ((this.visualGraph.nodes[fromUrl].type != null) && this.visualGraph.nodes[fromUrl].type === "unexplored") {
                found[fromUrl] = this.data.nodes[fromUrl];
              }
            }
          }
        }
        return found;
      };
      VisualDataModel.prototype._hasExploredRelationships = function(nodeUrl, excludeNodeUrl) {
        var fromUrl, relMeta, toMap, toUrl, _ref;
        if (excludeNodeUrl == null) {
          excludeNodeUrl = "";
        }
        _ref = this.visualGraph.edges;
        for (fromUrl in _ref) {
          toMap = _ref[fromUrl];
          for (toUrl in toMap) {
            relMeta = toMap[toUrl];
            if (fromUrl === nodeUrl) {
              if (!(toUrl === excludeNodeUrl) && this.visualGraph.nodes[toUrl].type === "explored") {
                return true;
              }
            }
            if (toUrl === nodeUrl) {
              if (!(fromUrl === excludeNodeUrl) && this.visualGraph.nodes[fromUrl].type === "explored") {
                return true;
              }
            }
          }
        }
        return false;
      };
      VisualDataModel.prototype._addRelationship = function(from, to, rel, relType) {
        var _base, _base2, _ref, _ref2;
        if (relType == null) {
          relType = null;
        }
        (_ref = (_base = this.visualGraph.edges)[from]) != null ? _ref : _base[from] = {};
        (_ref2 = (_base2 = this.visualGraph.edges[from])[to]) != null ? _ref2 : _base2[to] = {
          length: .5,
          relationships: {},
          directed: true,
          relType: relType
        };
        if (rel !== false) {
          return this.visualGraph.edges[from][to].relationships[rel.getSelf()] = rel;
        }
      };
      VisualDataModel.prototype._addUnexploredNode = function(baseNode, rel) {
        var unexploredUrl, _base, _ref;
        unexploredUrl = rel.getOtherNodeUrl(baseNode.getSelf());
        (_ref = (_base = this.visualGraph.nodes)[unexploredUrl]) != null ? _ref : _base[unexploredUrl] = {
          neoNode: this.data.nodes[unexploredUrl].node,
          type: "unexplored"
        };
        return this._addRelationship(rel.getStartNodeUrl(), rel.getEndNodeUrl(), rel);
      };
      VisualDataModel.prototype._addGroup = function(baseNode, relationships, direction) {
        var baseNodeUrl, group, grouped, key, meta, nodeCount, nodeMeta, nodeUrl, rel, url, _i, _len;
        baseNodeUrl = baseNode.getSelf();
        nodeCount = 0;
        grouped = {};
        for (_i = 0, _len = relationships.length; _i < _len; _i++) {
          rel = relationships[_i];
          nodeUrl = rel.getOtherNodeUrl(baseNodeUrl);
          if (!(this.data.nodes[nodeUrl] != null)) {
            continue;
          }
          nodeMeta = this.data.nodes[nodeUrl];
          if (!(grouped[nodeUrl] != null)) {
            grouped[nodeUrl] = {
              node: nodeMeta.node,
              relationships: []
            };
            nodeCount++;
          }
          grouped[nodeUrl].relationships.push(rel);
        }
        key = "group-" + (this.groupCount++);
        group = this.data.groups[key] = {
          key: key,
          baseNode: baseNode,
          grouped: grouped,
          nodeCount: nodeCount
        };
        this.visualGraph.nodes[key] = {
          key: key,
          type: "group",
          group: group
        };
        for (url in grouped) {
          meta = grouped[url];
          this.data.nodes[url].groups[key] = {
            group: group,
            relationships: meta.relationships
          };
        }
        if (direction === "outgoing") {
          return this._addRelationship(baseNode.getSelf(), key, false, relationships[0].getType());
        } else {
          return this._addRelationship(key, baseNode.getSelf(), false, relationships[0].getType());
        }
      };
      VisualDataModel.prototype._removeRelationshipsFor = function(node) {
        var fromUrl, nodeUrl, rel, relMeta, toMap, toUrl, url, _ref, _ref2;
        nodeUrl = node.getSelf();
        _ref = this.visualGraph.edges;
        for (fromUrl in _ref) {
          toMap = _ref[fromUrl];
          for (toUrl in toMap) {
            relMeta = toMap[toUrl];
            if (toUrl === nodeUrl || fromUrl === nodeUrl) {
              _ref2 = this.visualGraph.edges[fromUrl][toUrl].relationships;
              for (url in _ref2) {
                rel = _ref2[url];
                delete this.data.relationships[rel.getSelf()];
              }
            }
            if (toUrl === nodeUrl) {
              delete this.visualGraph.edges[fromUrl][toUrl];
            }
          }
        }
        if (this.visualGraph.edges[nodeUrl] != null) {
          return delete this.visualGraph.edges[nodeUrl];
        }
      };
      VisualDataModel.prototype._removeRelationshipsInGroup = function(group) {
        var grouped, node, rel, url, _ref, _results;
        _ref = group.grouped;
        _results = [];
        for (url in _ref) {
          grouped = _ref[url];
          node = this.data.nodes[url];
          if (node.groups[group.key] != null) {
            delete node.groups[group.key];
          }
          _results.push((function() {
            var _i, _len, _ref, _results;
            _ref = grouped.relationships;
            _results = [];
            for (_i = 0, _len = _ref.length; _i < _len; _i++) {
              rel = _ref[_i];
              _results.push(delete this.data.relationships[rel.getSelf()]);
            }
            return _results;
          }).call(this));
        }
        return _results;
      };
      VisualDataModel.prototype._removeGroup = function(group) {
        delete this.visualGraph.nodes[group.key];
        delete this.data.groups[group.key];
        if (this.visualGraph.edges[group.key] != null) {
          return delete this.visualGraph.edges[group.key];
        } else {
          if ((this.visualGraph.edges[group.baseNode.getSelf()] != null) && (this.visualGraph.edges[group.baseNode.getSelf()][group.key] != null)) {
            return delete this.visualGraph.edges[group.baseNode.getSelf()][group.key];
          }
        }
      };
      VisualDataModel.prototype._removeGroupsFor = function(node) {
        var group, key, nodeUrl, _ref, _results;
        nodeUrl = node.getSelf();
        _ref = this.data.groups;
        _results = [];
        for (key in _ref) {
          group = _ref[key];
          _results.push(group.baseNode.getSelf() === nodeUrl ? (this._removeRelationshipsInGroup(group), this._removeGroup(group)) : void 0);
        }
        return _results;
      };
      return VisualDataModel;
    })();
  });
}).call(this);
