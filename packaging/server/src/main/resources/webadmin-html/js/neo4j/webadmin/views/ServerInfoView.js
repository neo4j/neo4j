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
  define(['neo4j/webadmin/templates/server_info', 'neo4j/webadmin/templates/server_info_bean', 'lib/backbone'], function(baseTemplate, beanTemplate) {
    var ServerInfoView;
    return ServerInfoView = (function() {
      function ServerInfoView() {
        this.flattenAttributes = __bind(this.flattenAttributes, this);;
        this.renderBean = __bind(this.renderBean, this);;
        this.render = __bind(this.render, this);;        ServerInfoView.__super__.constructor.apply(this, arguments);
      }
      __extends(ServerInfoView, Backbone.View);
      ServerInfoView.prototype.initialize = function(options) {
        this.serverInfo = options.serverInfo;
        this.baseTemplate = baseTemplate;
        this.beanTemplate = beanTemplate;
        this.serverInfo.bind("change:domains", this.render);
        return this.serverInfo.bind("change:current", this.renderBean);
      };
      ServerInfoView.prototype.render = function() {
        $(this.el).html(this.baseTemplate({
          domains: this.serverInfo.get("domains")
        }));
        this.renderBean();
        return this;
      };
      ServerInfoView.prototype.renderBean = function() {
        var bean;
        bean = this.serverInfo.get("current");
        $("#info-bean", this.el).empty().append(this.beanTemplate({
          bean: bean,
          attributes: bean != null ? this.flattenAttributes(bean.attributes) : []
        }));
        return this;
      };
      ServerInfoView.prototype.flattenAttributes = function(attributes, flattened, indent) {
        var attr, name, pushedAttr, _i, _len;
        if (flattened == null) {
          flattened = [];
        }
        if (indent == null) {
          indent = 1;
        }
        for (_i = 0, _len = attributes.length; _i < _len; _i++) {
          attr = attributes[_i];
          name = attr.name != null ? attr.name : attr.type != null ? attr.type : "";
          pushedAttr = {
            name: name,
            description: attr.description,
            indent: indent
          };
          flattened.push(pushedAttr);
          if (!(attr.value != null)) {
            pushedAttr.value = "";
          } else if (_(attr.value).isArray() && _(attr.value[0]).isString()) {
            pushedAttr.value = attr.value.join(", ");
          } else if (_(attr.value).isArray()) {
            pushedAttr.value = "";
            this.flattenAttributes(attr.value, flattened, indent + 1);
          } else if (typeof attr.value === "object") {
            pushedAttr.value = "";
            this.flattenAttributes(attr.value.value, flattened, indent + 1);
          } else {
            pushedAttr.value = attr.value;
          }
        }
        return flattened;
      };
      return ServerInfoView;
    })();
  });
}).call(this);
