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
  define(['neo4j/webadmin/ui/Overlay', 'neo4j/webadmin/views/View', 'lib/backbone'], function(Overlay, View) {
    var Dialog;
    return Dialog = (function() {
      function Dialog() {
        this.attach = __bind(this.attach, this);;
        this.detach = __bind(this.detach, this);;
        this.remove = __bind(this.remove, this);;
        this.hide = __bind(this.hide, this);;
        this.wrapperClicked = __bind(this.wrapperClicked, this);;
        this.show = __bind(this.show, this);;        Dialog.__super__.constructor.apply(this, arguments);
      }
      __extends(Dialog, View);
      Dialog.prototype.className = "dialog";
      Dialog.prototype.initialize = function() {
        this.wrapper = $("<div class='dialog-wrap'></div>");
        this.wrapper.append(this.el);
        this.attachedToBody = false;
        return this.overlay = new Overlay();
      };
      Dialog.prototype.show = function(timeout) {
        if (timeout == null) {
          timeout = false;
        }
        this.overlay.show();
        this.bind();
        if (!this.attachedToBody) {
          $("body").append(this.wrapper);
        }
        this.render();
        this.wrapper.show();
        if (timeout) {
          return setTimeout(this.hide, timeout);
        }
      };
      Dialog.prototype.bind = function() {
        return this.wrapper.bind("click", this.wrapperClicked);
      };
      Dialog.prototype.unbind = function() {
        return this.wrapper.unbind("click", this.wrapperClicked);
      };
      Dialog.prototype.wrapperClicked = function(ev) {
        if (ev.originalTarget === ev.currentTarget) {
          return this.hide();
        }
      };
      Dialog.prototype.hide = function() {
        this.unbind();
        this.wrapper.hide();
        return this.overlay.hide();
      };
      Dialog.prototype.remove = function() {
        this.unbind();
        this.wrapper.remove();
        return this.overlay.hide();
      };
      Dialog.prototype.detach = function() {
        return this.wrapper.detach();
      };
      Dialog.prototype.attach = function(parent) {
        return $(parent).append(this.wrapper);
      };
      return Dialog;
    })();
  });
}).call(this);
