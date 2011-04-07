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
  define(['lib/backbone'], function() {
    var LoadingSpinner;
    return LoadingSpinner = (function() {
      function LoadingSpinner(baseElement) {
        this.baseElement = baseElement;
        this.position = __bind(this.position, this);;
        this.destroy = __bind(this.destroy, this);;
        this.hide = __bind(this.hide, this);;
        this.show = __bind(this.show, this);;
        this.el = $("<div class='loading-spinner'><div class='inner'></div></div>");
        this.el.hide();
        $("body").append(this.el);
        this.position();
      }
      LoadingSpinner.prototype.show = function() {
        return this.el.show();
      };
      LoadingSpinner.prototype.hide = function() {
        return this.el.hide();
      };
      LoadingSpinner.prototype.destroy = function() {
        return this.el.remove();
      };
      LoadingSpinner.prototype.position = function() {
        var basePos;
        basePos = $(this.baseElement).offset();
        return $(this.el).css({
          position: "absolute",
          top: basePos.top + "px",
          left: basePos.left + "px",
          width: $(this.baseElement).outerWidth() + "px",
          height: $(this.baseElement).outerHeight() + "px"
        });
      };
      return LoadingSpinner;
    })();
  });
}).call(this);
