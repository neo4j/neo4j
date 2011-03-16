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
  define(['lib/backbone', 'lib/jquery'], function() {
    var Tooltip;
    return Tooltip = (function() {
      Tooltip.prototype.defaultSettings = {
        hideOnMouseOut: true,
        css: {},
        position: "above",
        closeButton: true
      };
      function Tooltip(opts) {
        if (opts == null) {
          opts = {};
        }
        this.getTooltipPositionFor = __bind(this.getTooltipPositionFor, this);;
        this.getPosition = __bind(this.getPosition, this);;
        this.onWindowResized = __bind(this.onWindowResized, this);;
        this.remove = __bind(this.remove, this);;
        this.hide = __bind(this.hide, this);;
        this.show = __bind(this.show, this);;
        this._tooltip = $("<div class='tooltip-wrap'></div>");
        this._tooltipContent = $("<div class='tooltip'></div>");
        this._closeButton = $("<div class='tooltip-close'></div>");
        this._currentPos = [0, 0];
        this._currentContent = "";
        this._visible = false;
        this.settings = _.extend(this.defaultSettings, opts);
        if (this.settings.hideOnMouseOut) {
          this._tooltip.bind("mouseout", this.hide);
        }
        this._tooltip.addClass("tooltip-pos-" + this.settings.position);
        this._tooltip.css(this.settings.css);
        this._tooltip.append(this._tooltipContent);
        if (this.settings.closeButton) {
          this._tooltip.append(this._closeButton);
          this._closeButton.bind("click", this.hide);
        }
        this._tooltip.appendTo("body");
        $(window).resize(this.onWindowResized);
      }
      Tooltip.prototype.show = function(content, pos, timeout) {
        this._currentPos = pos;
        this._currentContent = content;
        this._tooltipContent.html(content);
        pos = this.getTooltipPositionFor(this.getPosition(pos));
        this._tooltip.css({
          left: pos[0],
          top: pos[1]
        }).show();
        this._visible = true;
        if (timeout) {
          return setTimeout(this.hide, timeout);
        }
      };
      Tooltip.prototype.hide = function() {
        this._tooltip.hide();
        return this._visible = false;
      };
      Tooltip.prototype.remove = function() {
        this._tooltip.unbind("mouseout", this.hide);
        this._closeButton.unbind("click", this.hide);
        return this._tooltip.remove();
      };
      Tooltip.prototype.onWindowResized = function() {
        var updatePosition;
        if (this._visible) {
          updatePosition = __bind(function() {
            return this.show(this._currentContent, this._currentPos);
          }, this);
          return setTimeout(updatePosition, 0);
        }
      };
      Tooltip.prototype.getPosition = function(pos) {
        var el;
        if (_.isArray(pos)) {
          return pos;
        } else {
          el = $(pos);
          pos = el.offset();
          switch (this.settings.position) {
            case "right":
              return [pos.left + (el.width()), pos.top + (el.height() / 2)];
            case "left":
              return [pos.left, pos.top + (el.height() / 2)];
            case "above":
              return [pos.left + (el.width() / 2), pos.top];
            default:
              return [pos.left + (el.width() / 2), pos.top - el.height()];
          }
        }
      };
      Tooltip.prototype.getTooltipPositionFor = function(pointToPosition) {
        switch (this.settings.position) {
          case "right":
            return [pointToPosition[0] + 10, pointToPosition[1] - (this._tooltip.height() / 2)];
          case "left":
            return [pointToPosition[0] - (this._tooltip.width() + 10), pointToPosition[1] - (this._tooltip.height() / 2)];
          case "above":
            return [pointToPosition[0] - this._tooltip.width() / 2, pointToPosition[1] - (this._tooltip.height() + 10)];
          default:
            return [pointToPosition[0] - this._tooltip.width() / 2, pointToPosition[1] + (this._tooltip.height())];
        }
      };
      return Tooltip;
    })();
  });
}).call(this);
