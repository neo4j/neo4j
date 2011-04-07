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
    var HtmlEscaper;
    return HtmlEscaper = (function() {
      function HtmlEscaper() {
        this.replaceAll = __bind(this.replaceAll, this);;
        this.escape = __bind(this.escape, this);;
      }
      HtmlEscaper.prototype.escape = function(text) {
        return this.replaceAll(text, [[/&/g, "&amp;"], [/</g, "&lt;"], [/>/g, "&gt;"], [/"/g, "&quot;"], [/\ /g, "&nbsp;"], [/'/g, "&#x27;"], [/\//g, "&#x2F;"]]);
      };
      HtmlEscaper.prototype.replaceAll = function(text, replacements) {
        var replacement, _i, _len;
        for (_i = 0, _len = replacements.length; _i < _len; _i++) {
          replacement = replacements[_i];
          text = text.replace(replacement[0], replacement[1]);
        }
        return text;
      };
      return HtmlEscaper;
    })();
  });
}).call(this);
