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
  */  define(['lib/backbone'], function() {
    var CookieStorage;
    return CookieStorage = (function() {
      function CookieStorage() {}
      /*
      A cookie manager based on Peter-Paul Kochs cookie code.
      */
      CookieStorage.prototype.get = function(key) {
        var cookieStr, cookieStrings, nameEQ, _i, _len;
        nameEQ = "" + key + "=";
        cookieStrings = document.cookie.split(';');
        for (_i = 0, _len = cookieStrings.length; _i < _len; _i++) {
          cookieStr = cookieStrings[_i];
          while (cookieStr.charAt(0) === ' ') {
            cookieStr = cookieStr.substring(1, cookieStr.length);
          }
          if (cookieStr.indexOf(nameEQ) === 0) {
            return cookieStr.substring(nameEQ.length, cookieStr.length);
          }
        }
        return null;
      };
      CookieStorage.prototype.set = function(key, value, days) {
        var date, expires;
        if (days == null) {
          days = false;
        }
        if (days) {
          date = new Date();
          date.setTime(date.getTime() + (days * 24 * 60 * 60 * 1000));
          expires = "; expires=" + date.toGMTString();
        } else {
          expires = "";
        }
        return document.cookie = "" + key + "=" + value + expires + "; path=/";
      };
      CookieStorage.prototype.remove = function(key) {
        return this.set(key, "", -1);
      };
      return CookieStorage;
    })();
  });
}).call(this);
