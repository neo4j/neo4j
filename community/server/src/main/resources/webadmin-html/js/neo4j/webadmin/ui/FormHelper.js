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
  */  define(['lib/backbone', 'lib/jquery'], function() {
    var FormHelper;
    return FormHelper = (function() {
      function FormHelper(context) {
        if (context == null) {
          context = "body";
        }
        this.context = $(context);
      }
      FormHelper.prototype.addErrorTo = function(selector, errorMsg, wrapperSelector) {
        var errorElement, wrap;
        if (wrapperSelector == null) {
          wrapperSelector = "p";
        }
        wrap = $(selector, this.context).closest(wrapperSelector);
        errorElement = wrap.find(".error");
        if (errorElement.length === 0) {
          errorElement = $("<div class='error'></div>");
          wrap.prepend(errorElement);
        }
        return errorElement.html(errorMsg);
      };
      FormHelper.prototype.removeAllErrors = function() {
        return $(".error", this.context).remove();
      };
      FormHelper.prototype.removeErrorsFrom = function(selector, wrapperSelector) {
        if (wrapperSelector == null) {
          wrapperSelector = "p";
        }
        return $(selector, this.context).closest(wrapperSelector).find(".error").remove();
      };
      return FormHelper;
    })();
  });
}).call(this);
