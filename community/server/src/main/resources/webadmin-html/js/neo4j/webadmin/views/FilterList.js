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
  */  var __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  };
  define(['neo4j/webadmin/views/View', 'neo4j/webadmin/templates/filterList', 'neo4j/webadmin/templates/filterListSelect', 'lib/backbone'], function(View, template, selectTemplate) {
    var FILTER_PLACEHOLDER_TEXT, FilterList;
    FILTER_PLACEHOLDER_TEXT = "Search available nodes";
    return FilterList = (function() {
      function FilterList() {
        FilterList.__super__.constructor.apply(this, arguments);
      }
      __extends(FilterList, View);
      FilterList.prototype.events = {
        'keyup .filterText': 'filterKeyUp',
        'change .filterText': 'filterChanged',
        'focus .filterText': 'filterFocused',
        'blur .filterText': 'filterUnfocused'
      };
      FilterList.prototype.initialize = function(items) {
        var item, _i, _len;
        this.items = this.filteredItems = items;
        this.filter = "";
        this.keyMap = {};
        for (_i = 0, _len = items.length; _i < _len; _i++) {
          item = items[_i];
          this.keyMap[item.key] = item;
        }
        return FilterList.__super__.initialize.call(this);
      };
      FilterList.prototype.render = function() {
        $(this.el).html(template({
          filter: this.filter
        }));
        this.renderListSelector();
        return $(".filterText", this.el).focus();
      };
      FilterList.prototype.height = function(val) {
        if (val != null) {
          return $(".selectList", this.el).height(val - 50);
        } else {
          return FilterList.__super__.height.call(this);
        }
      };
      FilterList.prototype.renderListSelector = function() {
        return $('.selectWrap', this.el).html(selectTemplate({
          items: this.filteredItems
        }));
      };
      FilterList.prototype.filterKeyUp = function(ev) {
        if (!($(ev.target).val().toLowerCase() === this.filter)) {
          return this.filterChanged(ev);
        }
      };
      FilterList.prototype.filterFocused = function(ev) {
        if ($(ev.target).val() === FILTER_PLACEHOLDER_TEXT) {
          return $(ev.target).val("");
        }
      };
      FilterList.prototype.filterUnfocused = function(ev) {
        if ($(ev.target).val().length === 0) {
          return $(ev.target).val(FILTER_PLACEHOLDER_TEXT);
        }
      };
      FilterList.prototype.filterChanged = function(ev) {
        var item, _i, _len, _ref;
        this.filter = $(ev.target).val().toLowerCase();
        if (this.filter.length === 0 || this.filter === FILTER_PLACEHOLDER_TEXT.toLowerCase()) {
          this.filteredItems = this.items;
        } else {
          this.filteredItems = [];
          _ref = this.items;
          for (_i = 0, _len = _ref.length; _i < _len; _i++) {
            item = _ref[_i];
            if (item.label.toLowerCase().indexOf(this.filter) !== -1) {
              this.filteredItems.push(item);
            }
          }
        }
        return this.renderListSelector();
      };
      FilterList.prototype.getFilteredItems = function() {
        var key, keys, _i, _len, _results;
        keys = $(".selectList", this.el).val();
        if (keys !== null) {
          _results = [];
          for (_i = 0, _len = keys.length; _i < _len; _i++) {
            key = keys[_i];
            _results.push(this.keyMap[key]);
          }
          return _results;
        } else {
          return [];
        }
      };
      return FilterList;
    })();
  });
}).call(this);
