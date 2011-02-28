(function() {
  var __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  };
  define(['lib/backbone'], function() {
    var DataItemModel;
    return DataItemModel = (function() {
      function DataItemModel() {
        DataItemModel.__super__.constructor.apply(this, arguments);
      }
      __extends(DataItemModel, Backbone.Model);
      return DataItemModel;
    })();
  });
}).call(this);
