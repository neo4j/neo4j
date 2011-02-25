(function() {
  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; }, __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  };
  define(['lib/backbone'], function() {
    var JmxBackedModel;
    return JmxBackedModel = (function() {
      function JmxBackedModel() {
        this.initialize = __bind(this.initialize, this);;        JmxBackedModel.__super__.constructor.apply(this, arguments);
      }
      __extends(JmxBackedModel, Backbone.Model);
      JmxBackedModel.prototype.initialize = function(options) {
        var definition, key, _ref, _results;
        this.server = options.server;
        _ref = this.beans;
        _results = [];
        for (key in _ref) {
          definition = _ref[key];
          _results.push(console.log(key, definition));
        }
        return _results;
      };
      return JmxBackedModel;
    })();
  });
}).call(this);
