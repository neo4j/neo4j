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
        this.parseBean = __bind(this.parseBean, this);;
        this.fetch = __bind(this.fetch, this);;
        this.setPollingInterval = __bind(this.setPollingInterval, this);;
        this.initialize = __bind(this.initialize, this);;        JmxBackedModel.__super__.constructor.apply(this, arguments);
      }
      __extends(JmxBackedModel, Backbone.Model);
      JmxBackedModel.prototype.initialize = function(options) {
        this.server = options.server;
        this.jmx = this.server.manage.jmx;
        if ((options.pollingInterval != null) && options.pollingInterval > 0) {
          this.fetch();
          return this.setPollingInterval(options.pollingInterval);
        }
      };
      JmxBackedModel.prototype.setPollingInterval = function(ms) {
        if (this.interval != null) {
          clearInterval(this.interval);
        }
        return this.interval = setInterval(this.fetch, ms);
      };
      JmxBackedModel.prototype.fetch = function() {
        var def, key, parseBean, x, _ref, _results;
        parseBean = this.parseBean;
        _ref = this.beans;
        _results = [];
        for (key in _ref) {
          def = _ref[key];
          _results.push((function() {
            var _results;
            _results = [];
            for (x = 0; x <= 1000; x++) {
              _results.push(this.jmx.getBean(def.domain, def.name, function(bean) {
                return parseBean(key, bean);
              }));
            }
            return _results;
          }).call(this));
        }
        return _results;
      };
      JmxBackedModel.prototype.parseBean = function(key, bean) {
        var attribute, values, _i, _len, _ref;
        if (bean != null) {
          values = {};
          _ref = bean.attributes;
          for (_i = 0, _len = _ref.length; _i < _len; _i++) {
            attribute = _ref[_i];
            values[attribute.name] = attribute.value;
          }
          return this.set(values);
        }
      };
      return JmxBackedModel;
    })();
  });
}).call(this);
