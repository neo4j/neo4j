(function() {
  var __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  };
  define(['./JmxBackedModel', 'lib/backbone'], function(JmxBackedModel) {
    var CacheUsage;
    return CacheUsage = (function() {
      function CacheUsage() {
        CacheUsage.__super__.constructor.apply(this, arguments);
      }
      __extends(CacheUsage, JmxBackedModel);
      CacheUsage.prototype.beans = {
        cache: {
          domain: 'neo4j',
          name: 'Cache'
        }
      };
      return CacheUsage;
    })();
  });
}).call(this);
