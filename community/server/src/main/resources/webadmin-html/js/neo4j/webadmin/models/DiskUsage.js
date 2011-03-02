(function() {
  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; }, __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  };
  define(['./JmxBackedModel', 'lib/backbone'], function(JmxBackedModel) {
    var DiskUsage;
    return DiskUsage = (function() {
      function DiskUsage() {
        this.getLogicalLogPercentage = __bind(this.getLogicalLogPercentage, this);;
        this.getLogicalLogSize = __bind(this.getLogicalLogSize, this);;
        this.getDatabasePercentage = __bind(this.getDatabasePercentage, this);;
        this.getDatabaseSize = __bind(this.getDatabaseSize, this);;        DiskUsage.__super__.constructor.apply(this, arguments);
      }
      __extends(DiskUsage, JmxBackedModel);
      DiskUsage.prototype.beans = {
        diskUsage: {
          domain: 'neo4j',
          name: 'Store file sizes'
        }
      };
      DiskUsage.prototype.getDatabaseSize = function() {
        return this.get("TotalStoreSize") - this.get("LogicalLogSize");
      };
      DiskUsage.prototype.getDatabasePercentage = function() {
        return Math.round(((this.get("TotalStoreSize") - this.get("LogicalLogSize")) / this.get("TotalStoreSize")) * 100);
      };
      DiskUsage.prototype.getLogicalLogSize = function() {
        return this.get("LogicalLogSize");
      };
      DiskUsage.prototype.getLogicalLogPercentage = function() {
        return Math.round((this.get("LogicalLogSize") / this.get("TotalStoreSize")) * 100);
      };
      return DiskUsage;
    })();
  });
}).call(this);
