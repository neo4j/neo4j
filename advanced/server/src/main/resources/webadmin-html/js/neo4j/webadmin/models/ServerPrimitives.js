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
    var ServerPrimitives;
    return ServerPrimitives = (function() {
      function ServerPrimitives() {
        ServerPrimitives.__super__.constructor.apply(this, arguments);
      }
      __extends(ServerPrimitives, JmxBackedModel);
      ServerPrimitives.prototype.beans = {
        primitives: {
          domain: 'neo4j',
          name: 'Primitive count'
        }
      };
      return ServerPrimitives;
    })();
  });
}).call(this);
