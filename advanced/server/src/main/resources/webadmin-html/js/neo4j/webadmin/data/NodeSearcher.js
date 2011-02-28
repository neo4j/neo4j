(function() {
  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; };
  define(["lib/backbone"], function() {
    var NodeSearcher;
    return NodeSearcher = (function() {
      function NodeSearcher() {
        this.exec = __bind(this.exec, this);;
        this.match = __bind(this.match, this);;        this.pattern = /^(node:)?([0-9]+)$/i;
      }
      NodeSearcher.prototype.match = function(statement) {
        return this.pattern.test(statement);
      };
      NodeSearcher.prototype.exec = function(statement) {
        return location.hash = "#/data/node/" + this.getNodeId(statement);
      };
      NodeSearcher.prototype.getNodeId = function(statement) {
        var match;
        match = this.pattern.exec(statement);
        return match[2];
      };
      return NodeSearcher;
    })();
  });
}).call(this);
