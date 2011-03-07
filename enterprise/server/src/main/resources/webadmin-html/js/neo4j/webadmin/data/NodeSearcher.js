(function() {
  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; };
  define(["./ItemUrlResolver", "lib/backbone"], function(ItemUrlResolver) {
    var NodeSearcher;
    return NodeSearcher = (function() {
      function NodeSearcher(server) {
        this.extractNodeId = __bind(this.extractNodeId, this);;
        this.exec = __bind(this.exec, this);;
        this.match = __bind(this.match, this);;        this.server = server;
        this.urlResolver = new ItemUrlResolver(server);
        this.pattern = /^(node:)?([0-9]+)$/i;
      }
      NodeSearcher.prototype.match = function(statement) {
        return this.pattern.test(statement);
      };
      NodeSearcher.prototype.exec = function(statement) {
        return this.server.node(this.urlResolver.getNodeUrl(this.extractNodeId(statement)));
      };
      NodeSearcher.prototype.extractNodeId = function(statement) {
        var match;
        match = this.pattern.exec(statement);
        return match[2];
      };
      return NodeSearcher;
    })();
  });
}).call(this);
