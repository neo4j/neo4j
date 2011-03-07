(function() {
  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; };
  define(["./ItemUrlResolver", "lib/backbone"], function(ItemUrlResolver) {
    var RelationshipSearcher;
    return RelationshipSearcher = (function() {
      function RelationshipSearcher(server) {
        this.extractRelId = __bind(this.extractRelId, this);;
        this.exec = __bind(this.exec, this);;
        this.match = __bind(this.match, this);;        this.server = server;
        this.urlResolver = new ItemUrlResolver(server);
        this.pattern = /^((rel)|(relationship)):([0-9]+)$/i;
      }
      RelationshipSearcher.prototype.match = function(statement) {
        return this.pattern.test(statement);
      };
      RelationshipSearcher.prototype.exec = function(statement) {
        return this.server.rel(this.urlResolver.getRelationshipUrl(this.extractRelId(statement)));
      };
      RelationshipSearcher.prototype.extractRelId = function(statement) {
        var match;
        match = this.pattern.exec(statement);
        return match[4];
      };
      return RelationshipSearcher;
    })();
  });
}).call(this);
