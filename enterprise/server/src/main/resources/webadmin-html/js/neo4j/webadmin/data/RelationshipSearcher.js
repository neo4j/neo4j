(function() {
  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; };
  define([], function() {
    var RelationshipSearcher;
    return RelationshipSearcher = (function() {
      function RelationshipSearcher() {
        this.exec = __bind(this.exec, this);;
        this.match = __bind(this.match, this);;        this.pattern = /^((rel)|(relationship)):([0-9]+)$/i;
      }
      RelationshipSearcher.prototype.match = function(statement) {
        return this.pattern.test(statement);
      };
      RelationshipSearcher.prototype.exec = function(statement) {
        return location.hash = "#/data/relationship/" + this.getRelationshipId(statement);
      };
      RelationshipSearcher.prototype.getRelationshipId = function(statement) {
        return this.pattern.exec(statement)[4];
      };
      return RelationshipSearcher;
    })();
  });
}).call(this);
