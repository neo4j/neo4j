(function() {
  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; };
  define(["./NodeSearcher", "./RelationshipSearcher"], function(NodeSearcher, RelationshipSearcher) {
    var Search;
    return Search = (function() {
      function Search(server) {
        this.pickSearcher = __bind(this.pickSearcher, this);;
        this.exec = __bind(this.exec, this);;        this.searchers = [new NodeSearcher(server), new RelationshipSearcher(server)];
      }
      Search.prototype.exec = function(statement) {
        var searcher;
        searcher = this.pickSearcher(statement);
        if (searcher != null) {
          return searcher.exec(statement);
        }
      };
      Search.prototype.pickSearcher = function(statement) {
        var searcher, _i, _len, _ref;
        _ref = this.searchers;
        for (_i = 0, _len = _ref.length; _i < _len; _i++) {
          searcher = _ref[_i];
          if (searcher.match(statement)) {
            return searcher;
          }
        }
      };
      return Search;
    })();
  });
}).call(this);
