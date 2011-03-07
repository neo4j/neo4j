(function() {
  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; };
  define(["lib/backbone"], function() {
    var ItemUrlResolver;
    return ItemUrlResolver = (function() {
      function ItemUrlResolver(server) {
        this.extractLastUrlSegment = __bind(this.extractLastUrlSegment, this);;
        this.extractRelationshipId = __bind(this.extractRelationshipId, this);;
        this.extractNodeId = __bind(this.extractNodeId, this);;
        this.getRelationshipUrl = __bind(this.getRelationshipUrl, this);;
        this.getNodeUrl = __bind(this.getNodeUrl, this);;        this.server = server;
      }
      ItemUrlResolver.prototype.getNodeUrl = function(id) {
        return this.server.url + "/db/data/node/" + id;
      };
      ItemUrlResolver.prototype.getRelationshipUrl = function(id) {
        return this.server.url + "/db/data/relationship/" + id;
      };
      ItemUrlResolver.prototype.extractNodeId = function(url) {
        return this.extractLastUrlSegment(url);
      };
      ItemUrlResolver.prototype.extractRelationshipId = function(url) {
        return this.extractLastUrlSegment(url);
      };
      ItemUrlResolver.prototype.extractLastUrlSegment = function(url) {
        if (url.substr(-1) === "/") {
          url = url.substr(0, url.length - 1);
        }
        return url.substr(url.lastIndexOf("/") + 1);
      };
      return ItemUrlResolver;
    })();
  });
}).call(this);
