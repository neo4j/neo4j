(function() {
  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; }, __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  };
  define(['neo4j/webadmin/templates/base', 'lib/backbone'], function(template) {
    var BaseView;
    return BaseView = (function() {
      function BaseView() {
        this.initialize = __bind(this.initialize, this);;        BaseView.__super__.constructor.apply(this, arguments);
      }
      __extends(BaseView, Backbone.View);
      BaseView.prototype.template = template;
      BaseView.prototype.initialize = function(options) {
        this.appState = options.appState;
        return this.appState.bind('change:mainView', __bind(function(event) {
          this.mainView = event.attributes.mainView;
          return this.render();
        }, this));
      };
      BaseView.prototype.render = function() {
        $(this.el).html(this.template({
          mainmenu: [
            {
              label: "Dashboard",
              url: "#",
              current: location.hash === ""
            }, {
              label: "Data",
              url: "#/data/",
              current: location.hash === "#/data/"
            }, {
              label: "Console",
              url: "#/console/",
              current: location.hash === "#/console/"
            }, {
              label: "Server info",
              url: "#/info/",
              current: location.hash === "#/info/"
            }
          ]
        }));
        if (this.mainView != null) {
          $("#contents").append(this.mainView.render().el);
        }
        return this;
      };
      return BaseView;
    })();
  });
}).call(this);
