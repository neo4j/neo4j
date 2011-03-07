(function() {
  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; }, __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  };
  define(['lib/backbone'], function() {
    var ServerInfo;
    return ServerInfo = (function() {
      function ServerInfo() {
        this.getBean = __bind(this.getBean, this);;
        this.setBean = __bind(this.setBean, this);;
        this.parseJmxBeans = __bind(this.parseJmxBeans, this);;
        this.fetch = __bind(this.fetch, this);;
        this.setCurrent = __bind(this.setCurrent, this);;
        this.initialize = __bind(this.initialize, this);;        ServerInfo.__super__.constructor.apply(this, arguments);
      }
      __extends(ServerInfo, Backbone.Model);
      ServerInfo.prototype.initialize = function(opts) {
        return this.server = opts.server;
      };
      ServerInfo.prototype.setCurrent = function(domain, beanName) {
        this.current = {
          domain: domain,
          beanName: beanName
        };
        if ((this.get("domains") != null) && (this.getBean(domain, beanName) != null)) {
          return this.set({
            current: this.getBean(domain, beanName)
          });
        } else {
          return this.fetch();
        }
      };
      ServerInfo.prototype.fetch = function() {
        return this.server.manage.jmx.query(["*:*"], this.parseJmxBeans);
      };
      ServerInfo.prototype.parseJmxBeans = function(beans) {
        var NEO4J_DOMAIN, bean, currentDomainBeans, currentDomainName, domains, _i, _len;
        NEO4J_DOMAIN = "org.neo4j";
        beans = beans.sort(function(a, b) {
          var aName, bName;
          aName = a.domain === NEO4J_DOMAIN ? "0" + a.getName() : a.jmxName;
          bName = b.domain === NEO4J_DOMAIN ? "0" + b.getName() : b.jmxName;
          return aName.toLowerCase() > bName.toLowerCase();
        });
        domains = [];
        currentDomainName = null;
        currentDomainBeans = [];
        for (_i = 0, _len = beans.length; _i < _len; _i++) {
          bean = beans[_i];
          if (currentDomainName !== bean.domain) {
            currentDomainName = bean.domain;
            currentDomainBeans = [];
            domains.push({
              name: bean.domain,
              beans: currentDomainBeans
            });
          }
          currentDomainBeans.push(bean);
          this.setBean(bean);
          if ((this.current != null) && this.current.domain === currentDomainName && this.current.beanName === bean.getName()) {
            this.set({
              current: bean
            }, {
              silent: true
            });
          }
        }
        if (!(this.current != null) && domains.length > 0 && domains[0].beans.length > 0) {
          this.set({
            current: domains[0].beans[0]
          }, {
            silent: true
          });
        }
        return this.set({
          domains: domains
        });
      };
      ServerInfo.prototype.setBean = function(bean) {
        var beanData;
        beanData = {};
        beanData["" + bean.domain + ":" + (bean.getName())] = bean;
        return this.set(beanData, {
          silent: true
        });
      };
      ServerInfo.prototype.getBean = function(domain, name) {
        return this.get("" + domain + ":" + name);
      };
      return ServerInfo;
    })();
  });
}).call(this);
