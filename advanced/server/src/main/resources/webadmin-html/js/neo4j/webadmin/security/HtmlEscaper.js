(function() {
  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; };
  define(['lib/backbone'], function() {
    var HtmlEscaper;
    return HtmlEscaper = (function() {
      function HtmlEscaper() {
        this.replaceAll = __bind(this.replaceAll, this);;
        this.escape = __bind(this.escape, this);;
      }
      HtmlEscaper.prototype.escape = function(text) {
        return this.replaceAll(text, [[/&/g, "&amp;"], [/</g, "&lt;"], [/>/g, "&gt;"], [/"/g, "&quot;"], [/\ /g, "&nbsp;"], [/'/g, "&#x27;"], [/\//g, "&#x2F;"]]);
      };
      HtmlEscaper.prototype.replaceAll = function(text, replacements) {
        var replacement, _i, _len;
        for (_i = 0, _len = replacements.length; _i < _len; _i++) {
          replacement = replacements[_i];
          text = text.replace(replacement[0], replacement[1]);
        }
        return text;
      };
      return HtmlEscaper;
    })();
  });
}).call(this);
