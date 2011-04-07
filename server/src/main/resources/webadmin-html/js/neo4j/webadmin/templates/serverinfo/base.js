define(function(){return function(vars){ with(vars||{}) { return "<div class=\"sidebar\"><ol>" + 
(function () { var __result__ = [], __key__, domain; for (__key__ in domains) { if (domains.hasOwnProperty(__key__)) { domain = domains[__key__]; __result__.push(
"<lh>" + 
domain.name + 
"</lh>" +
(function () { var __result__ = [], __key__, bean; for (__key__ in domain.beans) { if (domain.beans.hasOwnProperty(__key__)) { bean = domain.beans[__key__]; __result__.push(
"<li><a href=\"#/info/" +
bean.domain +
"/" +
bean.getName() +
"/\">" + 
bean.getName() + 
"</a></li>"
); } } return __result__.join(""); }).call(this)
); } } return __result__.join(""); }).call(this) + 
"</ol></div><div class=\"workarea with-sidebar\" id=\"info-bean\"></div>";}}; });