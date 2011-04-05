define(function(){return function(vars){ with(vars||{}) { return "<select multiple=\"multiple\" class=\"selectList\">" + 
(function () { var __result__ = [], __key__, item; for (__key__ in items) { if (items.hasOwnProperty(__key__)) { item = items[__key__]; __result__.push(
"<option value=\"" +
item.key +
"\">" + 
item.label + 
"</option>"
); } } return __result__.join(""); }).call(this) + 
"</select>";}}; });