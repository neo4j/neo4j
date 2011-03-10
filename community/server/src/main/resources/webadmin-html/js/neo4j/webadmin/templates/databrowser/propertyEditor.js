define(function(){return function(vars){ with(vars||{}) { return "<ul>" + 
(function () { var __result__ = [], __key__, property; for (__key__ in properties) { if (properties.hasOwnProperty(__key__)) { property = properties[__key__]; __result__.push(
"<li><ul><li><input type=\"hidden\" value=\"" +
property.getLocalId() +
"\" class=\"property-id\" />" +
(function () { if (property.isDuplicate()) { return (
"<div class=\"error\">This property already exists, please change the name.</div>"
);} else { return ""; } }).call(this) +
"<input type=\"text\" value=\"" +
escape(property.getKey()) +
"\" class=\"property-key\" /></li><li>" + 
(function () { if (property.getValueError()) { return (
"<div class=\"error\">" + 
property.getValueError() + 
"</div>"
);} else { return ""; } }).call(this) +
"<input type=\"text\" value=\"" +
property.getValueAsHtml() +
"\" class=\"property-value\" /></li><li><button class=\"delete-property\">Remove</button></li></ul></li>"
); } } return __result__.join(""); }).call(this) +
"<li><button class=\"add-property\">Add property</button></li></ul>";}}; });