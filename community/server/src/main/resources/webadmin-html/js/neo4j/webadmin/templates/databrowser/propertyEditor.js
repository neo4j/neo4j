define(function(){return function(vars){ with(vars||{}) { return "<ul class=\"property-list\">" + 
(function () { var __result__ = [], __key__, property; for (__key__ in properties) { if (properties.hasOwnProperty(__key__)) { property = properties[__key__]; __result__.push(
"<li><ul class=\"property-row\"><li class=\"property-key-wrap\"><div class=\"property-input-wrap\"><input type=\"hidden\" value=\"" +
property.getLocalId() +
"\" class=\"property-id\" /><div class=\"error\">" + 
(function () { if (property.hasKeyError()) { return (
property.getKeyError()
);} else { return ""; } }).call(this) + 
"</div><input type=\"text\" value=\"" +
escape(property.getKey()) +
"\" class=\"property-key\" /></div></li><li class=\"property-value-wrap\"><div class=\"property-input-wrap\"><div class=\"error\">" + 
(function () { if (property.hasValueError()) { return (
property.getValueError()
);} else { return ""; } }).call(this) + 
"</div><input type=\"text\" value=\"" +
property.getValueAsHtml() +
"\" class=\"property-value\" /></div></li><li class=\"property-actions-wrap\"><div class=\"property-input-wrap\"><button class=\"delete-property bad-button\">Remove</button></div></li></ul><div class=\"break\"></div></li>"
); } } return __result__.join(""); }).call(this) +
"<li class=\"property-controls\"><button title=\"Add a new property\" class=\"add-property text-icon-button\">Add property</button><div class=\"break\"></div></li></ul>";}}; });