define(function(){return function(vars){ with(vars||{}) { return "<p><label for=\"create-relationship-from\">From node</label><input type=\"text\" value=\"" +
from +
"\" id=\"create-relationship-from\" /></p><p><label for=\"create-relationship-type\">Type</label><input type=\"text\" value=\"" +
type +
"\" id=\"create-relationship-type\" /></p><p><select id=\"create-relationship-types\">&nbsp;<option>Types in use</option>" +
(function () { var __result__ = [], __key__, type; for (__key__ in types) { if (types.hasOwnProperty(__key__)) { type = types[__key__]; __result__.push(
"<option>" + 
type + 
"</option>"
); } } return __result__.join(""); }).call(this) + 
"</select><div class=\"break\"></div></p><p><label for=\"create-relationship-to\">To node</label><input type=\"text\" value=\"" +
to +
"\" id=\"create-relationship-to\" /></p><ul class=\"button-bar popout-controls\"><li><button class=\"button\" id=\"create-relationship\">Create</button></li></ul>";}}; });