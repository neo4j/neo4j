define(function(){return function(vars){ with(vars||{}) { return "<ul>" + 
(function () { var __result__ = [], __key__, line; for (__key__ in lines) { if (lines.hasOwnProperty(__key__)) { line = lines[__key__]; __result__.push(
"<li>" + 
line + 
"</li>"
); } } return __result__.join(""); }).call(this) +
"<li>" + 
(function () { if (showPrompt) { return (
"gremlin> <input type=\"text\" value=\"" +
prompt +
"\" id=\"console-input\" />"
);} else { return ""; } }).call(this) + 
"</li></ul>";}}; });