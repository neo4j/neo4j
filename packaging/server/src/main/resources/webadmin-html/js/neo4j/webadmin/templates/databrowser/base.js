define(function(){return function(vars){ with(vars||{}) { return "<div class=\"workarea\"><div class=\"controls pad\"><input value=\"" +
query +
"\" id=\"data-console\" /><button title=\"Search\" class=\"icon-button\" id=\"data-execute-console\"></button><div class=\"data-toolbar\"><button title=\"Create a node\" class=\"text-icon-button\" id=\"data-create-node\">Node</button><button title=\"Create a relationship\" class=\"text-icon-button\" id=\"data-create-relationship\">Relationship</button>" +
(function () { if (viewType === "tabular"      ) { return (
"<button title=\"Switch view mode\" class=\"icon-button\" id=\"data-switch-view\"></button>"
);} else { return ""; } }).call(this) +
(function () { if (viewType !== "tabular"      ) { return (
"<button title=\"Switch view mode\" class=\"icon-button tabular\" id=\"data-switch-view\"></button>"
);} else { return ""; } }).call(this) +
"<button title=\"Go to reference node\" class=\"icon-button\" id=\"data-home\"></button><button title=\"Refresh current data\" class=\"icon-button\" id=\"data-refresh\"></button></div><div class=\"break\"></div></div><div id=\"data-area\"></div></div>";}}; });