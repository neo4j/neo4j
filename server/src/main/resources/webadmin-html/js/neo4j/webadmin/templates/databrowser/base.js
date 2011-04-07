define(function(){return function(vars){ with(vars||{}) { return "<div class=\"workarea\"><div class=\"controls pad\"><div class=\"span-half data-console-wrap\"><input value=\"" +
query +
"\" id=\"data-console\" /><button title=\"Search\" class=\"icon-button\" id=\"data-execute-console\"></button><div class=\"helpbox break\">Search by: [node id], rel:[relationship id], rels:[node id]</div></div><div class=\"span-half last\"><ul class=\"data-toolbar button-bar\"><li><button title=\"Create a node\" class=\"text-icon-button\" id=\"data-create-node\">Node</button></li><li><button title=\"Create a relationship\" class=\"text-icon-button\" id=\"data-create-relationship\">Relationship</button></li>" +
(function () { if (viewType === "tabular"      ) { return (
"<li><button title=\"Switch view mode\" class=\"icon-button\" id=\"data-switch-view\"></button></li>"
);} else { return ""; } }).call(this) +
(function () { if (viewType !== "tabular"      ) { return (
"<li><button title=\"Switch view mode\" class=\"icon-button tabular\" id=\"data-switch-view\"></button></li>"
);} else { return ""; } }).call(this) + 
"</ul></div><div class=\"break\"></div></div><div id=\"data-area\"></div></div>";}}; });