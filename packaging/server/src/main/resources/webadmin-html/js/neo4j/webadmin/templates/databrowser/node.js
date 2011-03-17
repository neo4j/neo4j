define(function(){return function(vars){ with(vars||{}) { return "<div class=\"headline-bar pad\"><h3>" + 
item.getSelf() + 
"</h3><ul class=\"button-bar item-controls\"><li><a title=\"Show a list of relationships for this node\" href=\"#/data/search/rels:" +
item.getId() +
"/\" class=\"data-show-relationships button\">Show relationships</a></li><li><button disabled=\"true\" class=\"data-save-properties button\">Saved</button></li></ul></div><div class=\"properties\"></div>";}}; });