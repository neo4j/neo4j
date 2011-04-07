define(function(){return function(vars){ with(vars||{}) { return "<div class=\"headline-bar pad\"><div class=\"title\"><h3>" + 
"Node " + item.getId() + 
"</h3><p class=\"small\">" + 
item.getSelf() + 
"</p></div><ul class=\"button-bar item-controls\"><li><a title=\"Show a list of relationships for this node\" href=\"#/data/search/rels:" +
item.getId() +
"/\" class=\"data-show-relationships button\">Show relationships</a></li><li><button disabled=\"true\" class=\"data-save-properties button\">Saved</button></li><li><button class=\"data-delete-item bad-button\">Delete</button></li></ul><div class=\"break\"></div></div><div class=\"properties\"></div><div class=\"break\"></div>";}}; });