define(function(){return function(vars){ with(vars||{}) { return "<div class=\"headline-bar pad\"><h3>" + 
item.getSelf() + 
"</h3><ul class=\"button-bar item-controls\"><li><button disabled=\"true\" class=\"data-save-properties button\">Saved</button></li></ul><a href=\"#/data/search/" +
item.getItem().getStartNodeUrl() +
"/\">" + 
item.getItem().getStartNodeUrl() + 
"</a><a href=\"#/data/search/" +
item.getItem().getEndNodeUrl() +
"/\">" + 
item.getItem().getEndNodeUrl() + 
"</a></div><div class=\"properties\"></div>";}}; });