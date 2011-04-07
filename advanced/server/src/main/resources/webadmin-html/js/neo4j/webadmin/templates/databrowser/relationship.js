define(function(){return function(vars){ with(vars||{}) { return "<div class=\"headline-bar pad\"><div class=\"title\"><h3>" + 
"Relationship " + item.getId() + 
"</h3><p class=\"small\">" + 
item.getSelf() + 
"</p></div><ul class=\"button-bar item-controls\"><li><button disabled=\"true\" class=\"data-save-properties button\">Saved</button></li><li><button class=\"data-delete-item bad-button\">Delete</button></li></ul><ul class=\"relationship-meta\"><li><a href=\"#/data/search/" +
item.getStartId() +
"/\" class=\"micro-button\">" + 
"Node " + item.getStartId() + 
"</a></li><li class=\"type\">" + 
item.getItem().getType() + 
"</li><li><a href=\"#/data/search/" +
item.getEndId() +
"/\" class=\"micro-button\">" + 
"Node " + item.getEndId() + 
"</a></li></ul><div class=\"break\"></div></div><div class=\"properties\"></div><div class=\"break\"></div>";}}; });