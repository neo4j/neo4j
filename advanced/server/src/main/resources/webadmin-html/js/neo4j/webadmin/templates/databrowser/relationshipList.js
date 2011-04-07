define(function(){return function(vars){ with(vars||{}) { return "<table cellspacing=\"0\" class=\"data-table\"><tbody><tr><th><h3>Relationship</h3></th><th><h3>Start node</h3></th><th><h3>Type</h3></th><th><h3>End node</h3></th>" +
(function () { var __result__ = [], __key__, key; for (__key__ in relationshipList.getPropertyKeys()) { if (relationshipList.getPropertyKeys().hasOwnProperty(__key__)) { key = relationshipList.getPropertyKeys()[__key__]; __result__.push(
"<th><h3>" + 
key + 
"</h3></th>"
); } } return __result__.join(""); }).call(this) + 
"</tr>" +
(function () { var __result__ = [], __key__, relationship; for (__key__ in relationshipList.getRelationships()) { if (relationshipList.getRelationships().hasOwnProperty(__key__)) { relationship = relationshipList.getRelationships()[__key__]; __result__.push(
"<tr><td><a href=\"#/data/search/rel:" +
relationship.getId() +
"/\" class=\"micro-button\">" + 
"Relationship " + relationship.getId() + 
"</a></td><td><a href=\"#/data/search/" +
relationship.getStartId() +
"/\" class=\"micro-button\">" + 
"Node " +relationship.getStartId() + 
"</a></td><td class=\"small\">" + 
relationship.getItem().getType() + 
"</td><td><a href=\"#/data/search/" +
relationship.getEndId() +
"/\" class=\"micro-button\">" + 
"Node " + relationship.getEndId() + 
"</a></td>" +
(function () { var __result__ = [], __key__, key; for (__key__ in relationshipList.getPropertyKeys()) { if (relationshipList.getPropertyKeys().hasOwnProperty(__key__)) { key = relationshipList.getPropertyKeys()[__key__]; __result__.push(
"<td class=\"small\">" + 
(function () { if (relationship.getPropertyByKey(key)) { return (
relationship.getPropertyByKey(key).getTruncatedHtmlValue(50)
);} else { return ""; } }).call(this) + 
"</td>"
); } } return __result__.join(""); }).call(this) + 
"</tr>"
); } } return __result__.join(""); }).call(this) + 
"</tbody></table><div class=\"break\"></div>";}}; });