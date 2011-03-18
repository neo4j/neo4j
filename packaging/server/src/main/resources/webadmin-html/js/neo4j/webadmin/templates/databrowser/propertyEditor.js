/*
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
define(function(){return function(vars){ with(vars||{}) { return "<ul class=\"property-list\">" + 
(function () { var __result__ = [], __key__, property; for (__key__ in properties) { if (properties.hasOwnProperty(__key__)) { property = properties[__key__]; __result__.push(
"<li><ul class=\"property-row\"><li class=\"property-key-wrap\"><div class=\"property-input-wrap\"><input type=\"hidden\" value=\"" +
property.getLocalId() +
"\" class=\"property-id\" />" +
(function () { if (property.hasKeyError()) { return (
"<div class=\"error\">" + 
property.getKeyError() + 
"</div>"
);} else { return ""; } }).call(this) +
"<input type=\"text\" value=\"" +
escape(property.getKey()) +
"\" class=\"property-key\" /></div></li><li class=\"property-value-wrap\"><div class=\"property-input-wrap\">" + 
(function () { if (property.hasValueError()) { return (
"<div class=\"error\">" + 
property.getValueError() + 
"</div>"
);} else { return ""; } }).call(this) +
"<input type=\"text\" value=\"" +
property.getValueAsHtml() +
"\" class=\"property-value\" /></div></li><li class=\"property-actions-wrap\"><div class=\"property-input-wrap\"><button class=\"delete-property button\">Remove</button></div></li></ul><div class=\"break\"></div></li>"
); } } return __result__.join(""); }).call(this) +
"<li class=\"property-controls\"><button title=\"Add a new property\" class=\"add-property text-icon-button\">Add property</button><div class=\"break\"></div></li></ul>";}}; });