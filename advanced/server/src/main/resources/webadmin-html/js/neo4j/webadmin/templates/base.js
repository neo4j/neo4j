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
define(function(){return function(vars){ with(vars||{}) { return "<div id=\"header\"><div id=\"header_submenu\"><ul class=\"horizontal_menu\"><li><a href=\"http://docs.neo4j.org/\" class=\"micro-button dark-button\">Documentation</a></li></ul></div><img src=\"img/logo.png\" id=\"logo\" /><ul id=\"mainmenu\">" + 
(function () { var __result__ = [], __key__, item; for (__key__ in mainmenu) { if (mainmenu.hasOwnProperty(__key__)) { item = mainmenu[__key__]; __result__.push(
"<li class=\"title-button\">" + 
(function () { if (item.current) { return (
"<a href=\"" +
item.url +
"\" class=\"current\"><span class=\"subtitle\">" + 
item.subtitle + 
"</span><span>" + 
item.label + 
"</span></a>"
);} else { return ""; } }).call(this) +
(function () { if (!item.current) { return (
"<a href=\"" +
item.url +
"\"><span class=\"subtitle\">" + 
item.subtitle + 
"</span><span>" + 
item.label + 
"</span></a>"
);} else { return ""; } }).call(this) + 
"</li>"
); } } return __result__.join(""); }).call(this) + 
"</ul></div><div id=\"contents\"></div><div id=\"footer\"><p class=\"copyright\">Copyright (c) 2002-2010 <a href=\"http://neotechnology.com\">Neo Technology</a>. This is free software, available under the  <a href=\"http://www.gnu.org/licenses/agpl.html\">GNU Affero General Public License</a> version 3 or greater.</p></div>";}}; });